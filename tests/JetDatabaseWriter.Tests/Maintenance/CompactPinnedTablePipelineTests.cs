namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CompactPinnedTablePipelineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Pipeline_ReopensRawValuesAndEveryPhysicalIndex(DatabaseFormat kind, bool indexed)
    {
        byte[] original = await CreateAsync(kind, indexed);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, definition);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var firstFence = new MemoryStream();
        await using var secondFence = new MemoryStream();
        CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, destination, rows, values, firstFence, secondFence, new(), Ct);
        Assert.Equal(indexed ? 2 : 0, allocation.Indexes.Count);
        Assert.False(allocation.DatabaseValidated);
        Assert.True(allocation.Table.LongValues.PageCount > 0);
        Assert.Equal(destination.Length / source.Database.Format.PageSize, allocation.FirstPageNumber + allocation.PageCount);

        // A separately allocated suffix is owned by the enclosing database pipeline.
        await destination.WriteAsync(new byte[source.Database.Format.PageSize], Ct);

        // Validation uses new observations and closed immutable snapshots; builder fence bytes are irrelevant.
        firstFence.SetLength(0);
        secondFence.SetLength(0);
        indexes = await ObserveAsync(source, owner, definition);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await using var reopenedRows = new MemoryStream(rows.ToArray(), writable: false);
        await using var reopenedValues = new MemoryStream(values.ToArray(), writable: false);
        CompactPinnedTableAssemblyResult verified = await CompactPinnedTableAssembly.ValidateAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, reopened, reopenedRows, reopenedValues, allocation, new(), Ct);
        Assert.Equal(indexed ? 2 : 0, verified.Indexes.Count);
        Assert.False(verified.DatabaseValidated);
        Assert.False(verified.OwnershipProven);
        long next = allocation.FirstPageNumber;
        await foreach (CompactGlobalAllocationRecord run in allocation.AllocationsAsync(Ct))
        {
            Assert.Equal(next, run.Pages.FirstPageNumber);
            next += run.Pages.PageCount;
        }

        Assert.Equal(allocation.FirstPageNumber + allocation.PageCount, next);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(reopened, cancellationToken: Ct);
        Assert.Equal(owner, Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage);
        int count = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: Ct))
        {
            Assert.Equal(count, Assert.IsType<int>(row[0]));
            Assert.Equal(Payload(count), Assert.IsType<string>(row[1]));
            count++;
        }

        Assert.Equal(30, count);
        Assert.Equal(original, immutable.ToArray());
        Assert.True(rows.CanWrite);
        Assert.True(reopened.CanRead);
    }

    [Theory]
    [InlineData("entries")]
    [InlineData("definition")]
    [InlineData("gap")]
    [InlineData("extent")]
    [InlineData("patch")]
    [InlineData("ledger")]
    [InlineData("map-count")]
    [InlineData("extent-before-maps")]
    public async Task ReopenedValidator_RejectsIndependentEvidenceAndAllocationDamage(string damage)
    {
        byte[] original = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, definition);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, destination, rows, values, first, second, new(), Ct);
        byte[] bytes = destination.ToArray();
        byte[] ledger = rows.ToArray();
        switch (damage)
        {
            case "entries":
                indexes[1] = indexes[1] with { Entries = EntriesAsync([], Ct) };
                break;
            case "definition":
                bytes[checked((int)(owner * source.Database.Format.PageSize)) + 20] ^= 1;
                break;
            case "gap":
                allocation = allocation with { FirstPageNumber = allocation.FirstPageNumber - 1 };
                break;
            case "extent":
                allocation = allocation with { PageCount = allocation.PageCount - 1 };
                break;
            case "patch":
                allocation.Table.PatchedDefinition[20] ^= 1;
                break;
            case "ledger":
                ledger[4] ^= 1;
                break;
            case "map-count":
                allocation = allocation with { Table = allocation.Table with { LongValueMaps = new UnenumerableMaps(Constants.TableDefinition.MaxColumns + 1) } };
                break;
            case "extent-before-maps":
                allocation = allocation with { PageCount = 1, Table = allocation.Table with { LongValueMaps = new UnenumerableMaps(allocation.Table.LongValueMaps.Count) } };
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await using var reopenedRows = new MemoryStream(ledger, writable: false);
        await using var reopenedValues = new MemoryStream(values.ToArray(), writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedTableAssembly.ValidateAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, reopened, reopenedRows, reopenedValues, allocation, new(), Ct));
    }

    [Theory]
    [InlineData("count")]
    [InlineData("identity")]
    [InlineData("scratch")]
    [InlineData("output")]
    [InlineData("working")]
    [InlineData("active-working")]
    [InlineData("cancel")]
    public async Task Admission_RefusesBeforeChangingPinnedDefinition(string fault)
    {
        byte[] original = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, definition);
        if (fault == "count")
        {
            indexes = indexes[..1];
        }
        else if (fault == "identity")
        {
            indexes[1] = indexes[0];
        }

        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        if (fault == "cancel")
        {
            await cancellation.CancelAsync();
        }

        var budget = new CompactPinnedTableAssemblyBudget
        {
            MaxOutputBytes = fault == "output" ? 0 : 1_000_000,
            MaxWorkingBytes = fault switch { "working" => 1, "active-working" => 450_000, _ => 1_000_000 },
        };
        Exception? error = await Record.ExceptionAsync(async () => await CompactPinnedTableAssembly.BuildAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, destination, rows, values, first, fault == "scratch" ? first : second, budget, cancellation.Token));
        Assert.NotNull(error);
        Assert.True(error is ArgumentException or JetLimitationException or OperationCanceledException);
        Assert.Equal(original, destination.ToArray().AsSpan(0, original.Length).ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PhysicalAliases_AreRefusedBeforePrivateOutput(bool sharedMap)
    {
        byte[] original = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, definition);
        byte[] bytes = definition.CopyBytes();
        int secondRoot = definition.RealIndexes[1].Slot.FirstDpOffset;
        if (sharedMap)
        {
            UsageMap.WritePointer(bytes, secondRoot - 4, indexes[0].OwnedPages.RowIndex, indexes[0].OwnedPages.PageNumber);
            indexes[1] = indexes[1] with { OwnedPages = indexes[0].OwnedPages };
        }
        else
        {
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(secondRoot), indexes[0].RootPage);
            indexes[1] = indexes[1] with { RootPage = indexes[0].RootPage };
        }

        definition = Assert.IsType<TDefImage>(TDefCodec.Parse(source.Database.Format, bytes));
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedTableAssembly.BuildAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, destination, rows, values, first, second, new(), Ct));
        Assert.Equal(original, destination.ToArray());
        Assert.Equal(original, immutable.ToArray());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task OriginalObservationAdmission_IsCumulativeAcrossPhysicalIndexes(bool keyBytes)
    {
        byte[] original = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var immutable = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(immutable, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage definition = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, definition);
        long oneIndexBytes = 0;
        await foreach (IndexEntry entry in indexes[0].Entries.WithCancellation(Ct))
        {
            oneIndexBytes += entry.Key.Length;
        }

        var budget = new CompactPinnedTableAssemblyBudget
        {
            MaxObservedEntries = keyBytes ? 1_000 : 30,
            MaxObservedKeyBytes = keyBytes ? oneIndexBytes : 1_000_000,
        };
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedTableAssembly.BuildAsync(
            source.Database.Pages, source.Database.Format, owner, definition, indexes, destination, rows, values, first, second, budget, Ct));
        Assert.Equal(original, destination.ToArray().AsSpan(0, original.Length).ToArray());
        Assert.Equal(original, immutable.ToArray());
    }

    private static string Payload(int id) => new((char)('a' + (id % 26)), 3000);

    private static async Task<byte[]> CreateAsync(DatabaseFormat kind, bool indexed)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            IndexDefinition[] indexes = indexed ? [new("PrimaryKey", "Id") { IsPrimaryKey = true }, new("IX_Descending", "Id") { DescendingColumns = ["Id"] }] : [];
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Memo", typeof(string), 20_000)], indexes, Ct);

            await writer.InsertRowsAsync("Items", Enumerable.Range(0, 30).Select(static id => new object?[] { id, Payload(id) }), Ct);
        }

        return stream.ToArray();
    }

    private static async Task<CompactPinnedTableIndexSource[]> ObserveAsync(AccessReader reader, long owner, TDefImage definition)
    {
        var sources = new CompactPinnedTableIndexSource[definition.RealIndexes.Count];
        for (int slot = 0; slot < sources.Length; slot++)
        {
            TDefIndexDescriptor descriptor = definition.RealIndexes[slot];
            Assert.True(UsageMap.TryReadPointer(definition.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
            List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(reader.Database, owner, descriptor.RootPage, Ct);
            sources[slot] = new(descriptor.RootPage, map, EntriesAsync(entries, Ct));
        }

        return sources;
    }

    private static async IAsyncEnumerable<IndexEntry> EntriesAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private sealed class UnenumerableMaps(int count) : IReadOnlyList<CompactRawTableLongValueMaps>
    {
        public int Count => count;

        public CompactRawTableLongValueMaps this[int index] => throw new InvalidOperationException("Map traversal exceeded preflight bounds.");

        public IEnumerator<CompactRawTableLongValueMaps> GetEnumerator() => throw new InvalidOperationException("Map traversal exceeded preflight bounds.");

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}
