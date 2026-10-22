namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>One private coordinator validates all local tables, complete allocations and the retained prefix.</summary>
public sealed class CompactPinnedDatabaseValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task AllLocalTables_CompareWithoutAuthorizingPublication(DatabaseFormat kind)
        => WithFixtureAsync(kind, async fixture =>
        {
            fixture.Destination.Position = 17;
            fixture.Ledger.Position = 11;
            CompactPinnedDatabaseValidatorResult result = await ValidateAsync(fixture, new());
            Assert.Equal(fixture.Tables.Count, result.Tables);
            Assert.Equal(fixture.Source.Database.Pages.PageCount, result.Prefix.PinnedPages);
            Assert.Equal(fixture.Destination.Length / fixture.Source.Database.Format.PageSize, result.Allocations.DestinationPages);
            Assert.True(result.ReadBytes > result.Prefix.ReadBytes);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.OwnershipProven);
            Assert.False(result.ReclamationAuthorized);
            Assert.Equal(17, fixture.Destination.Position);
            Assert.Equal(11, fixture.Ledger.Position);
        });

    [Theory]
    [InlineData("prefix")]
    [InlineData("table")]
    [InlineData("global-map")]
    [InlineData("allocation-ledger")]
    public Task ChangedReopenedStorage_IsRefused(string mutation)
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            Stream target = mutation == "allocation-ledger" ? fixture.Ledger : fixture.Destination;
            long offset = mutation switch
            {
                "prefix" => 16,
                "global-map" => fixture.Source.Database.Format.PageSize + 10,
                "allocation-ledger" => 48,
                _ => (fixture.Tables[^1].Allocation.Table.Data.FirstPageNumber * fixture.Source.Database.Format.PageSize) + 1,
            };
            target.Position = offset;
            int original = target.ReadByte();
            target.Position = offset;
            target.WriteByte((byte)(original ^ 1));
            fixture.Destination.Position = 17;
            fixture.Ledger.Position = 11;
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(fixture, new()));
            Assert.Equal(17, fixture.Destination.Position);
            Assert.Equal(11, fixture.Ledger.Position);
        });

    [Fact]
    public Task MissingLocalTable_IsRefusedBeforeOpeningItsLedgers()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.Tables.RemoveAt(fixture.Tables.Count - 1);
            int opens = 0;
            for (int index = 0; index < fixture.Tables.Count; index++)
            {
                CompactPinnedDatabaseTable table = fixture.Tables[index];
                fixture.Tables[index] = table with
                {
                    OpenRows = token =>
                    {
                        opens++;
                        return table.OpenRows(token);
                    },
                };
            }

            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(fixture, new()));
            Assert.Equal(0, opens);
        });

    [Fact]
    public Task PartialReadRetries_ConsumeWholeRequestsAcrossPhases()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            CompactPinnedDatabaseValidatorResult completed = await ValidateAsync(fixture, new());
            await using var partial = new ReadProbe(fixture.Destination);
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture with { Destination = partial }, new() { MaxReadBytes = completed.ReadBytes }));
            Assert.True(partial.RequestedBytes > partial.ReturnedBytes);
        });

    [Fact]
    public Task PrivateReadFailure_PropagatesAndRestoresBorrowedPositions()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.Destination.Position = 17;
            fixture.Ledger.Position = 11;
            await using var failing = new ReadProbe(fixture.Destination, fail: true);
            await Assert.ThrowsAsync<IOException>(async () => await ValidateAsync(fixture with { Destination = failing }, new()));
            Assert.Equal(17, fixture.Destination.Position);
            Assert.Equal(11, fixture.Ledger.Position);
        });

    [Fact]
    public Task FactoryReturningBorrowedDestination_IsRefusedWithoutDisposingIt()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.Tables[0] = fixture.Tables[0] with { OpenRows = _ => ValueTask.FromResult(fixture.Destination) };
            await Assert.ThrowsAsync<ArgumentException>(async () => await ValidateAsync(fixture, new()));
            Assert.True(fixture.Destination.CanRead);
        });

    [Fact]
    public Task ReadAdmission_IsCumulativeAcrossInventoryTablesPrefixAndAllocationLedger()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            CompactPinnedDatabaseValidatorResult completed = await ValidateAsync(fixture, new());
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture, new() { MaxReadBytes = completed.ReadBytes - 1 }));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture, new() { MaxReadBytes = 0 }));
        });

    [Fact]
    public Task PhysicalMembershipAndRetainedStorageAdmission_RefuseBeforeComparison()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture, new() { MaxPages = fixture.Source.Database.Pages.PageCount - 1 }));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture, new() { MaxTables = fixture.Tables.Count - 1 }));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(fixture, new() { MaxWorkingBytes = 4096 }));
        });

    [Fact]
    public Task CancellationDuringLedgerOpen_DisposesOpenedStreamsAndLeavesSourceUnchanged()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            CompactPinnedDatabaseTable table = fixture.Tables[0];
            await using var opened = new MemoryStream();
            fixture.Tables[0] = table with
            {
                OpenRows = _ => ValueTask.FromResult<Stream>(opened),
                OpenLongValues = async _ =>
                {
                    await cancellation.CancelAsync();
                    cancellation.Token.ThrowIfCancellationRequested();
                    return new MemoryStream();
                },
            };
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ValidateAsync(fixture, new(), cancellation.Token));
            Assert.False(opened.CanRead);
        });

    private static ValueTask<CompactPinnedDatabaseValidatorResult> ValidateAsync(Fixture fixture, CompactPinnedDatabaseValidatorBudget budget, CancellationToken? token = null)
        => CompactPinnedDatabaseValidator.ValidateAsync(
            fixture.Source.Database.Pages,
            fixture.Source.Database.Format,
            fixture.Tables,
            fixture.Destination,
            new(fixture.AppendPoint, new()),
            fixture.Ledger,
            fixture.Allocation,
            budget,
            token ?? Ct);

    private static async Task WithFixtureAsync(DatabaseFormat kind, Func<Fixture, Task> action)
    {
        await using var original = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(original, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            foreach (string name in new[] { "Indexed", "Plain" })
            {
                await writer.CreateTableAsync(name, [new("Id", typeof(int)) { IsPrimaryKey = name == "Indexed" }, new("Memo", typeof(string), 20_000)], Ct);
                await writer.InsertRowsAsync(name, Enumerable.Range(0, 3).Select(id => new object?[] { id, name + new string('a', 3000) }), Ct);
            }
        }

        byte[] sourceBytes = original.ToArray();
        await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage catalog = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
        var reader = new CatalogRowReader(format, source.Database.TableDefs, source.Database.OwnedPages);
        List<CatalogRow> entries = await reader.GetCatalogRowsAsync(catalog.Definition, Ct);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(sourceBytes, Ct);
        var tables = new List<CompactPinnedDatabaseTable>();
        var assembly = new CompactPinnedTableAssemblySession(source.Database.Pages, format, new());
        foreach (CatalogRow entry in entries.Where(static entry => entry.ObjectType == Constants.SystemObjects.UserTableType).OrderBy(static entry => entry.TDefPage))
        {
            long owner = entry.TDefPage;
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
            CompactPinnedTableIndexSource[] indexes = await ObserveAsync(source, owner, image);
            await using var rows = new MemoryStream();
            await using var values = new MemoryStream();
            await using var first = new MemoryStream();
            await using var second = new MemoryStream();
            CompactPinnedTableAssemblyAllocation allocation = await CompactPinnedTableAssembly.BuildAsync(assembly, owner, image, indexes, destination, rows, values, first, second, Ct);
            byte[] rowBytes = rows.ToArray();
            byte[] valueBytes = values.ToArray();
            tables.Add(new(
                owner,
                image,
                indexes,
                allocation,
                _ => ValueTask.FromResult<Stream>(new MemoryStream(rowBytes, writable: false)),
                _ => ValueTask.FromResult<Stream>(new MemoryStream(valueBytes, writable: false))));
        }

        long appendPoint = destination.Length / format.PageSize;
        CompactGlobalMapResult global = await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, format, appendPoint, destination, new(), Ct);
        await using var ledger = new MemoryStream();
        CompactGlobalAllocationBuilderResult allocations = await CompactGlobalAllocationBuilder.BuildAsync(format, source.Database.Pages.PageCount, AllocationsAsync(tables, global, Ct), destination, ledger, new(), Ct);
        try
        {
            await action(new(source, tables, destination, appendPoint, ledger, allocations));
        }
        finally
        {
            Assert.Equal(sourceBytes, original.ToArray());
        }
    }

    private static async Task<CompactPinnedTableIndexSource[]> ObserveAsync(AccessReader source, long owner, TDefImage image)
    {
        var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
        for (int slot = 0; slot < indexes.Length; slot++)
        {
            TDefIndexDescriptor descriptor = image.RealIndexes[slot];
            Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
            List<IndexEntry> entries = await IndexLeafChain.ReadEntriesAsync(source.Database, owner, descriptor.RootPage, Ct);
            indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(entries, Ct));
        }

        return indexes;
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

    private static async IAsyncEnumerable<CompactGlobalAllocationRecord> AllocationsAsync(IEnumerable<CompactPinnedDatabaseTable> tables, CompactGlobalMapResult global, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (CompactPinnedDatabaseTable table in tables)
        {
            await foreach (CompactGlobalAllocationRecord record in table.Allocation.AllocationsAsync(token))
            {
                yield return record;
            }
        }

        token.ThrowIfCancellationRequested();
        if (global.BitmapPages > 0)
        {
            yield return new(CompactGlobalAllocationKind.UsageMap, 1, new(global.AppendPointPages, global.BitmapPages));
        }
    }

    private sealed record Fixture(AccessReader Source, List<CompactPinnedDatabaseTable> Tables, Stream Destination, long AppendPoint, MemoryStream Ledger, CompactGlobalAllocationBuilderResult Allocation);

    private sealed class ReadProbe(Stream inner, bool fail = false) : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => inner.Length;

        public override long Position { get => inner.Position; set => inner.Position = value; }

        internal long RequestedBytes { get; private set; }

        internal long ReturnedBytes { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            this.RequestedBytes += count;
            if (fail)
            {
                throw new IOException("Injected private destination read failure.");
            }

            int read = inner.Read(buffer, offset, Math.Min(1, count));
            this.ReturnedBytes += read;
            return read;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.RequestedBytes += buffer.Length;
            if (fail)
            {
                throw new IOException("Injected private destination read failure.");
            }

            int read = await inner.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
            this.ReturnedBytes += read;
            return read;
        }

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void Flush() => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
