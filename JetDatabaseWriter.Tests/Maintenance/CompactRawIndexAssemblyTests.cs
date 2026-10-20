namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CompactRawIndexAssemblyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReopenedMultilevelAssembly_PreservesOpaqueKeysAndExactTreeOwnership(DatabaseFormat kind, bool duplicateKeys)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexEntry[] originals = Entries(600, duplicateKeys);
        await using var destination = new MemoryStream();
        byte[] prefix = Enumerable.Repeat((byte)0xA5, 10 * format.PageSize).ToArray();
        await destination.WriteAsync(prefix, Ct);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, destination, originals);
        Assert.True(allocation.Tree.Levels >= 3);
        Assert.Equal(prefix, destination.ToArray().AsSpan(0, prefix.Length).ToArray());
        Assert.Equal(allocation.Tree.FirstPageNumber + allocation.Tree.PageCount, allocation.OwnedPages.FirstPageNumber);
        Assert.Equal(allocation.Tree.PageCount, allocation.OwnedPages.ListedPages);
        Assert.Equal(1, allocation.OwnedPages.InputRuns);

        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactRawIndexAssemblyResult result = await ValidateAsync(format, reopened, originals, allocation);
        Assert.Equal(600, result.Tree.EntryCount);
        Assert.Equal(allocation.Tree.PageCount, result.OwnedPages.ListedPages);
        Assert.Equal(checked((uint)allocation.Tree.RootPageNumber), result.References.RootPage);
        Assert.Equal(allocation.OwnedPages.Pointer, result.References.OwnedPages);
        Assert.True(destination.CanWrite);
        Assert.True(reopened.CanRead);
    }

    [Fact]
    public async Task EmptyIndex_StillOwnsItsSingleEmptyRoot()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Prefix(format);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, destination, []);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactRawIndexAssemblyResult result = await ValidateAsync(format, reopened, [], allocation);
        Assert.Equal(0, result.Tree.EntryCount);
        Assert.Equal(1, result.Tree.PageCount);
        Assert.Equal(1, result.OwnedPages.ListedPages);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TreeCrossingInlineWindow_ValidatesReferenceMapWithoutMapSelfMembership(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        int inlinePages = (format.UsageMapRowSize - Constants.UsageMap.InlineMapHeaderSize) * 8;
        await using var destination = new MemoryStream();
        destination.SetLength((inlinePages - 2L) * format.PageSize);
        IndexEntry[] originals = Entries(60, false);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, destination, originals);
        Assert.True(allocation.OwnedPages.PageCount > 1);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactRawIndexAssemblyResult result = await ValidateAsync(format, reopened, originals, allocation);
        Assert.Equal(allocation.Tree.PageCount, result.OwnedPages.ListedPages);
        Assert.Equal(allocation.OwnedPages.PageCount, result.OwnedPages.Pages);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task MicrosoftAuthoredIndex_ValidatesWithExactMapAndPreservedPinnedStorage(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var source = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(source, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(file, 2, Ct);
        var originals = new List<IndexEntry>();
        await CompactIndexAudit.ScanAsync(
            file.Pages,
            file.Format,
            2,
            roots.First(static root => root > 0),
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry")
                {
                    originals.Add(new(record.CanonicalKey!, record.TargetPageNumber!.Value, record.TargetRowIndex!.Value));
                }

                return ValueTask.CompletedTask;
            },
            Ct);
        originals.Sort(static (left, right) =>
        {
            int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
            if (key != 0)
            {
                return key;
            }

            int page = left.DataPage.CompareTo(right.DataPage);
            return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
        });
        Assert.NotEmpty(originals);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(image, Ct);
        await using var ledger = new MemoryStream();
        foreach (uint bookmark in originals.Select(static entry => checked((uint)((entry.DataPage << 8) | entry.DataRow))).Distinct().Order())
        {
            byte[] record = new byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(record, bookmark);
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), bookmark);
            ledger.Write(record);
        }

        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, ledger.Length / 8, 1_000_000, Ct);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexAssemblyAllocation allocation = await CompactRawIndexAssembly.BuildAsync(file.Format, 2, InputAsync(originals, Ct), relocations, destination, first, second, new(), Ct);
        Assert.Equal(image, destination.ToArray().AsSpan(0, image.Length).ToArray());
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await using var reopenedLedger = new MemoryStream(ledger.ToArray(), writable: false);
        CompactRowRelocationMap reopenedRelocations = await CompactRowRelocationMap.OpenAsync(reopenedLedger, reopenedLedger.Length / 8, 1_000_000, Ct);
        CompactRawIndexAssemblyResult result = await CompactRawIndexAssembly.ValidateAsync(file.Format, 2, reopened, InputAsync(originals, Ct), reopenedRelocations, allocation, new(), Ct);
        Assert.Equal(originals.Count, result.Tree.EntryCount);
        Assert.Equal(allocation.Tree.PageCount, result.OwnedPages.ListedPages);
    }

    [Theory]
    [InlineData("tree")]
    [InlineData("map")]
    [InlineData("gap")]
    [InlineData("overlap")]
    [InlineData("missing")]
    [InlineData("count")]
    public async Task ChangedTreeMapOrAllocation_RefusesReferences(string damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] originals = Entries(4, false);
        await using MemoryStream destination = Prefix(format);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, destination, originals);
        byte[] bytes = destination.ToArray();
        switch (damage)
        {
            case "tree":
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)(allocation.Tree.FirstPageNumber * format.PageSize)) + 4), 3);
                break;
            case "map":
                int bitmap = checked((int)(allocation.OwnedPages.FirstPageNumber * format.PageSize)) + format.PageSize - format.UsageMapRowSize + Constants.UsageMap.InlineBitmapOffset;
                bytes[bitmap] |= 1;
                break;
            case "gap":
                allocation = allocation with { OwnedPages = allocation.OwnedPages with { FirstPageNumber = allocation.OwnedPages.FirstPageNumber + 1 } };
                break;
            case "overlap":
                allocation = allocation with { OwnedPages = allocation.OwnedPages with { FirstPageNumber = allocation.Tree.FirstPageNumber } };
                break;
            case "missing":
                bytes = bytes[..^format.PageSize];
                break;
            case "count":
                allocation = allocation with { OwnedPages = allocation.OwnedPages with { ListedPages = 0 } };
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, originals, allocation));
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("output")]
    [InlineData("working")]
    [InlineData("fences")]
    public async Task AggregateConstructionAllowance_Refuses(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactRawIndexAssemblyBudget budget = limit switch
        {
            "pages" => new() { MaxPages = 1 },
            "output" => new() { MaxOutputBytes = format.PageSize },
            "working" => new() { MaxWorkingBytes = 1 },
            "fences" => new() { MaxFenceBytes = 1 },
            _ => throw new InvalidOperationException(),
        };
        await using MemoryStream destination = Prefix(format);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(format, destination, Entries(4, false), budget));
    }

    [Fact]
    public async Task AggregateValidationReads_ChargeTreeAndMapTogether()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] originals = Entries(4, false);
        await using MemoryStream destination = Prefix(format);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, destination, originals);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        CompactRawIndexAssemblyResult result = await ValidateAsync(format, reopened, originals, allocation);
        long total = result.Tree.ReadBytes + result.OwnedPages.ReadBytes;
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(format, reopened, originals, allocation, new() { MaxReadBytes = total - 1 }));
    }

    [Fact]
    public async Task IncompleteRelocationOrDifferentIndependentKeys_Refuse()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] originals = Entries(4, false);
        await using MemoryStream destination = Prefix(format);
        await using MemoryStream ledger = Ledger(originals[..^1]);
        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, 3, 4096, Ct);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawIndexAssembly.BuildAsync(format, 2, InputAsync(originals, Ct), relocations, destination, first, second, new(), Ct));

        await using MemoryStream complete = Prefix(format);
        CompactRawIndexAssemblyAllocation allocation = await BuildAsync(format, complete, originals);
        await using var reopened = new MemoryStream(complete.ToArray(), writable: false);
        IndexEntry[] changed = Entries(4, false);
        changed[^1].Key[^1]++;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, reopened, changed, allocation));
    }

    [Fact]
    public async Task RelocatedBookmarkIntoTreeAllocation_RefusesBeforeProducingTreePages()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Prefix(format);
        byte[] record = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(record, 100u << 8);
        BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), 10u << 8);
        await using var ledger = new MemoryStream(record, writable: false);
        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, 1, 4096, Ct);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawIndexAssembly.BuildAsync(format, 2, InputAsync(Entries(1, false), Ct), relocations, destination, first, second, new(), Ct));
        Assert.Equal(10L * format.PageSize, destination.Length);
    }

    [Fact]
    public async Task CancellationAtAdmission_LeavesPrivatePrefixUntouched()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Prefix(format);
        await using MemoryStream ledger = Ledger([]);
        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, 0, 4096, Ct);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawIndexAssembly.BuildAsync(format, 2, InputAsync([], cancellation.Token), relocations, destination, first, second, new(), cancellation.Token));
        Assert.Equal(10L * format.PageSize, destination.Length);
    }

    private static MemoryStream Prefix(JetFormat format)
    {
        var stream = new MemoryStream();
        stream.SetLength(10L * format.PageSize);
        return stream;
    }

    private static async ValueTask<CompactRawIndexAssemblyAllocation> BuildAsync(JetFormat format, Stream destination, IndexEntry[] originals, CompactRawIndexAssemblyBudget? budget = null)
    {
        await using MemoryStream ledger = Ledger(originals);
        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, originals.LongLength, 4_000_000, Ct);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        return await CompactRawIndexAssembly.BuildAsync(format, 2, InputAsync(originals, Ct), relocations, destination, first, second, budget ?? new(), Ct);
    }

    private static async ValueTask<CompactRawIndexAssemblyResult> ValidateAsync(JetFormat format, Stream destination, IndexEntry[] originals, CompactRawIndexAssemblyAllocation allocation, CompactRawIndexAssemblyBudget? budget = null)
    {
        await using MemoryStream ledger = Ledger(originals);
        CompactRowRelocationMap relocations = await CompactRowRelocationMap.OpenAsync(ledger, originals.LongLength, 4_000_000, Ct);
        return await CompactRawIndexAssembly.ValidateAsync(format, 2, destination, InputAsync(originals, Ct), relocations, allocation, budget ?? new(), Ct);
    }

    private static IndexEntry[] Entries(int count, bool duplicateKeys)
        => Enumerable.Range(0, count).Select(i =>
        {
            byte[] key = Enumerable.Repeat((byte)0x80, 300).ToArray();
            BinaryPrimitives.WriteInt32BigEndian(key, duplicateKeys ? 0 : i);
            return new IndexEntry(key, 100 + (i / 256), checked((byte)(i % 256)));
        }).ToArray();

    private static MemoryStream Ledger(IEnumerable<IndexEntry> originals)
    {
        var stream = new MemoryStream();
        foreach (IndexEntry entry in originals)
        {
            byte[] record = new byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(record, checked((uint)((entry.DataPage << 8) | entry.DataRow)));
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), checked((uint)(((entry.DataPage - 95) << 8) | entry.DataRow)));
            stream.Write(record);
        }

        return stream;
    }

    private static async IAsyncEnumerable<IndexEntry> InputAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }
}
