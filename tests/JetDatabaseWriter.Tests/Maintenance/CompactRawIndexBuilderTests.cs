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
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Private raw-key index staging preserves bytes without whole-tree snapshots.</summary>
public sealed class CompactRawIndexBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MultiLevelTree_PreservesKeysRelocatedBookmarksAndSiblingFences(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexEntry[] entries = Enumerable.Range(0, 600).Select(static i => Entry(i, 300)).ToArray();
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult result = await BuildAsync(format, entries, destination, first, second);
        Assert.True(result.Levels >= 3);
        Assert.False(result.IsValidated);
        Assert.Equal(entries.Length, result.EntryCount);
        Assert.Equal(3, result.FirstPageNumber);
        Assert.Equal(3L + result.PageCount, destination.Length / format.PageSize);
        List<IndexEntry> actual = ReadLeaves(format, destination, result);
        AssertEntries(entries, actual);
        ValidateFences(format, destination, result.RootPageNumber, result.LastLeafPageNumber);
        Assert.Equal(new byte[format.PageSize * 3], destination.ToArray().AsSpan(0, format.PageSize * 3).ToArray());
    }

    [Fact]
    public async Task DuplicateKeys_RequireIncreasingRelocatedBookmarks()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] entries = [new([1, 2], 5, 1), new([1, 2], 5, 2), new([1, 2], 6, 0)];
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult result = await BuildAsync(format, entries, destination, first, second);
        AssertEntries(entries, ReadLeaves(format, destination, result));
        foreach (IndexEntry[] invalid in (IndexEntry[][])[entries.AsEnumerable().Reverse().ToArray(), [entries[0], entries[0]], [new([2], 5, 0), new([1], 5, 0)]])
        {
            await using MemoryStream rejected = Destination(format);
            await using var scratchA = new MemoryStream();
            await using var scratchB = new MemoryStream();
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(format, invalid, rejected, scratchA, scratchB));
        }
    }

    [Fact]
    public async Task BorrowedInputKey_IsCopiedBeforeEnumeratorAdvances()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult result = await CompactRawIndexBuilder.BuildAsync(format, 2, ReusedKeyAsync(Ct), destination, first, second, new(), Ct);
        Assert.Equal<byte>([1, 2, 3], ReadLeaves(format, destination, result).Select(static entry => entry.Key[0]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task ChangedBookmarkPrefix_RebuildsCanonicalKeysWithoutPatchingCompressedStorage(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] native = new byte[format.PageSize];
        native[0] = 4;
        BinaryPrimitives.WriteUInt32LittleEndian(native.AsSpan(4, 4), 2);
        native[format.IndexPage.PrefLenOffset] = 5;
        byte[] full = [0x7F, 0x80, 0, 0, 5, 1];
        full.CopyTo(native, format.IndexPage.FirstEntryOffset);
        native[format.IndexPage.FirstEntryOffset + full.Length] = 2;
        native[format.IndexPage.BitmaskOffset] = 0xC0;
        BinaryPrimitives.WriteUInt16LittleEndian(native.AsSpan(2, 2), checked((ushort)(format.PageSize - format.IndexPage.FirstEntryOffset - 7)));
        List<IndexEntry> original = IndexPageCodec.DecodeLeafEntries(format.IndexPage, native, format.PageSize);
        Assert.Equal(2, original.Count);
        IndexEntry[] relocated = [original[0] with { DataPage = 0x010203 }, original[1] with { DataPage = 0x050607 }];
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult result = await BuildAsync(format, relocated, destination, first, second);
        AssertEntries(relocated, ReadLeaves(format, destination, result));
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task MicrosoftAuthoredCatalogKeys_RebuildWithIdenticalCanonicalFields(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(stream, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(file, 2, Ct);
        int observed = 0;
        foreach (long root in roots.Where(static root => root > 0))
        {
            var entries = new List<IndexEntry>();
            await CompactIndexAudit.ScanAsync(
                file.Pages,
                file.Format,
                2,
                root,
                new(),
                (record, _) =>
                {
                    if (record.Kind == "leaf-entry")
                    {
                        entries.Add(new(record.CanonicalKey!, record.TargetPageNumber!.Value, record.TargetRowIndex!.Value));
                    }

                    return default;
                },
                Ct);
            entries.Sort(Compare);
            await using MemoryStream destination = Destination(file.Format);
            await using var first = new MemoryStream();
            await using var second = new MemoryStream();
            CompactRawIndexBuilderResult result = await BuildAsync(file.Format, entries, destination, first, second);
            AssertEntries(entries, ReadLeaves(file.Format, destination, result));
            observed += entries.Count;
        }

        Assert.True(observed > 0);
        Assert.Equal(image, stream.ToArray());
    }

    [Theory]
    [InlineData("entries")]
    [InlineData("canonical")]
    [InlineData("pages")]
    [InlineData("output")]
    [InlineData("fences")]
    [InlineData("working")]
    [InlineData("levels")]
    public async Task Limits_RefuseWithoutReturningACompletedTree(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        CompactRawIndexBuilderBudget budget = limit switch
        {
            "entries" => new() { MaxEntries = 1 },
            "canonical" => new() { MaxCanonicalBytes = 1 },
            "pages" => new() { MaxTreePages = 1 },
            "output" => new() { MaxOutputBytes = 1 },
            "fences" => new() { MaxFenceBytes = 1 },
            "working" => new() { MaxWorkingBytes = 1 },
            "levels" => new() { MaxLevels = 1 },
            _ => throw new InvalidOperationException(),
        };
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(format, Enumerable.Range(0, 600).Select(static i => Entry(i, 300)), destination, first, second, budget));
        Assert.Equal(new byte[format.PageSize * 3], destination.ToArray().AsSpan(0, format.PageSize * 3).ToArray());
    }

    [Fact]
    public async Task OversizedAndNonreducingEntries_RefuseBoundedConstruction()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        foreach (int length in new[] { format.PageSize, 2000 })
        {
            await using MemoryStream destination = Destination(format);
            await using var first = new MemoryStream();
            await using var second = new MemoryStream();
            await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(format, Enumerable.Range(0, 5).Select(i => Entry(i, length)), destination, first, second));
        }
    }

    [Fact]
    public async Task EmptyInput_EmitsOneUnvalidatedEmptyLeaf()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult result = await BuildAsync(format, [], destination, first, second);
        Assert.Equal(1, result.PageCount);
        Assert.Equal(0, result.EntryCount);
        Assert.Empty(ReadLeaves(format, destination, result));
    }

    [Fact]
    public async Task AliasedOrOccupiedScratch_RefusesBeforeDestinationWrites()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, [], destination, destination, second));
        await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, [], destination, first, first));
        first.WriteByte(1);
        await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, [], destination, first, second));
        Assert.Equal(3L * format.PageSize, destination.Length);
    }

    [Fact]
    public async Task CancellationAndIoFailure_PropagateAndNeverProduceAResult()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawIndexBuilder.BuildAsync(format, 2, InputAsync([], Ct), destination, first, second, new(), cancelled.Token));
        await using var fault = new FaultStream();
        fault.SetLength(3L * format.PageSize);
        IOException error = await Assert.ThrowsAsync<IOException>(async () => await BuildAsync(format, [Entry(1, 8)], fault, first, second));
        Assert.Same(fault.Error, error);
        Assert.True(destination.CanWrite);
        Assert.True(first.CanWrite);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationDuringFinalWriteOrFlush_PreventsSuccess(bool duringFlush)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        using var cancellation = new CancellationTokenSource();
        await using var destination = new CancellingStream(cancellation, duringFlush);
        destination.SetLength(3L * format.PageSize);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawIndexBuilder.BuildAsync(format, 2, InputAsync([], Ct), destination, first, second, new(), cancellation.Token));
        Assert.Equal(4L * format.PageSize, destination.Length);
    }

    [Fact]
    public async Task FenceReadFailure_PreservesExceptionIdentity()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Destination(format);
        await using var first = new ReadFaultStream();
        await using var second = new MemoryStream();
        IOException error = await Assert.ThrowsAsync<IOException>(async () => await BuildAsync(format, Enumerable.Range(0, 60).Select(static i => Entry(i, 300)), destination, first, second));
        Assert.Same(first.Error, error);
    }

    [Fact]
    public async Task DiskBackedFences_AlternateLevelsWithinTheSameByteAccounting()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        string directory = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "jdw-raw-index-" + Guid.NewGuid().ToString("N"))).FullName;
        try
        {
            await using MemoryStream destination = Destination(format);
            await using var first = new FileStream(Path.Combine(directory, "first.fences"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
            await using var second = new FileStream(Path.Combine(directory, "second.fences"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
            CompactRawIndexBuilderResult result = await BuildAsync(format, Enumerable.Range(0, 600).Select(static i => Entry(i, 300)), destination, first, second);
            Assert.True(result.Levels >= 3);
            Assert.Equal(((result.PageCount * 2) - 1) * 312, result.FenceBytes);
        }
        finally
        {
            Directory.Delete(directory);
        }
    }

    [Fact]
    public async Task FenceStreamsOpeningTheSamePath_RefuseBeforeAppending()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        string path = Path.Combine(Path.GetTempPath(), "jdw-raw-index-alias-" + Guid.NewGuid().ToString("N"));
        try
        {
            await using MemoryStream destination = Destination(format);
            await using var first = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.ReadWrite);
            await using var second = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
            await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(format, [], destination, first, second));
            Assert.Equal(3L * format.PageSize, destination.Length);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(0x1000000L)]
    public async Task InvalidRelocatedBookmark_RefusesBeforeAppending(long bookmark)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        await using MemoryStream destination = Destination(format);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(format, [new([1], bookmark, 0)], destination, first, second));
        Assert.Equal(3L * format.PageSize, destination.Length);
    }

    private static ValueTask<CompactRawIndexBuilderResult> BuildAsync(JetFormat format, IEnumerable<IndexEntry> entries, Stream destination, Stream first, Stream second, CompactRawIndexBuilderBudget? budget = null)
        => CompactRawIndexBuilder.BuildAsync(format, 2, InputAsync(entries, Ct), destination, first, second, budget ?? new(), Ct);

    private static MemoryStream Destination(JetFormat format)
    {
        var stream = new MemoryStream();
        stream.SetLength(3L * format.PageSize);
        return stream;
    }

    private static IndexEntry Entry(int value, int length)
    {
        byte[] key = new byte[length];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return new(key, 0x010203 + value, checked((byte)(value % 256)));
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

    private static async IAsyncEnumerable<IndexEntry> ReusedKeyAsync([EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        byte[] key = [0];
        for (byte i = 1; i <= 3; i++)
        {
            token.ThrowIfCancellationRequested();
            key[0] = i;
            yield return new(key, 9, i);
        }
    }

    private static List<IndexEntry> ReadLeaves(JetFormat format, MemoryStream destination, CompactRawIndexBuilderResult result)
    {
        var entries = new List<IndexEntry>();
        for (long page = result.FirstPageNumber; page <= result.LastLeafPageNumber; page++)
        {
            byte[] bytes = Page(format, destination, page);
            Assert.True(IndexPageCodec.IsLeaf(bytes));
            Assert.Equal(page == result.FirstPageNumber ? 0 : page - 1, IndexPageCodec.ReadPrevPage(format.IndexPage, bytes));
            Assert.Equal(page == result.LastLeafPageNumber ? 0 : page + 1, IndexPageCodec.ReadNextPage(format.IndexPage, bytes));
            entries.AddRange(IndexPageCodec.DecodeLeafEntries(format.IndexPage, bytes, format.PageSize));
        }

        return entries;
    }

    private static IndexEntry ValidateFences(JetFormat format, MemoryStream destination, long page, long tail)
    {
        byte[] bytes = Page(format, destination, page);
        Assert.Equal(2U, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(4, 4)));
        if (IndexPageCodec.IsLeaf(bytes))
        {
            return IndexPageCodec.DecodeLeafEntries(format.IndexPage, bytes, format.PageSize)[^1];
        }

        Assert.Equal(tail, IndexPageCodec.ReadTailPage(format.IndexPage, bytes));
        List<DecodedIntermediateEntry> children = IndexPageCodec.DecodeIntermediateEntries(format.IndexPage, bytes, format.PageSize);
        foreach (DecodedIntermediateEntry child in children)
        {
            IndexEntry actual = ValidateFences(format, destination, child.ChildPage, tail);
            AssertEntries([child.Entry], [actual]);
        }

        return children[^1].Entry;
    }

    private static byte[] Page(JetFormat format, MemoryStream destination, long page)
        => destination.ToArray().AsSpan(checked((int)(page * format.PageSize)), format.PageSize).ToArray();

    private static void AssertEntries(IEnumerable<IndexEntry> expected, IEnumerable<IndexEntry> actual)
        => Assert.Equal(expected.Select(static entry => (Convert.ToHexString(entry.Key), entry.DataPage, entry.DataRow)), actual.Select(static entry => (Convert.ToHexString(entry.Key), entry.DataPage, entry.DataRow)));

    private static int Compare(IndexEntry left, IndexEntry right)
    {
        int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
        if (key != 0)
        {
            return key;
        }

        int page = left.DataPage.CompareTo(right.DataPage);
        return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
    }

    private sealed class FaultStream : MemoryStream
    {
        internal IOException Error { get; } = new("Injected destination write failure.");

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => throw this.Error;
    }

    private sealed class ReadFaultStream : MemoryStream
    {
        internal IOException Error { get; } = new("Injected fence read failure.");

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => throw this.Error;
    }

    private sealed class CancellingStream(CancellationTokenSource cancellation, bool duringFlush) : MemoryStream
    {
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await base.WriteAsync(buffer, cancellationToken);
            if (!duringFlush)
            {
                await cancellation.CancelAsync();
            }
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            if (duringFlush)
            {
                await cancellation.CancelAsync();
            }
        }
    }
}
