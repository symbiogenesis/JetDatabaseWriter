namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using Xunit;

/// <summary>Stored bookmarks must resolve before they can support a compact copy.</summary>
public sealed class CompactIndexBookmarkAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task StoredBookmark_ProvesOriginalHeaderAndResolvedPayloadWithoutMutatingSource(DatabaseFormat kind, bool moved)
    {
        var source = new MemoryPages(kind);
        byte[] payload = moved ? [0, 5, 0, 0] : [0xFE, 0xFF, 0, 0x80];
        source.SetRows(4, (moved ? 0x4000 : 0, payload));
        if (moved)
        {
            source.SetRows(5, (0x8000, new byte[] { 0xFE, 0xFF, 0, 0x80 }));
        }

        byte[][] original = source.Images.Values.Select(static page => page.ToArray()).ToArray();
        var records = new List<CompactIndexRecord>();
        await ScanAsync(source, records);
        CompactIndexRecord entry = Assert.Single(records, static record => record.Kind == "leaf-entry");
        Assert.True(entry.BookmarkTargetValidated);
        Assert.Equal(4, entry.TargetPageNumber);
        Assert.Equal((byte)0, entry.TargetRowIndex);
        Assert.Equal(moved ? 5 : 4, entry.BookmarkDataPageNumber);
        Assert.Equal(0, entry.BookmarkDataRowIndex);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.BookmarkTargetsValidated);
        Assert.False(terminal.TreeCompletenessProven);
        Assert.All(records, static record => Assert.True(record.IsPinned));
        Assert.Equal(moved ? 3 : 2, source.Reads);
        for (int index = 0; index < original.Length; index++)
        {
            Assert.Equal(original[index], source.Images.Values.ElementAt(index));
        }
    }

    [Fact]
    public async Task IntermediateBookmark_ReceivesTheSameTargetProofAsLeafEntries()
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        source.Images[3] = IndexPageCodec.BuildIntermediatePage(source.Format.IndexPage, source.PageSize, 2, [new(new([1], 4, 0), 5)], 0, 0, 0);
        source.Images[5] = IndexPageCodec.BuildLeafPage(source.Format.IndexPage, source.PageSize, 2, [new([1], 4, 0)], enablePrefixCompression: false);
        var records = new List<CompactIndexRecord>();
        await ScanAsync(source, records);
        Assert.True(Assert.Single(records, static record => record.Kind == "intermediate-entry").BookmarkTargetValidated);
        Assert.True(Assert.Single(records, static record => record.Kind == "leaf-entry").BookmarkTargetValidated);
        Assert.True(Assert.Single(records, static record => record.Kind == "index-terminal").BookmarkTargetsValidated);
    }

    [Fact]
    public async Task RecoveredCatalogBookmark_DisclosesRecoveryAndWithholdsSameOwnerProof()
    {
        var source = new MemoryPages(DatabaseFormat.AceAccdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        source.Images[4][4] = 0;
        var records = new List<CompactIndexRecord>();
        await ScanAsync(source, records, recoveredCatalogResolver: (page, row, token) =>
        {
            token.ThrowIfCancellationRequested();
            Assert.Equal(4, page);
            Assert.Equal(0, row);
            return ValueTask.FromResult<RowLocation?>(new(4, 0, source.PageSize - 2, 2));
        });
        CompactIndexRecord entry = Assert.Single(records, static record => record.Kind == "leaf-entry");
        Assert.True(entry.BookmarkTargetRecovered);
        Assert.False(entry.BookmarkTargetValidated);
        CompactIndexRecord terminal = Assert.Single(records, static record => record.Kind == "index-terminal");
        Assert.True(terminal.BookmarkTargetsRecovered);
        Assert.False(terminal.BookmarkTargetsValidated);
        Assert.Contains("same-owner proof is withheld", terminal.Coverage, StringComparison.Ordinal);
        Assert.All(records, static record => Assert.True(record.IsPinned));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnlistedOrMismatchedRecovery_RefusesWithoutASuccessfulTerminal(bool mismatched)
    {
        var source = new MemoryPages(DatabaseFormat.AceAccdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        source.Images[4][4] = 0;
        var records = new List<CompactIndexRecord>();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(
            source,
            records,
            recoveredCatalogResolver: (_, _, _) => ValueTask.FromResult<RowLocation?>(mismatched ? new(5, 0, source.PageSize - 2, 2) : null)));
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
        Assert.DoesNotContain(records, static record => record.BookmarkTargetRecovered);
    }

    [Fact]
    public async Task SamePageMovedPayload_KeepsTheOriginalHeaderSlot()
    {
        var source = new MemoryPages(DatabaseFormat.Jet3Mdb);
        source.SetRows(4, (0x4000, new byte[] { 1, 4, 0, 0 }), (0x8000, new byte[] { 0xFE, 0xFF, 0, 0x80 }));
        var records = new List<CompactIndexRecord>();
        await ScanAsync(source, records);
        CompactIndexRecord entry = Assert.Single(records, static record => record.Kind == "leaf-entry");
        Assert.Equal((byte)0, entry.TargetRowIndex);
        Assert.Equal(4, entry.BookmarkDataPageNumber);
        Assert.Equal(1, entry.BookmarkDataRowIndex);
        Assert.Equal(3, source.Reads);
    }

    [Theory]
    [InlineData("page-type")]
    [InlineData("owner")]
    [InlineData("missing-slot")]
    [InlineData("deleted-header")]
    public async Task InvalidBookmarkTarget_RefusesBeforeTerminal(string damage)
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        switch (damage)
        {
            case "page-type":
                source.Images[4][0] = 0;
                break;
            case "owner":
                source.Images[4][4] = 7;
                break;
            case "missing-slot":
                source.SetRoot(4, 1);
                break;
            default:
                BinaryPrimitives.WriteUInt16LittleEndian(source.Images[4].AsSpan(source.Format.DataPage.RowsStart), checked((ushort)(source.Format.PageSize - 2 + 0x8000)));
                break;
        }

        var records = new List<CompactIndexRecord>();
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, records));
        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        Assert.Equal(4, error.ErrorInfo.PageNumber);
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
    }

    [Theory]
    [InlineData("count")]
    [InlineData("offset")]
    [InlineData("flags")]
    [InlineData("live-alias")]
    public async Task InvalidTargetDirectory_RefusesWithoutClamping(string damage)
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }), (0, new byte[] { 3, 4 }));
        int offset = source.Format.DataPage.RowsStart;
        switch (damage)
        {
            case "count":
                BinaryPrimitives.WriteUInt16LittleEndian(source.Images[4].AsSpan(source.Format.DataPage.NumRows), ushort.MaxValue);
                break;
            case "offset":
                BinaryPrimitives.WriteUInt16LittleEndian(source.Images[4].AsSpan(offset + 2), 1);
                break;
            case "flags":
                BinaryPrimitives.WriteUInt16LittleEndian(source.Images[4].AsSpan(offset + 2), 0x2000);
                break;
            default:
                source.Images[4].AsSpan(offset, 2).CopyTo(source.Images[4].AsSpan(offset + 2));
                break;
        }

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, []));
        Assert.Equal(4, error.ErrorInfo.PageNumber);
    }

    [Theory]
    [InlineData("cycle")]
    [InlineData("live-target")]
    [InlineData("live-alias")]
    [InlineData("truncated-pointer")]
    [InlineData("outside-source")]
    public async Task InvalidMovedPayload_RefusesAmbiguousOrUnresolvedTargets(string damage)
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0x4000, new byte[] { 0, 5, 0, 0 }));
        source.SetRows(5, (0x8000, new byte[] { 1, 2, 3 }));
        switch (damage)
        {
            case "cycle":
                source.SetRows(5, (0xC000, new byte[] { 0, 4, 0, 0 }));
                break;
            case "live-target":
                source.SetRows(5, (0, new byte[] { 1, 2, 3 }));
                break;
            case "live-alias":
                source.SetRows(5, (0x8000, new byte[] { 1, 2, 3 }), (0, new byte[] { 4, 5 }));
                BinaryPrimitives.WriteUInt16LittleEndian(source.Images[5].AsSpan(source.Format.DataPage.RowsStart + 2), checked((ushort)(source.PageSize - 3)));
                break;
            case "truncated-pointer":
                source.SetRows(4, (0x4000, new byte[] { 0, 5 }));
                break;
            default:
                source.SetRows(4, (0x4000, new byte[] { 0, 30, 0, 0 }));
                break;
        }

        var records = new List<CompactIndexRecord>();
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, records));
        Assert.DoesNotContain(records, static record => record.BookmarkTargetsValidated);
        Assert.DoesNotContain(records, static record => record.BookmarkTargetValidated);
    }

    [Theory]
    [InlineData("reads")]
    [InlineData("slots")]
    [InlineData("hops")]
    [InlineData("working")]
    public async Task TargetLimits_RefuseBeforeExceedingAllowance(string limit)
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0x4000, new byte[] { 0, 5, 0, 0 }));
        source.SetRows(5, (0xC000, new byte[] { 0, 6, 0, 0 }));
        source.SetRows(6, (0x8000, new byte[] { 1, 2 }));
        CompactIndexBookmarkBudget budget = limit switch
        {
            "reads" => new() { MaxPageReads = 1 },
            "slots" => new() { MaxDirectorySlots = 1 },
            "hops" => new() { MaxOverflowHops = 1 },
            _ => new() { MaxWorkingBytes = 1 },
        };
        var records = new List<CompactIndexRecord>();
        JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, records, budget));
        Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
        Assert.DoesNotContain(records, static record => record.Kind == "index-terminal");
        int expectedReads = limit switch { "working" => 0, "reads" => 2, _ => 3 };
        Assert.Equal(expectedReads, source.Reads);
    }

    [Fact]
    public async Task RepeatedBookmarks_ConsumeRepeatedReadAndDirectoryAllowances()
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        source.Images[3] = IndexPageCodec.BuildLeafPage(source.Format.IndexPage, source.PageSize, 2, [new([1], 4, 0), new([2], 4, 0)], enablePrefixCompression: false);
        var records = new List<CompactIndexRecord>();
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ScanAsync(source, records, new() { MaxPageReads = 1 }));
        Assert.Equal(2, source.Reads);
        Assert.Single(records, static record => record.BookmarkTargetValidated);
        Assert.DoesNotContain(records, static record => record.BookmarkTargetsValidated);
        records.Clear();
        await ScanAsync(source, records, new() { MaxPageReads = 2, MaxDirectorySlots = 2 });
        Assert.Equal(2, records.Count(static record => record.BookmarkTargetValidated));
    }

    [Fact]
    public async Task TargetIoCancellationAndGeometryChanges_PreserveFailureAndNeverComplete()
    {
        var source = new MemoryPages(DatabaseFormat.Jet4Mdb);
        source.SetRows(4, (0, new byte[] { 1, 2 }));
        var failure = new IOException("bookmark target read failed");
        source.TargetFailure = failure;
        IOException actual = await Assert.ThrowsAsync<IOException>(async () => await ScanAsync(source, []));
        Assert.Same(failure, actual);
        source.TargetFailure = null;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        source.OnTargetRead = cancellation.Cancel;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ScanAsync(source, [], cancellationToken: cancellation.Token));
        source.OnTargetRead = () => source.PageCount++;
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ScanAsync(source, []));
    }

    private static ValueTask ScanAsync(MemoryPages source, List<CompactIndexRecord> records, CompactIndexBookmarkBudget? budget = null, CancellationToken? cancellationToken = null, Func<long, int, CancellationToken, ValueTask<RowLocation?>>? recoveredCatalogResolver = null)
        => CompactIndexBookmarkAudit.ScanAsync(
            source,
            source.Format,
            2,
            3,
            new(),
            budget ?? new(),
            (record, _) =>
            {
                records.Add(record);
                return ValueTask.CompletedTask;
            },
            cancellationToken ?? Ct,
            recoveredCatalogResolver);

    private sealed class MemoryPages : IPageSource
    {
        internal MemoryPages(DatabaseFormat kind)
        {
            this.Format = JetFormat.ForNewDatabase(kind);
            this.SetRoot(4, 0);
        }

        public int PageSize => this.Format.PageSize;

        public long PageCount { get; set; } = 20;

        public bool IsDisposed => false;

        internal JetFormat Format { get; }

        internal Dictionary<long, byte[]> Images { get; } = [];

        internal int Reads { get; private set; }

        internal Exception? TargetFailure { get; set; }

        internal Action? OnTargetRead { get; set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.Reads++;
            if (pageNumber != 3)
            {
                if (this.TargetFailure is { } failure)
                {
                    return ValueTask.FromException<byte[]>(failure);
                }

                this.OnTargetRead?.Invoke();
            }

            byte[] result = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.Images[pageNumber].CopyTo(result, 0);
            return ValueTask.FromResult(result);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();

        internal void SetRoot(long target, byte row)
            => this.Images[3] = IndexPageCodec.BuildLeafPage(this.Format.IndexPage, this.PageSize, 2, [new([1], target, row)], enablePrefixCompression: false);

        internal void SetRows(long pageNumber, params (int Flags, byte[] Bytes)[] rows)
        {
            byte[] page = new byte[this.PageSize];
            page[0] = 1;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(this.Format.DataPage.TDefOff), 2);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.NumRows), checked((ushort)rows.Length));
            int offset = this.PageSize;
            for (int index = 0; index < rows.Length; index++)
            {
                offset -= rows[index].Bytes.Length;
                rows[index].Bytes.CopyTo(page, offset);
                BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(this.Format.DataPage.RowsStart + (index * 2)), checked((ushort)(offset | rows[index].Flags)));
            }

            this.Images[pageNumber] = page;
        }
    }
}
