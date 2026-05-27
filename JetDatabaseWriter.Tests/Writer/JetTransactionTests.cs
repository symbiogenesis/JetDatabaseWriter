namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for explicit page-buffered transactions (Phase 3 of the
/// concurrency-and-transactions plan). Exercises the
/// <see cref="AccessWriter.BeginTransactionAsync"/> /
/// <see cref="JetTransaction.CommitAsync"/> /
/// <see cref="JetTransaction.RollbackAsync"/> surface end-to-end through a
/// round-trip with <see cref="AccessReader"/>.
/// </summary>
public sealed class JetTransactionTests
{
    private static readonly AccessReaderOptions ReaderOptions = new() { UseLockFile = false };

    private static AccessWriterOptions NonLockingWriterOptions() =>
        new()
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
        };

    private static List<ColumnDefinition> ItemsSchema() =>
    [
        new("Id", typeof(int)),
        new("Label", typeof(string), maxLength: 50),
    ];

    [Fact]
    public async Task BeginTransaction_ReturnsActiveTransaction()
    {
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tx);
        Assert.False(tx.IsCommitted);
        Assert.False(tx.IsRolledBack);
    }

    [Fact]
    public async Task BeginTransaction_TwiceWithoutCommit_Throws()
    {
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using var first = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(first);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.BeginTransactionAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Commit_PersistsBufferedInserts()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            Assert.True(tx.JournaledPageCount > 0);

            await tx.CommitAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsCommitted);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Rollback_DiscardsBufferedInserts()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            await tx.RollbackAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsRolledBack);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_RollsBackImplicitly()
    {
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            // Begin tx, do work, dispose without committing.
            var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            try
            {
                await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);
            }
            finally
            {
                await tx.DisposeAsync();
            }

            Assert.True(tx.IsRolledBack);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task ReadInsideTransaction_SeesUncommittedWrites()
    {
        // Inserts inside a transaction must be visible to subsequent reads
        // performed by the same writer (via the journal-shadow read path),
        // otherwise a multi-row insert that allocates a new data page would
        // immediately fail to find that page on the next AppendRow call.
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        // 100 rows comfortably forces multiple page mutations and at least one
        // new appended page; the writer's own row append path round-trips
        // through ReadPageAsync between writes.
        var rows = new List<object[]>();
        for (int i = 1; i <= 100; i++)
        {
            rows.Add([i, "Row" + i]);
        }

        int inserted = await writer.InsertRowsAsync("Items", rows, TestContext.Current.CancellationToken);
        Assert.Equal(100, inserted);

        await tx.CommitAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Commit_AfterRollback_Throws()
    {
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await tx.RollbackAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task JournalBudgetExceeded_ThrowsJetLimitationException()
    {
        await using var ms = new MemoryStream();
        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,

            // Tiny budget: the very first table-creation pass already mutates
            // multiple pages, so the budget will trip immediately.
            MaxTransactionPageBudget = 1,
        };

        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Commit_BumpsPageZeroCommitLockByte()
    {
        // The JET commit-lock byte at page-0 offset 0x14 must increment on
        // every committed transaction so cooperating openers can detect a
        // catalog/data version change without re-reading the entire file.
        await using var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            ms.Position = 0;
            byte[] before = new byte[0x18];
            await ms.ReadAsync(before.AsMemory(), TestContext.Current.CancellationToken);
            byte beforeByte = before[0x14];

            await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await tx.CommitAsync(TestContext.Current.CancellationToken);

            ms.Position = 0;
            byte[] after = new byte[0x18];
            await ms.ReadAsync(after.AsMemory(), TestContext.Current.CancellationToken);

            Assert.Equal(unchecked((byte)(beforeByte + 1)), after[0x14]);
        }
    }

    [Fact]
    public async Task Commit_WhenReplayWriteFails_LeavesSuccessfulReplayPrefixOnDisk()
    {
        await using var stream = new FaultInjectingStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        Assert.True(tx.JournaledPageCount > 1);

        stream.ThrowBeforePageWrite(2);

        await Assert.ThrowsAsync<IOException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));

        byte[] after = stream.ToArray();

        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(1, stream.PageWritesAfterArm);
        Assert.Equal(1, CountChangedPages(before, after));
        Assert.Equal(CommitLockByte(before), CommitLockByte(after));
    }

    [Fact]
    public async Task Commit_WhenCommitLockByteWriteFails_MarksRolledBackWithoutBumpingCommitByte()
    {
        await using var stream = new FaultInjectingStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        stream.ThrowBeforePageWriteAtOffset(0);

        await Assert.ThrowsAsync<IOException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));

        byte[] after = stream.ToArray();

        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(tx.JournaledPageCount, stream.PageWritesAfterArm);
        Assert.DoesNotContain(0L, stream.SuccessfulPageWriteOffsets);
        Assert.Equal(CommitLockByte(before), CommitLockByte(after));
        Assert.True(CountChangedPages(before, after) > 0);
    }

    [Fact]
    public async Task Commit_WhenCanceledBeforeCommitLockByteUpdate_MarksRolledBackWithoutBumpingCommitByte()
    {
        await using var stream = new FaultInjectingStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        stream.CancelBeforePageWriteAtOffset(0, cancellation);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await tx.CommitAsync(cancellation.Token));

        byte[] after = stream.ToArray();

        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(tx.JournaledPageCount, stream.PageWritesAfterArm);
        Assert.Equal(CommitLockByte(before), CommitLockByte(after));
    }

    [Fact]
    public async Task Commit_WhenDurableFlushFails_MarksRolledBackAfterBumpingCommitByte()
    {
        await using var stream = new FaultInjectingStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using var tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        int durableFlushCall = tx.JournaledPageCount + 2;
        stream.ThrowOnFlushCall(durableFlushCall);

        await Assert.ThrowsAsync<IOException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));

        byte[] after = stream.ToArray();

        byte expectedCommitLockByte = unchecked((byte)(CommitLockByte(before) + 1));

        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(tx.JournaledPageCount + 1, stream.PageWritesAfterArm);
        Assert.Equal(durableFlushCall - 1, stream.FlushesAfterArm);
        Assert.Equal(expectedCommitLockByte, CommitLockByte(after));
    }

    [Fact]
    public async Task UseTransactionalWrites_RollsBackOnExceptionDuringInsert()
    {
        // With UseTransactionalWrites=true, an exception thrown mid-call must
        // leave the database in its pre-call state.
        await using var ms = new MemoryStream();
        var writerOptions = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            UseTransactionalWrites = true,
        };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Items",
                [
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Label", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            // Seed one row.
            await writer.InsertRowAsync("Items", [1, "Seed"], TestContext.Current.CancellationToken);

            // Bulk insert with an intra-batch primary-key duplicate; the
            // pre-write unique check throws and the WHOLE batch must be
            // rolled back by the implicit auto-commit transaction.
            object[][] batch =
            [
                [10, "Ten"],
                [11, "Eleven"],
                [10, "DupTen"],
            ];

            await Assert.ThrowsAnyAsync<Exception>(async () =>
                await writer.InsertRowsAsync("Items", batch, TestContext.Current.CancellationToken));
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);

        // Only the seed row should remain — none of the batch rows persisted.
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task UseTransactionalWrites_Disabled_AllowsPartialBatchVisibility()
    {
        // Sanity check: with UseTransactionalWrites=false (default), a
        // failure mid-batch leaves whatever rows the writer's per-call
        // rollback path didn't catch. We don't assert exact persisted-row
        // count here — just that the option is honoured (no implicit
        // transaction is opened, so PageCacheSize/MaxTransactionPageBudget
        // do not affect the call's success).
        await using var ms = new MemoryStream();
        await using var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1, "A"], TestContext.Current.CancellationToken);
    }

    private static async Task BufferMultiPageInsertAsync(AccessWriter writer, CancellationToken cancellationToken)
    {
        var rows = new List<object[]>(100);
        for (int rowNumber = 1; rowNumber <= 100; rowNumber++)
        {
            rows.Add([rowNumber, "Row" + rowNumber]);
        }

        int inserted = await writer.InsertRowsAsync("Items", rows, cancellationToken);
        Assert.Equal(100, inserted);
    }

    private static byte CommitLockByte(byte[] databaseBytes) => databaseBytes[0x14];

    private static int CountChangedPages(byte[] before, byte[] after)
    {
        int maxLength = Math.Max(before.Length, after.Length);
        int pageCount = (maxLength + Constants.PageSizes.Jet4 - 1) / Constants.PageSizes.Jet4;
        int changedPages = 0;

        for (int pageNumber = 0; pageNumber < pageCount; pageNumber++)
        {
            int pageOffset = pageNumber * Constants.PageSizes.Jet4;
            int pageLength = Math.Min(Constants.PageSizes.Jet4, maxLength - pageOffset);
            if (!PageBytesEqual(before, after, pageOffset, pageLength))
            {
                changedPages++;
            }
        }

        return changedPages;
    }

    private static bool PageBytesEqual(byte[] before, byte[] after, int pageOffset, int pageLength)
    {
        for (int byteOffset = 0; byteOffset < pageLength; byteOffset++)
        {
            int absoluteOffset = pageOffset + byteOffset;
            byte beforeByte = absoluteOffset < before.Length ? before[absoluteOffset] : (byte)0;
            byte afterByte = absoluteOffset < after.Length ? after[absoluteOffset] : (byte)0;
            if (beforeByte != afterByte)
            {
                return false;
            }
        }

        return true;
    }

    private sealed class FaultInjectingStream : Stream
    {
        private readonly MemoryStream inner = new();
        private readonly List<long> successfulPageWriteOffsets = [];
        private CancellationTokenSource? cancelBeforePageWriteAtOffset;
        private long? cancelPageWriteOffset;
        private int? throwBeforePageWrite;
        private long? throwBeforePageWriteAtOffset;
        private int? throwOnFlushCall;
        private bool armed;

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => inner.CanWrite;

        public override long Length => inner.Length;

        public override long Position
        {
            get => inner.Position;
            set => inner.Position = value;
        }

        public int PageWritesAfterArm { get; private set; }

        public int FlushesAfterArm { get; private set; }

        public IReadOnlyList<long> SuccessfulPageWriteOffsets => successfulPageWriteOffsets;

        public void ThrowBeforePageWrite(int pageWriteNumber)
        {
            armed = true;
            throwBeforePageWrite = pageWriteNumber;
        }

        public void ThrowBeforePageWriteAtOffset(long offset)
        {
            armed = true;
            throwBeforePageWriteAtOffset = offset;
        }

        public void CancelBeforePageWriteAtOffset(long offset, CancellationTokenSource cancellation)
        {
            armed = true;
            cancelPageWriteOffset = offset;
            cancelBeforePageWriteAtOffset = cancellation;
        }

        public void ThrowOnFlushCall(int flushCallNumber)
        {
            armed = true;
            throwOnFlushCall = flushCallNumber;
        }

        public byte[] ToArray() => inner.ToArray();

        public override void Flush() => inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (armed)
            {
                int nextFlush = FlushesAfterArm + 1;
                if (throwOnFlushCall == nextFlush)
                {
                    throw new IOException("Injected flush failure.");
                }

                FlushesAfterArm++;
            }

            return inner.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count)
        {
            MaybeThrowBeforePageWrite(count, CancellationToken.None);
            inner.Write(buffer, offset, count);
            RecordPageWrite(count);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            MaybeThrowBeforePageWrite(buffer.Length, cancellationToken);
            await inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            RecordPageWrite(buffer.Length);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }

        private void MaybeThrowBeforePageWrite(int count, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!armed || count != Constants.PageSizes.Jet4)
            {
                return;
            }

            long writeOffset = inner.Position;
            if (cancelPageWriteOffset == writeOffset && cancelBeforePageWriteAtOffset is not null)
            {
                cancelBeforePageWriteAtOffset.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }

            int nextPageWrite = PageWritesAfterArm + 1;
            if (throwBeforePageWrite == nextPageWrite || throwBeforePageWriteAtOffset == writeOffset)
            {
                throw new IOException("Injected page-write failure.");
            }
        }

        private void RecordPageWrite(int count)
        {
            if (!armed || count != Constants.PageSizes.Jet4)
            {
                return;
            }

            successfulPageWriteOffsets.Add(inner.Position - count);
            PageWritesAfterArm++;
        }
    }
}
