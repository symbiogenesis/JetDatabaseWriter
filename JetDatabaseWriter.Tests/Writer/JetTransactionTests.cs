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
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(tx);
        Assert.False(tx.IsCommitted);
        Assert.False(tx.IsRolledBack);
    }

    [Fact]
    public async Task BeginTransaction_TwiceWithoutCommit_Throws()
    {
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction first = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(first);

        await Assert.ThrowsAsync<JetOperationException>(async () =>
            await writer.BeginTransactionAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Commit_PersistsBufferedInserts()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            Assert.True(tx.JournaledPageCount > 0);

            await tx.CommitAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsCommitted);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Rollback_DiscardsBufferedInserts()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, "Beta"], TestContext.Current.CancellationToken);

            await tx.RollbackAsync(TestContext.Current.CancellationToken);
            Assert.True(tx.IsRolledBack);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_RollsBackImplicitly()
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

            // Begin tx, do work, dispose without committing.
            JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
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
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
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
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

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
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await tx.RollbackAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetOperationException>(async () =>
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

        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            writerOptions,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Commit_PreservesPageZeroFormatVersionByte(DatabaseFormat format)
    {
        // Page-0 offset 0x14 is the Jet/ACE format version byte, not a
        // commit counter: changing it makes a Jet4 .mdb reopen as ACCDB.
        await using var ms = new MemoryStream();
        byte before;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
            before = FormatVersionByte(ms.ToArray());

            for (int id = 1; id <= 2; id++)
            {
                await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Items", [id, "Row" + id], TestContext.Current.CancellationToken);
                await tx.CommitAsync(TestContext.Current.CancellationToken);
            }
        }

        Assert.Equal(before, FormatVersionByte(ms.ToArray()));

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(format, reader.DatabaseFormat);
        Assert.Equal(2, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Commit_WhenReplayWriteFails_RestoresFileAndRollsBack()
    {
        await using var stream = new FaultInjectingStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        Assert.True(tx.JournaledPageCount > 1);

        stream.ThrowBeforePageWrite(2);

        await Assert.ThrowsAsync<IOException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));

        byte[] after = stream.ToArray();

        // Replay and flush failures restore every page and the original length.
        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(before, after);
        Assert.Equal(FormatVersionByte(before), FormatVersionByte(after));

        await Assert.ThrowsAsync<JetOperationException>(async () =>
            await tx.RollbackAsync(TestContext.Current.CancellationToken));
        await using JetTransaction next = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.False(next.IsRolledBack);
    }

    [Fact]
    public async Task Commit_WhenDurableFlushFails_RestoresFileAndRollsBack()
    {
        await using var stream = new FaultInjectingStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();

        await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

        // Replay writes all pages before its single durable flush.
        const int durableFlushCall = 1;
        stream.ThrowOnFlushCall(durableFlushCall);

        await Assert.ThrowsAsync<IOException>(async () =>
            await tx.CommitAsync(TestContext.Current.CancellationToken));

        byte[] after = stream.ToArray();

        Assert.True(tx.IsRolledBack);
        Assert.False(tx.IsCommitted);
        Assert.Equal(before, after);
        Assert.Equal(FormatVersionByte(before), FormatVersionByte(after));
    }

    /// <summary>
    /// Cancelling the commit's token after the first page has been written
    /// must not stop the replay: the file would hold only part of the
    /// transaction. The commit used to check the token before every page.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="encryption">The page encryption applied before the transaction.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.None)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.None)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4)]
    public async Task Commit_WhenCancelledAfterReplayStarts_CompletesReplay(DatabaseFormat format, AccessEncryptionFormat encryption)
    {
        string? password = encryption == AccessEncryptionFormat.None ? null : "secret";
        var options = new AccessWriterOptions(password) { UseLockFile = false, UseByteRangeLocks = false };
        await using var stream = new FaultInjectingStream();
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            options,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await creator.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);
            Assert.True(tx.JournaledPageCount > 1);

            using var cancellation = new CancellationTokenSource();
            stream.CancelAfterPageWrite(1, cancellation);

            await tx.CommitAsync(cancellation.Token);

            Assert.True(cancellation.IsCancellationRequested);
            Assert.True(tx.IsCommitted);
            Assert.Equal(tx.JournaledPageCount, stream.PageWritesAfterArm);
        }

        stream.Position = 0;
        var readerOptions = new AccessReaderOptions(password) { UseLockFile = false };
        await using AccessReader reader = await AccessReader.OpenAsync(stream, readerOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(100, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A commit cancelled before it writes anything ends as a rollback: the
    /// file is untouched, <see cref="JetTransaction.IsRolledBack"/> is set, and
    /// the writer is usable. A token that was already cancelled used to throw
    /// before the transaction was detached, leaving it active.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task Commit_WithCancelledToken_RollsBackWithoutWritingPages()
    {
        await using var stream = new FaultInjectingStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
            byte[] before = stream.ToArray();

            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await BufferMultiPageInsertAsync(writer, TestContext.Current.CancellationToken);

            using var cancellation = new CancellationTokenSource();
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await tx.CommitAsync(cancellation.Token));

            Assert.True(tx.IsRolledBack);
            Assert.False(tx.IsCommitted);
            Assert.Equal(0, CountChangedPages(before, stream.ToArray()));

            await writer.InsertRowAsync("Items", [500, "After"], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken));
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

        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
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
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync("Items", TestContext.Current.CancellationToken);

        // Only the seed row should remain — none of the batch rows persisted.
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task UseTransactionalWrites_Disabled_CompletesDefaultStatement()
    {
        // Default statements are atomic without requiring a durable flush.
        await using var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync("Items", ItemsSchema(), TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Items", [1, "A"], TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTables_UntilCatalogNeedsNewPage_InTransaction_ResolvesEveryTable(DatabaseFormat format)
    {
        // Enough tables that MSysObjects spills onto data pages the
        // transaction appends. Catalog lookups read those pages from the
        // journal, past the physical end of the file.
        const int tableCount = 80;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            NonLockingWriterOptions(),
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await using JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            for (int i = 0; i < tableCount; i++)
            {
                string tableName = "Items" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                await writer.CreateTableAsync(tableName, ItemsSchema(), TestContext.Current.CancellationToken);
                await writer.InsertRowAsync(tableName, [i, "Row" + i], TestContext.Current.CancellationToken);
            }

            await tx.CommitAsync(TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, ReaderOptions, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        IReadOnlyList<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(tableCount, tables.Count);
        for (int i = 0; i < tableCount; i++)
        {
            string tableName = "Items" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            Assert.Equal(1, await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken));
        }
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

    private static byte FormatVersionByte(byte[] databaseBytes) => databaseBytes[0x14];

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
        private int? throwBeforePageWrite;
        private int? throwOnFlushCall;
        private int? cancelAfterPageWrite;
        private CancellationTokenSource? cancellation;
        private bool armed;

        public override bool CanRead => this.inner.CanRead;

        public override bool CanSeek => this.inner.CanSeek;

        public override bool CanWrite => this.inner.CanWrite;

        public override long Length => this.inner.Length;

        public override long Position
        {
            get => this.inner.Position;
            set => this.inner.Position = value;
        }

        public int PageWritesAfterArm { get; private set; }

        public int FlushesAfterArm { get; private set; }

        public void ThrowBeforePageWrite(int pageWriteNumber)
        {
            this.armed = true;
            this.throwBeforePageWrite = pageWriteNumber;
        }

        public void ThrowOnFlushCall(int flushCallNumber)
        {
            this.armed = true;
            this.throwOnFlushCall = flushCallNumber;
        }

        public void CancelAfterPageWrite(int pageWriteNumber, CancellationTokenSource cancellation)
        {
            this.armed = true;
            this.cancelAfterPageWrite = pageWriteNumber;
            this.cancellation = cancellation;
        }

        public byte[] ToArray() => this.inner.ToArray();

        public override void Flush() => this.inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (this.armed)
            {
                int nextFlush = this.FlushesAfterArm + 1;
                if (this.throwOnFlushCall == nextFlush)
                {
                    this.throwOnFlushCall = null;
                    throw new IOException("Injected flush failure.");
                }

                this.FlushesAfterArm++;
            }

            return this.inner.FlushAsync(cancellationToken);
        }

        public override int Read(byte[] buffer, int offset, int count) => this.inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
            this.inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => this.inner.Seek(offset, origin);

        public override void SetLength(long value) => this.inner.SetLength(value);

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.MaybeThrowBeforePageWrite(count, CancellationToken.None);
            this.inner.Write(buffer, offset, count);
            this.RecordPageWrite(count);
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            this.MaybeThrowBeforePageWrite(buffer.Length, cancellationToken);
            await this.inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            this.RecordPageWrite(buffer.Length);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.inner.Dispose();
            }

            base.Dispose(disposing);
        }

        private void MaybeThrowBeforePageWrite(int count, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!this.armed || count != Constants.PageSizes.Jet4)
            {
                return;
            }

            if (this.throwBeforePageWrite == this.PageWritesAfterArm + 1)
            {
                this.throwBeforePageWrite = null;
                throw new IOException("Injected page-write failure.");
            }
        }

        private void RecordPageWrite(int count)
        {
            if (!this.armed || count != Constants.PageSizes.Jet4)
            {
                return;
            }

            this.PageWritesAfterArm++;
            if (this.cancelAfterPageWrite == this.PageWritesAfterArm)
            {
                this.cancellation?.Cancel();
            }
        }
    }
}
