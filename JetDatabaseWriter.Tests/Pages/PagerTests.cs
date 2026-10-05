namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the page-file seam: the writer's <see cref="Pager"/> journals a
/// transaction's writes and appends, serves its pending pages as plaintext
/// (on an encrypted file too), counts its appended pages in
/// <see cref="Pager.PageCount"/>, keeps positional reads off while the
/// journal is attached, and returns to the file's bytes on rollback; and a
/// page cache over the writer's file must not cache. That the reader's graph
/// holds no pager is checked in <see cref="Architecture.ServiceGraphTests"/>.
/// Each case runs on writer-created Jet3, Jet4 and ACCDB databases and on an
/// ACCDB encrypted as <see cref="AccessEncryptionFormat.AccdbAesCfbWrapped"/>.
/// </summary>
public sealed class PagerTests
{
    private const string Password = "secret";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReadPage_WhileJournalAttached_ReturnsJournaledPlaintext(DatabaseFormat format, bool encrypted)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, encrypted);
        byte[] fileBefore = stream.ToArray();
        await using WriterHarness harness = await OpenAsync(stream, encrypted);
        DatabaseFile db = harness.Database;
        Pager pager = harness.Pager;
        int pageSize = db.Format.PageSize;
        const long pageNumber = 2;

        byte[] original = await db.Pages.ReadPageCopyAsync(pageNumber, Ct);
        byte[] changed = (byte[])original.Clone();
        changed[pageSize - 1] ^= 0x5A;

        JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(Ct);
        Assert.True(pager.IsJournalActive);
        await pager.WritePageAsync(pageNumber, changed, Ct);

        Assert.Equal(changed, await db.Pages.ReadPageCopyAsync(pageNumber, Ct));
        Assert.Equal(fileBefore, stream.ToArray());

        await tx.RollbackAsync(Ct);

        Assert.False(pager.IsJournalActive);
        Assert.Equal(original, await db.Pages.ReadPageCopyAsync(pageNumber, Ct));
        Assert.Equal(fileBefore, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task PageCount_WhileJournalAttached_IncludesAppendedPages(DatabaseFormat format, bool encrypted)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, encrypted);
        await using WriterHarness harness = await OpenAsync(stream, encrypted);
        DatabaseFile db = harness.Database;
        Pager pager = harness.Pager;
        long physical = pager.PhysicalPageCount;
        Assert.Equal(physical, pager.PageCount);

        JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(Ct);
        for (int i = 0; i < 3; i++)
        {
            long appended = await pager.AppendPageAsync(FilledPage(db.Format.PageSize, (byte)(0x41 + i)), Ct);
            Assert.Equal(physical + i, appended);
        }

        Assert.Equal(physical + 3, pager.PageCount);
        Assert.Equal(physical + 3, db.Pages.PageCount);
        Assert.Equal(physical, pager.PhysicalPageCount);
        Assert.Equal(FilledPage(db.Format.PageSize, 0x43), await db.Pages.ReadPageCopyAsync(physical + 2, Ct));

        await tx.RollbackAsync(Ct);

        Assert.Equal(physical, pager.PageCount);
        Assert.Equal(physical * db.Format.PageSize, stream.Length);
    }

    /// <summary>
    /// A writer's file opened by path reads its pages positionally once
    /// <see cref="PageFile.EnableRandomAccessPageReadsIfSupported"/> has run
    /// (on the net10.0 build), but not while a transaction journal is
    /// attached: a pending page and a page appended past the physical end of
    /// file come from the journal, where a positional read would return the
    /// file's old bytes or run past its end. After rollback, and after a
    /// commit, the positional read returns the file's bytes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="encrypted">Whether the file is encrypted as <see cref="AccessEncryptionFormat.AccdbAesCfbWrapped"/>.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task ReadPage_PositionalReadsEnabled_WhileJournalAttached_ReadsTheJournal(DatabaseFormat format, bool encrypted)
    {
        string extension = format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
        string path = Path.Combine(Path.GetTempPath(), $"PagerPositionalReads_{Guid.NewGuid():N}{extension}");
        try
        {
            await using (MemoryStream created = await CreateDatabaseAsync(format, encrypted))
            {
                await File.WriteAllBytesAsync(path, created.ToArray(), Ct);
            }

            await using WriterHarness harness = await WriterHarness.OpenAsync(path, WriterOptions(encrypted), Ct);
            DatabaseFile db = harness.Database;
            Pager pager = harness.Pager;
            db.Pages.EnableRandomAccessPageReadsIfSupported();
            Assert.Equal(!LibraryTarget.IsNetStandard, pager.UsesRandomAccessPageReads);

            const long pageNumber = 2;
            int pageSize = db.Format.PageSize;
            long physical = pager.PhysicalPageCount;
            byte[] original = await db.Pages.ReadPageCopyAsync(pageNumber, Ct);
            byte[] changed = (byte[])original.Clone();
            changed[pageSize - 1] ^= 0x5A;
            byte[] appendedPage = FilledPage(pageSize, 0x6B);

            JetTransaction rolledBack = await harness.Services.Transactions.BeginTransactionAsync(Ct);
            await pager.WritePageAsync(pageNumber, changed, Ct);
            long appended = await pager.AppendPageAsync(appendedPage, Ct);
            Assert.Equal(physical, appended);

            Assert.Equal(changed, await db.Pages.ReadPageCopyAsync(pageNumber, Ct));
            Assert.Equal(appendedPage, await db.Pages.ReadPageCopyAsync(appended, Ct));

            await rolledBack.RollbackAsync(Ct);

            Assert.Equal(physical, pager.PageCount);
            Assert.Equal(original, await db.Pages.ReadPageCopyAsync(pageNumber, Ct));

            JetTransaction committed = await harness.Services.Transactions.BeginTransactionAsync(Ct);
            await pager.WritePageAsync(pageNumber, changed, Ct);
            await committed.CommitAsync(Ct);

            Assert.False(pager.IsJournalActive);
            Assert.Equal(changed, await db.Pages.ReadPageCopyAsync(pageNumber, Ct));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Commit_WritesJournaledPages_EncryptedOnlyOnDisk(DatabaseFormat format, bool encrypted)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, encrypted);
        byte[] page;
        long appended;
        int pageSize;
        await using (WriterHarness harness = await OpenAsync(stream, encrypted))
        {
            DatabaseFile db = harness.Database;
            pageSize = db.Format.PageSize;
            page = FilledPage(pageSize, 0x7E);

            JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(Ct);
            appended = await harness.Pager.AppendPageAsync(page, Ct);
            await tx.CommitAsync(Ct);

            Assert.True(tx.IsCommitted);
            Assert.False(harness.Pager.IsJournalActive);
            Assert.Equal(page, await db.Pages.ReadPageCopyAsync(appended, Ct));
        }

        byte[] onDisk = stream.ToArray().AsSpan(checked((int)(appended * pageSize)), pageSize).ToArray();
        if (encrypted)
        {
            Assert.NotEqual(page, onDisk);
        }
        else
        {
            Assert.Equal(page, onDisk);
        }

        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, ReaderOptions(encrypted), cancellationToken: Ct);
        Assert.Equal(page, await reader.ReadPageCopyAsync(appended, Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BeginTransaction_WhileActive_KeepsExistingMessage(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateDatabaseAsync(format, encrypted: false);
        await using WriterHarness harness = await OpenAsync(stream, encrypted: false);

        JetTransaction tx = await harness.Services.Transactions.BeginTransactionAsync(Ct);
        InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await harness.Services.Transactions.BeginTransactionAsync(Ct));

        Assert.Equal(
            "A transaction is already active on this writer. Only one concurrent transaction per AccessWriter is supported.",
            ex.Message);
        Assert.True(harness.Pager.IsJournalActive);

        await tx.RollbackAsync(Ct);
        Assert.False(harness.Pager.IsJournalActive);
    }

    [Fact]
    public async Task JournalGate_AttachTwice_Throws_AndDisposeReleasesTheGate()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, encrypted: false);
        await using WriterHarness harness = await OpenAsync(stream, encrypted: false);
        Pager pager = harness.Pager;

        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(Ct))
        {
            Assert.Null(gate.Current);
            Assert.Equal(stream.Length, gate.PhysicalLengthBytes);
            var journal = new PageJournal(gate.PhysicalLengthBytes, pager.PageSize, maxPages: 4);
            gate.Attach(journal);
            Assert.Same(journal, gate.Current);
            _ = Assert.Throws<InvalidOperationException>(() => gate.Attach(new PageJournal(gate.PhysicalLengthBytes, pager.PageSize, maxPages: 4)));
            gate.Detach();
            Assert.Null(gate.Current);
        }

        // The gate is free again: a page read takes it.
        byte[] page = await pager.ReadPageCopyAsync(1, Ct);
        Assert.Equal(pager.PageSize, page.Length);
    }

    [Fact]
    public async Task ReaderPageCache_OverPager_RejectsPositiveCapacity()
    {
        await using MemoryStream stream = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, encrypted: false);
        await using WriterHarness writer = await OpenAsync(stream, encrypted: false);

        ArgumentException ex = Assert.Throws<ArgumentException>(() => new ReaderPageCache(writer.Database.Format, writer.Database.Pages, capacity: 1));
        Assert.Equal("capacity", ex.ParamName);

        using var uncached = new ReaderPageCache(writer.Database.Format, writer.Database.Pages, capacity: 0);
        Assert.Equal(0, uncached.Hits);

        await using ReaderHarness reader = await ReaderHarness.OpenAsync(stream, cancellationToken: Ct);
        using var cached = new ReaderPageCache(reader.Database.Format, reader.Database.Pages, capacity: 8);
        Assert.Equal(0, cached.Misses);
    }

    private static byte[] FilledPage(int pageSize, byte value)
    {
        byte[] page = new byte[pageSize];
        page.AsSpan().Fill(value);
        return page;
    }

    private static AccessReaderOptions ReaderOptions(bool encrypted)
        => new(encrypted ? Password : null) { UseLockFile = false };

    private static AccessWriterOptions WriterOptions(bool encrypted)
        => new(encrypted ? Password : null) { UseLockFile = false, UseByteRangeLocks = false };

    private static ValueTask<WriterHarness> OpenAsync(MemoryStream stream, bool encrypted)
        => WriterHarness.OpenAsync(stream, WriterOptions(encrypted), cancellationToken: Ct);

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, bool encrypted)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], Ct);
            await writer.InsertRowAsync("Items", [1], Ct);
        }

        if (encrypted)
        {
            stream.Position = 0;
            await AccessWriter.EncryptAsync(stream, Password.AsMemory(), AccessEncryptionFormat.AccdbAesCfbWrapped, Ct);
        }

        stream.Position = 0;
        return stream;
    }
}
