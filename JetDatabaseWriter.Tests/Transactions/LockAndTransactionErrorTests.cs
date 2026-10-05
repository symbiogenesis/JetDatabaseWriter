namespace JetDatabaseWriter.Tests.Transactions;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;using Xunit;

/// <summary>Checks deliberate locking, file and transaction refusals expose stable codes.</summary>
public sealed class LockAndTransactionErrorTests
{
    /// <summary>A second begin and operations on an ended transaction keep their BCL base and code.</summary>
    [Fact]
    public async Task TransactionState_ReportsCodes()
    {
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        JetOperationException active = await Assert.ThrowsAsync<JetOperationException>(() => writer.BeginTransactionAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.IsType<InvalidOperationException>(active, exactMatch: false);
        Assert.Equal(JetErrorCode.TransactionAlreadyActive, active.ErrorCode);
        await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        JetOperationException ended = await Assert.ThrowsAsync<JetOperationException>(() => transaction.CommitAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(JetErrorCode.TransactionEnded, ended.ErrorCode);
    }

    /// <summary>A transaction from a different lifecycle is refused without ending the active one.</summary>
    [Fact]
    public async Task ForeignTransaction_ReportsNotActiveCode()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new JetDatabaseWriter.Models.ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        await using JetTransaction active = await harness.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await using var foreign = new JetTransaction(harness.Services.Transactions, new PagerTransaction(stream.Length, harness.Database.Format.PageSize, 16));
        JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(() => foreign.CommitAsync(TestContext.Current.CancellationToken).AsTask());
        Assert.Equal(JetErrorCode.TransactionNotActive, error.ErrorCode);
        Assert.False(active.IsCommitted);
        Assert.False(active.IsRolledBack);
        await active.RollbackAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>An existing destination reports the typed file refusal.</summary>
    [Fact]
    public async Task ExistingDatabase_ReportsFileCode()
    {
        string path = Path.GetTempFileName();
        try
        {
            JetIOException error = await Assert.ThrowsAsync<JetIOException>(() => AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: TestContext.Current.CancellationToken).AsTask());
            Assert.IsType<IOException>(error, exactMatch: false);
            Assert.Equal(JetErrorCode.DatabaseFileExists, error.ErrorCode);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>A pre-existing lock file reports DatabaseInUse.</summary>
    [Fact]
    public void ExistingLockFile_ReportsInUseCode()
    {
        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".accdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        File.WriteAllText(lockPath, "Held by another process.");
        try
        {
            JetLockException error = Assert.Throws<JetLockException>(() => LockFileSlotWriter.Open(path, nameof(AccessWriter), respectExisting: true, "Machine", "User"));
            Assert.Equal(JetErrorCode.DatabaseInUse, error.ErrorCode);
        }
        finally
        {
            File.Delete(lockPath);
        }
    }

    /// <summary>All live lock-file slots give the stable full-file refusal.</summary>
    [Fact]
    public void FullLockFile_ReportsFullCode()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".accdb");
        List<LockFileSlotWriter> slots = [];
        try
        {
            for (int index = 0; index < LockFileSlotWriter.MaxSlots; index++)
            {
#pragma warning disable CA2000 // Every acquired slot is owned by slots and disposed in finally.
                var slot = LockFileSlotWriter.Open(path, nameof(AccessWriter), respectExisting: true, "Machine", "User");
#pragma warning restore CA2000
                Assert.NotNull(slot);
                slots.Add(slot);
            }

            JetLockException error = Assert.Throws<JetLockException>(() => LockFileSlotWriter.Open(path, nameof(AccessWriter), respectExisting: true, "Machine", "User"));
            Assert.Equal(JetErrorCode.LockFileFull, error.ErrorCode);
        }
        finally
        {
            foreach (LockFileSlotWriter slot in slots)
            {
                slot.Dispose();
            }

            File.Delete(LockFileSlotWriter.GetLockFilePath(path));
        }
    }
}
