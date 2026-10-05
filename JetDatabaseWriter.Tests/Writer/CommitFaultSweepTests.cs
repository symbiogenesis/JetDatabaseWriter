namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Commit replay restores raw file bytes, including encrypted pages.</summary>
public class CommitFaultSweepTests
{
    /// <summary>Every physical write and flush either commits or restores the file.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="encryption">The page encryption.</param>
    /// <param name="automatic">Whether to use an implicit transaction.</param>
    /// <returns>The asynchronous completion.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, AccessEncryptionFormat.None, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, AccessEncryptionFormat.None, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.None, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.None, true)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.None, false)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.None, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4, true)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAesCfbWrapped, false)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAesCfbWrapped, true)]
    public async Task CommitFaults_RestoreOriginalImage(DatabaseFormat format, AccessEncryptionFormat encryption, bool automatic)
    {
        byte[] baseline;
        await using (var initial = new MemoryStream())
        {
            await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(initial, format, Options(false, null), leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
            {
                await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Text", typeof(string), 200)], TestContext.Current.CancellationToken);
            }

            if (encryption != AccessEncryptionFormat.None)
            {
                initial.Position = 0;
                await AccessWriter.EncryptAsync(initial, "secret".AsMemory(), encryption, TestContext.Current.CancellationToken);
            }

            baseline = initial.ToArray();
        }

        string? password = encryption == AccessEncryptionFormat.None ? null : "secret";
        for (int fault = 0; ; fault++)
        {
            await using var stream = new WriteFaultStream();
            stream.Write(baseline);
            stream.Position = 0;
            await using AccessWriter writer = await AccessWriter.OpenAsync(stream, Options(automatic, password), leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            JetTransaction? transaction = automatic ? null : await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            try
            {
                if (transaction is not null)
                {
                    await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
                }

                if (fault == 0)
                {
                    stream.FailOnFlush(1);
                }
                else
                {
                    stream.FailOnWrite(fault);
                }

                try
                {
                    if (transaction is null)
                    {
                        await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
                    }
                    else
                    {
                        await transaction.CommitAsync(TestContext.Current.CancellationToken);
                    }
                }
                catch (IOException)
                {
                    Assert.True(stream.Faulted);
                    Assert.Equal(baseline, stream.ToArray());
                    if (transaction is not null)
                    {
                        Assert.True(transaction.IsRolledBack);
                        Assert.False(transaction.IsCommitted);
                    }

                    await writer.InsertRowAsync("Items", [999, "After"], TestContext.Current.CancellationToken);
                    continue;
                }

                Assert.False(stream.Faulted);
                Assert.NotEqual(baseline.Length, stream.Length);
                break;
            }
            finally
            {
                if (transaction is not null)
                {
                    await transaction.DisposeAsync();
                }
            }
        }
    }

    /// <summary>Capture failure and a torn page write both leave a usable writer.</summary>
    /// <param name="duringWrite">Whether to tear a page write or fail capture.</param>
    /// <returns>The asynchronous completion.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Commit_CaptureOrPartialWriteFailure_RestoresOriginalImage(bool duringWrite)
    {
        await using var stream = new WriteFaultStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, Options(false, null), leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Text", typeof(string), 200)], TestContext.Current.CancellationToken);
        byte[] before = stream.ToArray();
        await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
        await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
        if (duringWrite)
        {
            stream.FailDuringWrite(2);
        }
        else
        {
            stream.FailOnRead(1);
        }

        await Assert.ThrowsAsync<IOException>(async () => await transaction.CommitAsync(TestContext.Current.CancellationToken));
        Assert.True(transaction.IsRolledBack);
        Assert.Equal(before, stream.ToArray());
        await writer.InsertRowAsync("Items", [999, "After"], TestContext.Current.CancellationToken);
    }
    /// <summary>Successful commit undo rewinds cached AutoNumber state.</summary>
    /// <param name="automatic">Whether to use a private transaction.</param>
    /// <returns>The asynchronous completion.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommitUndo_RestoresAutoNumber(bool automatic)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, Options(automatic, null), leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [DBNull.Value], TestContext.Current.CancellationToken);
            JetTransaction? transaction = automatic ? null : await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            try
            {
                if (transaction is not null)
                {
                    await writer.InsertRowsAsync("Items", [[DBNull.Value], [DBNull.Value]], TestContext.Current.CancellationToken);
                }

                stream.FailOnFlush(1);
                await Assert.ThrowsAsync<IOException>(async () =>
                {
                    if (transaction is not null)
                    {
                        await transaction.CommitAsync(TestContext.Current.CancellationToken);
                    }
                    else
                    {
                        await writer.InsertRowsAsync("Items", [[DBNull.Value], [DBNull.Value]], TestContext.Current.CancellationToken);
                    }
                });
                await writer.InsertRowAsync("Items", [DBNull.Value], TestContext.Current.CancellationToken);
            }
            finally
            {
                if (transaction is not null)
                {
                    await transaction.DisposeAsync();
                }
            }
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        System.Data.DataTable table = await reader.ReadDataTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, table.Rows.Count);
        Assert.Equal(1, table.Rows[0]["Id"]);
        Assert.Equal(2, table.Rows[1]["Id"]);
    }
    /// <summary>An unsuccessful undo prohibits mutations and disposal writes.</summary>
    /// <returns>The asynchronous completion.</returns>
    [Fact]
    public async Task UndoFailure_FaultsWriterAndDisposeDoesNotWrite()
    {
        await using var stream = new WriteFaultStream();
        AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, Options(false, null), leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Text", typeof(string), 200)], TestContext.Current.CancellationToken);
            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync("Items", Rows(), TestContext.Current.CancellationToken);
            stream.FailWritesFrom(2);
            await Assert.ThrowsAsync<AggregateException>(async () => await transaction.CommitAsync(TestContext.Current.CancellationToken));
            Assert.False(transaction.IsRolledBack);
            Assert.False(transaction.IsCommitted);
            JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync("Items", [999, "After"], TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.WriterFaulted, error.ErrorCode);
            error = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.BeginTransactionAsync(TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.WriterFaulted, error.ErrorCode);
            error = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.WriterFaulted, error.ErrorCode);
            error = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.WriterFaulted, error.ErrorCode);
            int writes = stream.WriteCount;
            await writer.DisposeAsync();
            Assert.Equal(writes, stream.WriteCount);
        }
        finally
        {
            await writer.DisposeAsync();
        }
    }

    private static AccessWriterOptions Options(bool automatic, string? password) => new(password)
    {
        UseLockFile = false,
        UseByteRangeLocks = false,
        UseTransactionalWrites = automatic,
    };

    private static List<object[]> Rows()
    {
        var rows = new List<object[]>(100);
        for (int i = 0; i < 100; i++)
        {
            rows.Add([i, new string('x', 180)]);
        }

        return rows;
    }
}