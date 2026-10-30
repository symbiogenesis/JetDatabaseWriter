namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks statement budgets, encrypted undo and failed cancellation recovery.</summary>
/// <param name="db">Caches Access-authored fixtures with relationship catalogs.</param>
/// <param name="generated">Caches generated statement fault seeds.</param>
public sealed class StatementAtomicityTests(DatabaseCache db, GeneratedDatabaseCache generated) : StatementAtomicityTestBase(db, generated)
{
    /// <summary>A default statement does not inherit the explicit transaction page budget.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DefaultStatement_IgnoresExplicitTransactionBudget(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false, MaxTransactionPageBudget = 1 };
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, cancellationToken: Ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], Ct);
        Assert.Equal(300, await writer.InsertRowsAsync("T", Enumerable.Range(1, 300).Select(id => new object?[] { id }), Ct));
    }

    /// <summary>Default commit undo restores native Jet ciphertext, including partially overwritten pages.</summary>
    [Fact]
    public async Task EncryptedDefaultStatement_RestoresCiphertext()
    {
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), Ct);
        await using var encrypted = new MemoryStream();
        await encrypted.WriteAsync(fixture, Ct);
        encrypted.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(encrypted, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.DropTableAsync("T", Ct);
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Note", typeof(string))], Ct);
            string note = new('a', 35000);
            _ = await writer.InsertRowsAsync("T", Enumerable.Range(1, 8).Select(id => new object?[] { id, note }), Ct);
        }

        byte[] original = encrypted.ToArray();
        await using WriteFaultStream baseline = Copy(original);
        await using (AccessWriter writer = await OpenAsync(baseline, "Native123"))
        {
            await MutateAsync(writer, "Update");
        }

        byte[] completed = baseline.ToArray();
        await AssertCompletedAsync(baseline, "Update", "Native123");
        await using var faultStream = new WriteFaultStream();
        await AssertFaultAsync(faultStream, original, completed, "Update", stream => stream.FailDuringWrite(1), "Native123");
        await AssertFaultAsync(faultStream, original, completed, "Update", stream => stream.FailOnFlush(1), "Native123");
    }

    /// <summary>Cancellation before final replay cannot leave a detached transaction active when early undo fails.</summary>
    [Fact]
    public async Task CancellationBeforeFinalReplay_UndoFailureFaultsWriter()
    {
        byte[] original = await this.CreateSourceAsync(DatabaseFormat.AceAccdb);
        await using WriteFaultStream baseline = Copy(original);
        int reads;
        int earlyWrites;
        await using (AccessWriter writer = await OpenAsync(baseline))
        {
            int initialReads = baseline.ReadCount;
            int initialWrites = baseline.WriteCount;
            await MutateAsync(writer, "Update");
            reads = baseline.ReadCount - initialReads;
            earlyWrites = baseline.WriteCount - initialWrites;
        }

        Assert.True(earlyWrites > 64);
        await using WriteFaultStream stream = Copy(original);
        AccessWriter target = await OpenAsync(stream);
        try
        {
            using var source = new CancellationTokenSource();
            stream.FailWritesFrom(int.MaxValue);
            stream.CancelAfterReads(reads, source);
            AggregateException failure = await Assert.ThrowsAsync<AggregateException>(async () =>
                await target.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Note"] = UpdatedNote }, source.Token));
            Assert.True(source.IsCancellationRequested);
            Assert.Contains(failure.Flatten().InnerExceptions, exception => exception is OperationCanceledException);
            Assert.Contains(failure.Flatten().InnerExceptions, exception => exception is IOException);
            JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(async () => await target.BeginTransactionAsync(Ct));
            Assert.Equal(JetErrorCode.WriterFaulted, error.ErrorCode);
            int writes = stream.WriteCount;
            await target.DisposeAsync();
            Assert.Equal(writes, stream.WriteCount);
        }
        finally
        {
            await target.DisposeAsync();
        }
    }
}
