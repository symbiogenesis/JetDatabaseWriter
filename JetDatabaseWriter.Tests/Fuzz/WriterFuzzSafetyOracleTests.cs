namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;
using Xunit.Sdk;

/// <summary>Ordinary regression checks for the writer fuzzer's refusal oracle.</summary>
public sealed class WriterFuzzSafetyOracleTests
{
    /// <summary>A refusal must fail the oracle if it changed even one database byte.</summary>
    [Fact]
    public async Task RejectedWrite_AfterChangingBytes_FailsOracle()
    {
        await using var stream = new MemoryStream([1, 2, 3]);
        _ = await Assert.ThrowsAnyAsync<XunitException>(async () =>
            await WriterFuzzSafetyOracle.ExecuteAsync(stream, () =>
            {
                stream.WriteByte(4);
                throw new JetConstraintException(JetErrorCode.UniqueViolation, "Deliberate refusal after mutation.");
            }));
    }

    /// <summary>Programming failures must escape rather than become accepted fuzz outcomes.</summary>
    [Fact]
    public async Task UnexpectedOperationFailure_Propagates()
    {
        await using var stream = new MemoryStream([1, 2, 3]);
        var failure = new InvalidOperationException("Unexpected implementation failure.");
        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await WriterFuzzSafetyOracle.ExecuteAsync(stream, () => throw failure));
        Assert.Same(failure, actual);
    }

    /// <summary>Valid output corruption is always a failure, including password-protected output.</summary>
    [Fact]
    public async Task RoundTrip_InvalidOutput_Propagates()
    {
        await using var stream = new MemoryStream([1, 2, 3]);
        _ = await Assert.ThrowsAnyAsync<Exception>(async () =>
            await WriterFuzzSafetyOracle.VerifyRoundTripAsync(stream, "secret".AsMemory(), TestContext.Current.CancellationToken));
    }

    /// <summary>The explicit transaction budget does not reject creation or private statement transactions.</summary>
    /// <param name="format">The database generation.</param>
    /// <param name="encrypted">Whether the image is password protected.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CreateWithMinimumTransactionBudget_Succeeds(DatabaseFormat format, bool encrypted)
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions(encrypted ? "secret" : null) { UseLockFile = false, UseTransactionalWrites = true, MaxTransactionPageBudget = 1 };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        }

        await WriterFuzzSafetyOracle.VerifyRoundTripAsync(stream, options.Password, TestContext.Current.CancellationToken);
    }

    /// <summary>Refused row and schema mutations preserve ciphertext, and rollback restores the original image.</summary>
    /// <param name="format">The database generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RejectedMutationsAndRollback_PreserveDatabase(DatabaseFormat format)
    {
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions("secret") { UseLockFile = false, UseTransactionalWrites = true };
        byte[] before;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            ColumnDefinition[] columns = [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Value", typeof(int)) { IsNullable = false }];
            await writer.CreateTableAsync("Items", columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1, 10], TestContext.Current.CancellationToken);
            before = stream.ToArray();

            Assert.NotNull(await WriterFuzzSafetyOracle.ExecuteAsync(stream, async () =>
                await writer.CreateTableAsync("Items", columns, TestContext.Current.CancellationToken)));
            object?[][] rows = [[2, 20], [1, 30]];
            Assert.NotNull(await WriterFuzzSafetyOracle.ExecuteAsync(stream, async () =>
                _ = await writer.InsertRowsAsync("Items", rows, TestContext.Current.CancellationToken)));
            Assert.NotNull(await WriterFuzzSafetyOracle.ExecuteAsync(stream, async () =>
                _ = await writer.UpdateRowsAsync("Items", "Id", 1, new Dictionary<string, object?> { ["Value"] = null }, TestContext.Current.CancellationToken)));
            Assert.NotNull(await WriterFuzzSafetyOracle.ExecuteAsync(stream, async () =>
                _ = await writer.DeleteRowsAsync("Missing", "Id", 1, TestContext.Current.CancellationToken)));

            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [2, 20, 30], TestContext.Current.CancellationToken);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            WriterFuzzSafetyOracle.AssertUnchanged(stream, before);
        }

        WriterFuzzSafetyOracle.AssertUnchanged(stream, before);
        await WriterFuzzSafetyOracle.VerifyRoundTripAsync(stream, options.Password, TestContext.Current.CancellationToken);
    }
}
