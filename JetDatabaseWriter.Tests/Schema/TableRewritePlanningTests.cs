namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Complex reference reservations made during preparation belong to the caller's transaction.</summary>
public sealed class TableRewritePlanningTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedComplexPreparation_RollbackRestoresReferenceCache(bool cancel)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            ct))
        {
            await writer.CreateTableAsync(
                "Original",
                [new("Id", typeof(int)), new ColumnDefinition("Old", typeof(byte[])) { IsAttachment = true }],
                ct);
            await writer.InsertRowsAsync("Original", [[1, DBNull.Value], [2, DBNull.Value]], ct);
        }

        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = true },
            cancellationToken: ct);

        // Warm the reference cache, so rollback must rewind an existing holder.
        await harness.InsertRowAsync("Original", [3, DBNull.Value], ct);
        byte[] original = stream.ToArray();
        int[]? reserved = null;
        using var cancellation = new CancellationTokenSource();

        async ValueTask PrepareAndFailAsync(CancellationToken token)
        {
            TableRewritePlan plan = await PrepareReplacementAsync(harness, token);
            reserved = References(plan);
            if (cancel)
            {
                await cancellation.CancelAsync();
                cancellation.Token.ThrowIfCancellationRequested();
            }

            throw new IOException("Injected failure after reserving complex references.");
        }

        Task MutateAsync() => harness.Services.Transactions.RunAutoCommitAsync(PrepareAndFailAsync, ct).AsTask();
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(MutateAsync);
        }
        else
        {
            await Assert.ThrowsAsync<IOException>(MutateAsync);
        }

        Assert.Equal(original, stream.ToArray());
        Assert.Null(harness.Services.Transactions.ActiveTransaction);
        await using JetTransaction retryTransaction = await harness.BeginTransactionAsync(ct);
        TableRewritePlan retry = await harness.Services.Transactions.RunAutoCommitAsync(
            token => PrepareReplacementAsync(harness, token),
            ct);
        Assert.NotNull(reserved);
        Assert.Equal([4, 5, 6], reserved);
        Assert.Equal(reserved, References(retry));
        await retryTransaction.RollbackAsync(ct);
        Assert.Equal(original, stream.ToArray());
        Assert.Equal(["Original"], (await harness.Services.Catalog.GetUserTablesAsync(ct)).Select(static entry => entry.Name));
    }

    private static ValueTask<TableRewritePlan> PrepareReplacementAsync(WriterHarness harness, CancellationToken cancellationToken)
        => harness.Services.RewritePlanner.PrepareAsync(
            "Original",
            static (columns, _) =>
            [
                columns[0],
                new("New", typeof(byte[])) { IsAttachment = true },
            ],
            static (values, _) => [values[0], DBNull.Value],
            static name => string.Equals(name, "Old", StringComparison.Ordinal) ? null : name,
            cancellationToken);

    private static int[] References(TableRewritePlan plan)
        => [.. plan.Rows.Select(static row => Assert.IsType<ComplexIdRef>(row[1]).Id)];
}
