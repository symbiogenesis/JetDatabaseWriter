namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Synthetic Agile variants exercise page maintenance; they do not establish native producer provenance.</summary>
public sealed class AgileMaintenanceFaultTests
{
    /// <summary>Physical truncation failures preserve ciphertext and allow a successful retry.</summary>
    /// <param name="hash">The page and password digest.</param>
    /// <param name="bits">The AES key size.</param>
    /// <param name="chaining">The chaining mode.</param>
    /// <param name="fault">The physical failure to inject.</param>
    [Theory]
    [InlineData("SHA256", 192, "ChainingModeCBC", "truncate")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "truncated")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "flush")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "truncated-flush")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "write")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "partial")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "truncate")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "truncated")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "flush")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "truncated-flush")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "write")]
    [InlineData("SHA384", 192, "ChainingModeCFB", "partial")]
    [InlineData("SHA256", 192, "ChainingModeCBC", "spilled-truncated")]
    public async Task ShrinkFailureRestoresCiphertext(string hash, int bits, string chaining, string fault)
    {
        byte[] variant = await NativeAgileAlgorithmTests.CreateVariantAsync(hash, hash, bits, bits, chaining);
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(variant, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("vector") { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages }, leaveOpen: true, TestContext.Current.CancellationToken);
        await PrepareFreeTailAsync(writer, fault == "spilled-truncated" ? 400_000 : 32_000);
        byte[] baseline = stream.ToArray();
        switch (fault)
        {
            case "truncate":
                stream.FailOnTruncate(afterTruncate: false);
                break;
            case "spilled-truncated":
            case "truncated":
                stream.FailOnTruncate(afterTruncate: true);
                break;
            case "flush":
                stream.FailOnFlush(1);
                break;
            case "truncated-flush":
                stream.FailOnFlush(2);
                break;
            case "write":
                stream.FailOnWrite(2);
                break;
            default:
                stream.FailDuringWrite(2);
                break;
        }

        await Assert.ThrowsAsync<IOException>(async () => await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken));
        Assert.True(stream.Faulted);
        Assert.Equal(baseline, stream.ToArray());
        Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
        await writer.InsertRowAsync("T", [8, "After truncation fault"], TestContext.Current.CancellationToken);
        await writer.DisposeAsync();
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("vector") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 7) && Equals(row["Label"], "Native encrypted row"));
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 8) && Equals(row["Label"], "After truncation fault"));
    }

    /// <summary>An unsuccessful physical shrink undo faults the writer and prevents disposal writes.</summary>
    [Fact]
    public async Task ShrinkUndoFailureProhibitsFurtherMutations()
    {
        byte[] variant = await NativeAgileAlgorithmTests.CreateVariantAsync("SHA384", "SHA384", 192, 192, "ChainingModeCFB");
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(variant, TestContext.Current.CancellationToken);
        stream.Position = 0;
        AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("vector") { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages }, leaveOpen: true, TestContext.Current.CancellationToken);
        try
        {
            await PrepareFreeTailAsync(writer);
            stream.FailWritesFrom(2);
            await Assert.ThrowsAsync<AggregateException>(async () => await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken));
            JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.InsertRowAsync("T", [9, "Refused after failed undo"], TestContext.Current.CancellationToken));
            Assert.Equal(JetErrorCode.WriterFaulted, failure.ErrorCode);
            int writes = stream.WriteCount;
            await writer.DisposeAsync();
            Assert.Equal(writes, stream.WriteCount);
        }
        finally
        {
            await writer.DisposeAsync();
        }
    }

    private static async Task PrepareFreeTailAsync(AccessWriter writer, int payloadLength = 32_000)
    {
        await writer.CreateTableAsync("Maintenance", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Maintenance", [1, new byte[payloadLength]], TestContext.Current.CancellationToken);
        await writer.DropTableAsync("Maintenance", TestContext.Current.CancellationToken);
    }
}
