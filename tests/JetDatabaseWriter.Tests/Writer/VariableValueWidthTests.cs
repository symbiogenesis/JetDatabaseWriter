namespace JetDatabaseWriter.Tests.Writer;

using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Oversized variable values refuse without changing rows, indexes or transaction state.</summary>
public sealed class VariableValueWidthTests
{
    /// <summary>Gets the supported database formats and write modes.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes
        => WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    /// <summary>A unique index must never contain a different value from its stored row.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_OversizedTextRefusesBeforeChangingUniqueIndex(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(string), 5)], [new IndexDefinition("IX", "Value") { IsUnique = true }], ct);
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                byte[] original = stream.ToArray();
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.InsertRowAsync("T", new RowValues { ["Value"] = "ABCDEFG" }, ct));
                Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
                Assert.Equal(original, stream.ToArray());
                await writer.InsertRowAsync("T", new RowValues { ["Value"] = "ABCDE" }, ct);
            },
                ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        using DataTable table = await reader.ReadTableAsync("T", cancellationToken: ct);
        DataRow stored = Assert.Single(table.AsEnumerable());
        Assert.Equal("ABCDE", stored["Value"]);
        int matches = 0;
        await foreach (object[] row in reader.SeekRowsAsync("T", "IX", ["ABCDE"], ct))
        {
            Assert.Equal("ABCDE", row[0]);
            matches++;
        }

        Assert.Equal(1, matches);
    }

    /// <summary>A late oversized row cannot partially insert a batch or consume its identities.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Batch_OversizedTextRestoresBytesAndAutoNumber(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new ColumnDefinition("Value", typeof(string), 5)], ct);
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                byte[] original = stream.ToArray();
                RowValues[] rows = [new() { ["Value"] = "ABCDE" }, new() { ["Value"] = "ABCDEZZ" }];
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowsAsync("T", rows, ct));
                Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
                Assert.Equal(original, stream.ToArray());
                await writer.InsertRowAsync("T", new RowValues { ["Value"] = "ABCDE" }, ct);
            },
                ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        using DataTable table = await reader.ReadTableAsync("T", cancellationToken: ct);
        DataRow stored = Assert.Single(table.AsEnumerable());
        Assert.Equal(1, stored["Id"]);
        Assert.Equal("ABCDE", stored["Value"]);
    }

    /// <summary>An oversized replacement cannot lose the original row or its key.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Update_OversizedTextPreservesOriginalRow(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(string), 5)], [new IndexDefinition("IX", "Value") { IsUnique = true }], ct);
            await writer.InsertRowAsync("T", new RowValues { ["Value"] = "ABCDE" }, ct);
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
            {
                byte[] original = stream.ToArray();
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.UpdateRowsAsync("T", "Value", "ABCDE", new Dictionary<string, object?> { ["Value"] = "ABCDEFG" }, ct));
                Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
                Assert.Equal(original, stream.ToArray());
            },
                ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        using DataTable table = await reader.ReadTableAsync("T", cancellationToken: ct);
        DataRow stored = Assert.Single(table.AsEnumerable());
        Assert.Equal("ABCDE", stored["Value"]);
    }

    /// <summary>Native variable binary descriptors also refuse rather than clipping the payload.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The transaction mode.</param>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Insert_OversizedVariableBinaryRefuses(DatabaseFormat format, WriteMode mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: ct);
        await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(byte[]), 5) { ForceVariableLengthStorage = true }], ct);
        await WriteModes.RunAsync(
            writer,
            mode,
            async () =>
        {
            byte[] original = stream.ToArray();
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.InsertRowAsync("T", new RowValues { ["Value"] = new byte[6] }, ct));
            Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
            Assert.Equal(original, stream.ToArray());
        },
            ct);
    }

    /// <summary>Compression cannot increase the character limit or split a surrogate pair.</summary>
    /// <param name="format">The Unicode database format.</param>
    /// <param name="compressed">Whether Unicode compression is enabled.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task UnicodeWidth_BoundaryPreservesCompleteCodeUnits(DatabaseFormat format, bool compressed)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        foreach (string value in new[] { "ABCDE", "AB\u4E2DDE", "\uD83D\uDE00ABC" })
        {
            await using var stream = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, leaveOpen: true, cancellationToken: ct))
            {
                await writer.CreateTableAsync("T", [new ColumnDefinition("Value", typeof(string), 5) { IsCompressedUnicode = compressed }], ct);
                byte[] original = stream.ToArray();
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.InsertRowAsync("T", new RowValues { ["Value"] = "ABCD\uD83D\uDE00" }, ct));
                Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
                Assert.Equal(original, stream.ToArray());
                await writer.InsertRowAsync("T", new RowValues { ["Value"] = value }, ct);
            }

            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
            using DataTable table = await reader.ReadTableAsync("T", cancellationToken: ct);
            DataRow stored = Assert.Single(table.AsEnumerable());
            Assert.Equal(value, stored["Value"]);
        }
    }
}
