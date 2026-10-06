namespace JetDatabaseWriter.Tests.Schema;

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

/// <summary>Access limits every table to 255 fields, in every database format.</summary>
public sealed class TableColumnLimitTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every database format and write mode.</summary>
    /// <returns>The format and mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() =>
        WriteModes.Combine(DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb);

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task TooManyColumns_RejectsCreateAndAddWithoutChangingDatabase(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Full", Columns(255), Ct);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(
            stream, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                JetLimitationException create = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.CreateTableAsync("TooWide", Columns(256), Ct));
                Assert.Contains("TooWide", create.Message, StringComparison.Ordinal);
                Assert.Contains("255", create.Message, StringComparison.Ordinal);
                JetLimitationException add = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.AddColumnAsync("Full", new ColumnDefinition("C255", typeof(int)), Ct));
                Assert.Contains("Full", add.Message, StringComparison.Ordinal);
                Assert.Contains("255", add.Message, StringComparison.Ordinal);
            }, Ct);
        }

        Assert.Equal(before, stream.ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal("Full", Assert.Single(await reader.ListTablesAsync(Ct)));
        Assert.Equal(255, (await reader.GetColumnMetadataAsync("Full", Ct)).Count);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Adding255thColumn_AcceptsBoundaryAndPreservesRows(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        object[] values = [.. Enumerable.Range(0, 254).Select(i => (object)i)];
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, WriteModes.WriterOptions(mode), leaveOpen: true, cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () =>
            {
                await writer.CreateTableAsync("Boundary", Columns(254), Ct);
                await writer.InsertRowAsync("Boundary", values, Ct);
                await writer.AddColumnAsync("Boundary", new ColumnDefinition("C254", typeof(int)), Ct);
                await Assert.ThrowsAsync<JetLimitationException>(async () =>
                    await writer.AddColumnAsync("Boundary", new ColumnDefinition("C255", typeof(int)), Ct));
                await writer.CreateTableAsync("StillUsable", [new("Id", typeof(int))], Ct);
            }, Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        Assert.Equal(255, (await reader.GetColumnMetadataAsync("Boundary", Ct)).Count);
        object[] row = Assert.Single(await reader.Rows("Boundary", cancellationToken: Ct).ToListAsync(Ct));
        Assert.Equal(values, row.Take(254));
        Assert.Equal(DBNull.Value, row[254]);
        Assert.Contains("StillUsable", await reader.ListTablesAsync(Ct));
    }

    private static ColumnDefinition[] Columns(int count) =>
        [.. Enumerable.Range(0, count).Select(i => new ColumnDefinition($"C{i}", typeof(int)))];
}
