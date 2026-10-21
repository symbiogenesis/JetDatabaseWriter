namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>Native ordinary BINARY columns occupy their full declared fixed width.</summary>
public sealed class FixedBinaryTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterBinary_DaoReadsZeroPaddingAndCompacts()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken);
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), 4)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Samples", [1, new byte[] { 0x01 }], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            Write-Output "ATTR=$($db.TableDefs('Samples').Fields('Data').Attributes)"
            $rs = $db.OpenRecordset('Samples')
            try {
                $blob = [byte[]]$rs.Fields('Data').Value
                Write-Output "VALUE=$([BitConverter]::ToString($blob))"
            } finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ATTR=1", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("VALUE=01-00-00-00", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable table = await reader.ReadTableAsync("Samples", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(new byte[] { 0x01, 0x00, 0x00, 0x00 }, Assert.IsType<byte[]>(Assert.Single(table.Select())["Data"]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void BinaryDeclaration_UsesFixedWidthAndZeroPadding(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        TableDef definition = TDefPageBuilder.BuildTableDefinition([new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), 4)], format);
        ColumnInfo column = definition.Columns[1];
        Assert.True(column.IsFixed);
        Assert.Equal(4, column.Size);
        Assert.Equal(4, column.FixedOff);
        var encoder = new RowEncoder(format);
        byte[] row = encoder.SerializeRow(definition, [1, new byte[] { 0x01 }]);
        int start = format.RowFields.NumCols + 4;
        Assert.Equal(new byte[] { 0x01, 0x00, 0x00, 0x00 }, row.AsSpan(start, 4).ToArray());
        byte[] nullRow = encoder.SerializeRow(definition, [1, DBNull.Value]);
        Assert.Equal(row.Length, nullRow.Length);
        Assert.Equal(new byte[4], nullRow.AsSpan(start, 4).ToArray());
        Assert.Throws<JetLimitationException>(() => encoder.SerializeRow(definition, [1, new byte[5]]));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public void BinaryCatalogOverrides_ReceiveSeparateVariableSlots(DatabaseFormat databaseFormat)
    {
        var format = JetFormat.ForNewDatabase(databaseFormat);
        TableDef definition = TDefPageBuilder.BuildTableDefinition(
            [
                new ColumnDefinition("Name", typeof(string), 10),
                new ColumnDefinition("Owner", typeof(byte[]), 255) { DescriptorFlagsOverride = 0x32 },
                new ColumnDefinition("RmtInfoShort", typeof(byte[]), 255) { DescriptorFlagsOverride = 0x12 },
                new ColumnDefinition("Id", typeof(int)),
            ],
            format);
        Assert.False(definition.Columns[1].IsFixed);
        Assert.False(definition.Columns[2].IsFixed);
        Assert.Equal(1, definition.Columns[1].VarIdx);
        Assert.Equal(2, definition.Columns[2].VarIdx);
        Assert.Equal(0, definition.Columns[3].FixedOff);
    }

    [Fact]
    public void BinaryDescriptor_VariableNativeVariantRemainsVariable()
    {
        Assert.False(new ColumnInfo { Type = ColumnType.BinaryType, Flags = Constants.ColumnDescriptorFlags.Unknown, Size = 255 }.IsFixed);
        Assert.True(new ColumnInfo { Type = ColumnType.BinaryType, Flags = Constants.ColumnDescriptorFlags.Unknown | Constants.ColumnDescriptorFlags.Fixed, Size = 255 }.IsFixed);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task FixedBinary_ReadsFullWidthAndNullThroughPublicReader(DatabaseFormat databaseFormat)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, databaseFormat, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Samples", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Data", typeof(byte[]), 4)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Samples", [1, new byte[] { 0x01 }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Samples", [2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: TestContext.Current.CancellationToken);
        Assert.IsType<DBNull>(Assert.Single(catalog.Select("Name = 'Samples'"))["LvProp"]);
        Assert.True(Assert.Single(await reader.GetColumnMetadataAsync("Samples", TestContext.Current.CancellationToken), column => column.Name == "Data").IsFixedLength);
        using DataTable table = await reader.ReadTableAsync("Samples", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(new byte[] { 0x01, 0x00, 0x00, 0x00 }, Assert.IsType<byte[]>(Assert.Single(table.Select("Id = 1"))["Data"]));
        Assert.IsType<DBNull>(Assert.Single(table.Select("Id = 2"))["Data"]);
    }
}
