namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

#pragma warning disable CA1812 // Rows<T> instantiates the fixture projection.

/// <summary>Access-authored fixed Text reads, corroborated by Jackcess DatabaseTest.testFixedText.</summary>
public sealed class FixedTextFixtureTests
{
    /// <summary>Gets the four fixed Text fixtures from Jackcess.</summary>
    public static TheoryData<string> Fixtures =>
    [
        TestDatabases.FixedTextTestV2000,
        TestDatabases.FixedTextTestV2003,
        TestDatabases.FixedTextTestV2007,
        TestDatabases.FixedTextTestV2010,
    ];

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task FixedText_ReadsThroughTypedStringAndPocoPaths(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        ColumnMetadata column = Assert.Single(await reader.GetColumnMetadataAsync("users", TestContext.Current.CancellationToken), c => c.Name == "c_flag_");
        Assert.True(column.IsFixedLength);
        using DataTable table = await reader.ReadTableAsync("users", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("N", table.Rows[0]["c_flag_"]);
        int ordinal = table.Columns["c_flag_"]!.Ordinal;
        await foreach (string[] row in reader.RowsAsStrings("users", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("N", row[ordinal]);
            break;
        }

        await foreach (FlagRow row in reader.Rows<FlagRow>("users", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Equal("N", row.Flag);
            break;
        }
    }

    [Fact]
    public void FixedText_ExactWidthSerializesInTheFixedArea()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TableDef definition = FixedTextDefinition();
        byte[] row = new RowEncoder(format).SerializeRow(definition, ["Y"]);
        Assert.Equal(new byte[] { 0x59, 0x00 }, row[2..4]);
        byte[] nullRow = new RowEncoder(format).SerializeRow(definition, [DBNull.Value]);
        Assert.Equal(row.Length, nullRow.Length);
        Assert.Equal(new byte[] { 0x00, 0x00 }, nullRow[2..4]);
    }

    [Fact]
    public void FixedText_ShortValueIsSpacePadded()
    {
        var encoder = new RowEncoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));
        byte[] row = encoder.SerializeRow(FixedTextDefinition(), [string.Empty]);
        Assert.Equal(new byte[] { 0x20, 0x00 }, row[2..4]);
    }

    [Fact]
    public void FixedText_OverWidthIsRefusedWithoutTruncation()
    {
        var encoder = new RowEncoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb));
        Assert.Throws<JetLimitationException>(() => encoder.SerializeRow(FixedTextDefinition(), ["YY"]));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(".mdb", 64)]
    [InlineData(".accdb", 128)]
    public async Task FixedText_DaoPadsShortValuesAndCompactsWriterValues(string extension, int attributes)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: extension);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{path}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{attributes}})
            try {
                $td = $db.CreateTableDef('FixedText')
                $field = $td.CreateField('Value', 10, 4)
                $field.Attributes = 1
                $field.AllowZeroLength = $true
                $td.Fields.Append($field)
                $td.Fields.Append($td.CreateField('Id', 4))
                $db.TableDefs.Append($td)
                $db.Execute("INSERT INTO FixedText (Id, [Value]) VALUES (1, '')", 128)
                $db.Execute("INSERT INTO FixedText (Id, [Value]) VALUES (2, 'A')", 128)
                $db.Execute("INSERT INTO FixedText (Id, [Value]) VALUES (3, 'ABCD')", 128)
                $db.Execute("INSERT INTO FixedText (Id, [Value]) VALUES (4, 'ABCDE')", 128)
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0 && File.Exists(session.SourcePath), $"DAO failed: {result.StdOut}\n{result.StdErr}");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("FixedText", ["B", 5], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("FixedText", [string.Empty, 6], TestContext.Current.CancellationToken);
        }

        session.RunDaoCompact();
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        Assert.True(Assert.Single(await reader.GetColumnMetadataAsync("FixedText", TestContext.Current.CancellationToken), column => column.Name == "Value").IsFixedLength);
        using DataTable table = await reader.ReadTableAsync("FixedText", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(6, table.Rows.Count);
        Assert.Equal("    ", Assert.Single(table.Select("Id = 1"))["Value"]);
        Assert.Equal("A   ", Assert.Single(table.Select("Id = 2"))["Value"]);
        Assert.Equal("ABCD", Assert.Single(table.Select("Id = 3"))["Value"]);
        Assert.Equal("ABCD", Assert.Single(table.Select("Id = 4"))["Value"]);
        Assert.Equal("B   ", Assert.Single(table.Select("Id = 5"))["Value"]);
        Assert.Equal("    ", Assert.Single(table.Select("Id = 6"))["Value"]);
    }

    private static TableDef FixedTextDefinition() => new()
    {
        Columns = [new ColumnInfo { Name = "Flag", Type = ColumnType.TextType, Size = 2, Flags = Constants.ColumnDescriptorFlags.Fixed }],
    };

    private sealed class FlagRow
    {
        [Column("c_flag_")]
        public string? Flag { get; set; }
    }
}
