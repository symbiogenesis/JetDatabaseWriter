namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>DAO3.6 independently establishes Jet3 Currency descriptors, keys and declaration limits.</summary>
public sealed class DaoJet3CurrencyDefinitionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(false, 18, 4, false)]
    [InlineData(false, 10, 2, false)]
    [InlineData(true, 18, 4, false)]
    [InlineData(true, 10, 2, false)]
    [InlineData(false, 0, 0, true)]
    [InlineData(true, 0, 0, true)]
    public async Task CurrencyDeclarations_MatchNativeDescriptorsAndKeys(bool addColumn, int precision, int scale, bool currency)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.Jet3Currency", databaseExtension: ".mdb");
        string oraclePath = session.CreateDatabasePath("oracle");
        string oracle = Literal(oraclePath);
        string subject = Literal(session.SourcePath);
        decimal largest = (currency, precision) switch
        {
            (true, _) => 922_337_203_685_477.5807m,
            (_, 10) => 99_999_999.99m,
            _ => 99_999_999_999_999.9999m,
        };
        decimal smallest = currency ? -922_337_203_685_477.5808m : -largest;
        object[][] rows =
        [
            [1, largest], [2, -0.0001m], [3, DBNull.Value], [4, 12.3456m],
            [5, 0m], [6, smallest], [7, 0.0001m], [8, -12.3456m],
        ];
        string nativeColumns = addColumn
            ? "$db.Execute('CREATE TABLE [Ledger] ([Id] LONG)',128)\n$db.Execute('ALTER TABLE [Ledger] ADD COLUMN [Amount] CURRENCY',128)"
            : "$db.Execute('CREATE TABLE [Ledger] ([Id] LONG,[Amount] CURRENCY)',128)";
        string nativeInserts = string.Join(Environment.NewLine, rows.Select(row =>
            "$db.Execute('INSERT INTO [Ledger] ([Id],[Amount]) VALUES (" + row[0] + "," +
            (row[1] is decimal amount ? amount.ToString(CultureInfo.InvariantCulture) : "NULL") + ")',128)"));
        PowerShellRunResult created = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $db = $engine.CreateDatabase({{oracle}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                {{nativeColumns}}
                $db.Execute('CREATE UNIQUE INDEX [Ascending] ON [Ledger] ([Amount])',128)
                $db.Execute('CREATE INDEX [Descending] ON [Ledger] ([Amount] DESC)',128)
                {{nativeInserts}}
                try { $db.Execute('CREATE TABLE [Unsupported] ([Amount] DECIMAL(18,4))',128); throw 'Jet3 accepted DECIMAL.' }
                catch {
                    if ($_.Exception.Message -eq 'Jet3 accepted DECIMAL.' -or 3292 -notin @($engine.Errors | ForEach-Object { $_.Number })) { throw }
                    Write-Output 'DECIMAL_REFUSED=3292'
                }
            } finally { $db.Close() }
            $db = $engine.CreateDatabase({{subject}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            try {
                if ({{(addColumn ? "$true" : "$false")}}) { $db.Execute('CREATE TABLE [Ledger] ([Id] LONG)',128) }
            } finally { $db.Close() }
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        AssertDao(created);
        Assert.Contains("DECIMAL_REFUSED=3292", created.StandardOutput, StringComparison.Ordinal);

        var amountDefinition = new ColumnDefinition("Amount", typeof(decimal))
        {
            IsCurrency = currency,
            NumericPrecision = (byte)precision,
            NumericScale = (byte)scale,
        };
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            if (addColumn)
            {
                await writer.AddColumnAsync("Ledger", amountDefinition, Ct);
            }
            else
            {
                await writer.CreateTableAsync(
                    "Ledger",
                    [new ColumnDefinition("Id", typeof(int)), amountDefinition],
                    [new IndexDefinition("Ascending", "Amount") { IsUnique = true }, new IndexDefinition("Descending", "Amount") { DescendingColumns = ["Amount"] }],
                    Ct);
            }
        }

        if (addColumn)
        {
            PowerShellRunResult indexed = Jet3RoundTripEnvironment.RunEngineScript(
                $$"""
                $db = $engine.OpenDatabase({{subject}})
                try {
                    $db.Execute('CREATE UNIQUE INDEX [Ascending] ON [Ledger] ([Amount])',128)
                    $db.Execute('CREATE INDEX [Descending] ON [Ledger] ([Amount] DESC)',128)
                } finally { $db.Close() }
                """,
                session.WorkDir,
                TimeSpan.FromMinutes(2));
            AssertDao(indexed);
        }

        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.InsertRowsAsync("Ledger", rows, Ct);
        }

        CurrencyImage expected = await ReadImageAsync(oraclePath);
        CurrencyImage actual = await ReadImageAsync(session.SourcePath);
        Assert.Equal(expected.AscendingKeys, actual.AscendingKeys);
        Assert.Equal(expected.DescendingKeys, actual.DescendingKeys);

        byte[] beforeRefusal = await File.ReadAllBytesAsync(session.SourcePath, Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("Ledger", [100, 12.3456m], Ct));
            foreach ((byte refusedPrecision, byte refusedScale) in new (byte, byte)[] { (18, 0), (15, 0), (19, 4), (10, 5) })
            {
                var refused = new ColumnDefinition("Refused", typeof(decimal)) { NumericPrecision = refusedPrecision, NumericScale = refusedScale };
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.CreateTableAsync("Refused", [refused], Ct));
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.AddColumnAsync("Ledger", refused, Ct));
            }
        }

        Assert.Equal(beforeRefusal, await File.ReadAllBytesAsync(session.SourcePath, Ct));
        string compacted = Literal(session.CompactedPath);
        PowerShellRunResult verified = Jet3RoundTripEnvironment.RunEngineScript(
            $$"""
            $expectedOrder = '3,6,8,2,5,7,4,1'
            $db = $engine.OpenDatabase({{subject}})
            try {
                if ($db.TableDefs('Ledger').Fields('Amount').Type -ne 5) { throw 'Not Currency.' }
                $rs = $db.OpenRecordset('SELECT [Id] FROM [Ledger] ORDER BY [Amount]')
                try {
                    $ids = @()
                    while (-not $rs.EOF) { $ids += [string]$rs.Fields('Id').Value; $rs.MoveNext() }
                    if (($ids -join ',') -ne $expectedOrder) { throw ('Wrong Currency ordering: ' + ($ids -join ',')) }
                } finally { $rs.Close() }
                $rejected = $false
                try { $db.Execute('INSERT INTO [Ledger] ([Id],[Amount]) VALUES (100,12.3456)',128) }
                catch { if (3022 -notin @($engine.Errors | ForEach-Object { $_.Number })) { throw }; $rejected = $true }
                if (-not $rejected) { throw 'DAO accepted duplicate Currency key.' }
                $db.Execute('INSERT INTO [Ledger] ([Id],[Amount]) VALUES (9,NULL)',128)
                $db.Execute('INSERT INTO [Ledger] ([Id],[Amount]) VALUES (10,4.5678)',128)
                $db.Execute('UPDATE [Ledger] SET [Amount]=4.5679 WHERE [Id]=10',128)
                $db.Execute('DELETE FROM [Ledger] WHERE [Id]=2',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{subject}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', 32)
            $db = $engine.OpenDatabase({{compacted}})
            try {
                $rs = $db.OpenRecordset('SELECT [Amount] FROM [Ledger] WHERE [Id]=10')
                try { if ($rs.EOF -or $rs.Fields('Amount').Value -ne [decimal]4.5679) { throw 'Lost Currency mutation.' } } finally { $rs.Close() }
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Ledger] WHERE [Amount] IS NULL')
                try { if ($rs.Fields('N').Value -ne 2) { throw 'Lost nullable unique keys.' } } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'CURRENCY_NATIVE_OK'
            """,
            session.WorkDir,
            TimeSpan.FromMinutes(2));
        AssertDao(verified);
        Assert.Contains("CURRENCY_NATIVE_OK", verified.StandardOutput, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, Ct);
        using DataTable table = await reader.ReadTableAsync("Ledger", cancellationToken: Ct);
        Assert.Equal(9, table.Rows.Count);
        Assert.Equal(4.5679m, Assert.Single(table.Select("Id = 10"))["Amount"]);
        Assert.Equal(largest, Assert.Single(table.Select("Id = 1"))["Amount"]);
        Assert.Equal(smallest, Assert.Single(table.Select("Id = 6"))["Amount"]);
        Assert.Equal(2, table.Select("Amount IS NULL").Length);
        Assert.Empty(table.Select("Id = 2"));
        Assert.Equal(expected.Descriptor, actual.Descriptor);
    }

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private static void AssertDao(PowerShellRunResult result)
        => Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");

    private static async Task<CurrencyImage> ReadImageAsync(string path)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Ledger", Ct));
        TableDef definition = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(entry.TDefPage, Ct));
        ColumnInfo amount = Assert.Single(definition.Columns, column => column.Name == "Amount");
        Assert.Equal(ColumnType.MoneyType, amount.Type);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Ledger", Ct);
        string[] ascending = await ReadKeysAsync(reader, Assert.Single(indexes, index => index.Name == "Ascending"));
        string[] descending = await ReadKeysAsync(reader, Assert.Single(indexes, index => index.Name == "Descending"));
        return new CurrencyImage([.. amount.RawDescriptor], ascending, descending);
    }

    private static async Task<string[]> ReadKeysAsync(AccessReader reader, IndexMetadata index)
    {
        byte[] page = await reader.ReadPageCopyAsync(index.FirstDp, Ct);
        Assert.Equal(Constants.PageTypes.IndexLeaf, page[0]);
        return [.. IndexPageCodec.DecodeLeafEntries(reader.Database.Format.IndexPage, page, reader.Database.Format.PageSize).Select(entry => Convert.ToHexString(entry.Key))];
    }

    private sealed record CurrencyImage(byte[] Descriptor, string[] AscendingKeys, string[] DescendingKeys);
}
