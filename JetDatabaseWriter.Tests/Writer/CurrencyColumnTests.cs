namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Currency columns declared with <see cref="ColumnDefinition.IsCurrency"/>.
/// Before it existed, no public declaration created a Currency column: a
/// <see cref="decimal"/> column was always Decimal, so copying an Access
/// Currency column's metadata into <c>CreateTableAsync</c> gave Decimal(18,0),
/// which stored 12.3456 as 12.
/// </summary>
public sealed class CurrencyColumnTests
{
    private const string TableName = "Ledger";

    /// <summary>Gets each Access-authored fixture with a Currency column, paired with every target format.</summary>
    public static TheoryData<string, DatabaseFormat> CurrencySourcesAndFormats
    {
        get
        {
            var data = new TheoryData<string, DatabaseFormat>();
            foreach (string source in CurrencySources.Where(TestDatabases.IsReadable))
            {
                foreach (DatabaseFormat format in Formats)
                {
                    data.Add(Path.GetFileName(source), format);
                }
            }

            return data;
        }
    }

    /// <summary>Gets every format the writer creates.</summary>
    public static TheoryData<DatabaseFormat> AllFormats => [.. Formats];

    private static DatabaseFormat[] Formats => [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    private static string[] CurrencySources => [TestDatabases.Jet3Test, TestDatabases.TestV2000, TestDatabases.NorthwindTraders];

    /// <summary>
    /// Copies an Access Currency column's metadata into <c>CreateTableAsync</c>.
    /// The new column must be Currency too and keep four decimal places.
    /// </summary>
    /// <param name="sourceFile">The Access-authored fixture's file name.</param>
    /// <param name="format">The format of the database the column is copied into.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(CurrencySourcesAndFormats))]
    public async Task CopiedCurrencyMetadata_CreatesCurrencyColumn(string sourceFile, DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string source = CurrencySources.Single(path => Path.GetFileName(path) == sourceFile);

        ColumnMetadata currency = await FindCurrencyColumnAsync(source, ct);
        Assert.True(currency.IsCurrency);
        Assert.Equal(typeof(decimal), currency.ClrType);

        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition(currency.Name, currency.ClrType)
                    {
                        NumericPrecision = currency.NumericPrecision,
                        NumericScale = currency.NumericScale,
                        IsCurrency = currency.IsCurrency,
                    },
                ],
                ct);
            await writer.InsertRowAsync(TableName, [1, 12.3456m], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        ColumnMetadata copied = Assert.Single(await reader.GetColumnMetadataAsync(TableName, ct), c => c.Name == currency.Name);
        Assert.Equal("Currency", copied.TypeName);
        Assert.True(copied.IsCurrency);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(12.3456m, Assert.Single(table.Rows.Cast<DataRow>())[currency.Name]);
    }

    /// <summary>
    /// <see cref="ColumnDefinition.IsCurrency"/> creates a Currency descriptor
    /// (0x05) on every format, through <c>CreateTableAsync</c> and
    /// <c>AddColumnAsync</c>. It keeps four decimal places whatever
    /// <see cref="ColumnDefinition.NumericScale"/> says, rounds half to even
    /// past them, holds the whole Currency range and rejects a value outside it.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task IsCurrency_CreatesCurrencyColumnOnEveryFormat(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        decimal[] values = [12.3456m, -0.0001m, 922_337_203_685_477.5807m, -922_337_203_685_477.5808m, 1.23455m, 1.23465m];
        decimal[] expected = [12.3456m, -0.0001m, 922_337_203_685_477.5807m, -922_337_203_685_477.5808m, 1.2346m, 1.2346m];

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Amount", typeof(decimal)) { IsCurrency = true },
                    new ColumnDefinition("Scaled", typeof(decimal)) { IsCurrency = true, NumericPrecision = 3, NumericScale = 1 },
                ],
                ct);

            for (int i = 0; i < values.Length; i++)
            {
                await writer.InsertRowAsync(TableName, [i, values[i], i == 0 ? 12.3456m : DBNull.Value], ct);
            }

            await writer.AddColumnAsync(TableName, new ColumnDefinition("Fee", typeof(decimal)) { IsCurrency = true }, ct);
            Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "Id", 0, new Dictionary<string, object?> { ["Fee"] = 0.0005m }, ct));

            await Assert.ThrowsAsync<OverflowException>(async () =>
                await writer.InsertRowAsync(TableName, [99, 922_337_203_685_477.5808m, DBNull.Value, DBNull.Value], ct));
        }

        IReadOnlyList<ColumnInfo> descriptors = await ReadColumnDescriptorsAsync(ms, ct);
        Assert.All(descriptors.Skip(1), column => Assert.Equal(ColumnType.MoneyType, column.Type));

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        foreach (ColumnMetadata column in (await reader.GetColumnMetadataAsync(TableName, ct)).Skip(1))
        {
            Assert.Equal("Currency", column.TypeName);
            Assert.True(column.IsCurrency);
            Assert.Equal(typeof(decimal), column.ClrType);
            Assert.Equal(0, column.NumericPrecision);
            Assert.Equal(0, column.NumericScale);
        }

        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        DataRow[] rows = [.. table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"])];
        Assert.Equal(expected, rows.Select(row => (decimal)row["Amount"]));
        Assert.Equal(12.3456m, rows[0]["Scaled"]);
        Assert.Equal(0.0005m, rows[0]["Fee"]);
        Assert.Equal(DBNull.Value, rows[1]["Fee"]);
    }

    /// <summary>
    /// A unique index on a Currency column compares stored values.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task UniqueIndex_OnCurrencyColumn_RejectsDuplicate(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amount", typeof(decimal)) { IsCurrency = true }],
                [new IndexDefinition("UX_Amount", "Amount") { IsUnique = true }],
                ct);
            await writer.InsertRowAsync(TableName, [1, 12.3456m], ct);

            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync(TableName, [2, 12.3456m], ct));
            await writer.InsertRowAsync(TableName, [3, 12.3457m], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal([12.3456m, 12.3457m], table.Rows.Cast<DataRow>().Select(row => (decimal)row["Amount"]));
    }

    /// <summary>
    /// <see cref="ColumnDefinition.IsCurrency"/> needs a <see cref="decimal"/>
    /// column, in <c>CreateTableAsync</c> and <c>AddColumnAsync</c>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task IsCurrency_OnNonDecimalColumn_ThrowsArgumentException(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            foreach (Type clrType in new[] { typeof(double), typeof(int), typeof(string), typeof(DateTime) })
            {
                ArgumentException create = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await writer.CreateTableAsync("Bad", [new ColumnDefinition("Amount", clrType) { IsCurrency = true }], ct));
                Assert.Contains("IsCurrency", create.Message, StringComparison.Ordinal);
            }

            await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int))], ct);
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.AddColumnAsync(TableName, new ColumnDefinition("Amount", typeof(double)) { IsCurrency = true }, ct));
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal(TableName, Assert.Single(await reader.ListTablesAsync(ct)));
        Assert.Single(await reader.GetColumnMetadataAsync(TableName, ct));
    }

    /// <summary>
    /// Access has no multi-value Currency type, so
    /// <see cref="ColumnDefinition.IsCurrency"/> is refused on Attachment and
    /// multi-value columns.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task IsCurrency_OnComplexColumn_ThrowsArgumentException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb, ct);
        ColumnDefinition[] complex =
        [
            new("Amounts", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(decimal), IsCurrency = true },
            new("Files", typeof(byte[])) { IsAttachment = true, IsCurrency = true },
        ];

        await using (AccessWriter writer = await OpenWriterAsync(ms, ct))
        {
            foreach (ColumnDefinition column in complex)
            {
                ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                    await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), column], ct));
                Assert.Contains("Currency", ex.Message, StringComparison.Ordinal);
            }
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Empty(await reader.ListTablesAsync(ct));
    }

    private static async Task<ColumnMetadata> FindCurrencyColumnAsync(string path, CancellationToken cancellationToken)
    {
        await using AccessReader reader = await TestDatabases.OpenAsync(path, cancellationToken: cancellationToken);
        foreach (string table in await reader.ListTablesAsync(cancellationToken))
        {
            IReadOnlyList<ColumnMetadata> columns = await reader.GetColumnMetadataAsync(table, cancellationToken);
            if (columns.FirstOrDefault(c => c.TypeName == "Currency") is { } column)
            {
                return column;
            }
        }

        throw new InvalidOperationException($"{Path.GetFileName(path)} has no Currency column.");
    }

    private static async Task<IReadOnlyList<ColumnInfo>> ReadColumnDescriptorsAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(TableName, cancellationToken);
        Assert.NotNull(entry);
        TableDef? definition = await harness.ReadTableDefAsync(entry.TDefPage, cancellationToken);
        Assert.NotNull(definition);
        return definition.Columns;
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format, CancellationToken cancellationToken)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken))
        {
        }

        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }
}
