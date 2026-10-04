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
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Decimal columns on Jet3 (Access 97) databases, which have no Decimal type.
/// <c>JetTypeInfo.TypeCodeFromDefinition</c> mapped every decimal to Numeric
/// (0x10) on every format, and <c>TDefPageBuilder</c> left the Jet3
/// descriptor's precision and scale bytes zero, so the column read back as
/// Decimal(18,0) (precision 0 read as 18) and <c>RowEncoder</c> rounded every
/// value to a whole number: 12.34 was stored as 12. A decimal column on Jet3 is
/// now created as Currency (0x05), which keeps four decimal places; a declared
/// precision or scale Currency cannot hold is refused before anything is
/// written.
/// </summary>
public sealed class Jet3DecimalColumnTests
{
    private const string TableName = "Ledger";

    private enum WriteMode
    {
        /// <summary>Each write commits on its own.</summary>
        AutoCommit = 0,

        /// <summary>The writer journals each operation (<see cref="AccessWriterOptions.UseTransactionalWrites"/>).</summary>
        TransactionalWrites = 1,

        /// <summary>One explicit transaction holds every write.</summary>
        ExplicitTransaction = 2,
    }

    /// <summary>Gets the write modes.</summary>
    public static TheoryData<string> WriteModes => [.. Enum.GetNames<WriteMode>()];

    /// <summary>
    /// A decimal column, created in each write mode, is a Currency column that
    /// keeps every value's fractions. A declared scale below four is not
    /// applied, because Currency does not store it.
    /// </summary>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task CreateTable_Decimal_IsStoredAsCurrencyAndKeepsFractions(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);

        await WriteInModeAsync(
            ms,
            mode,
            async writer =>
            {
                await writer.CreateTableAsync(
                    TableName,
                    [
                        new ColumnDefinition("Id", typeof(int)),
                        new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 },
                        new ColumnDefinition("Wide", typeof(decimal)) { NumericPrecision = 18, NumericScale = 4 },
                    ],
                    ct);
                await writer.InsertRowAsync(TableName, [1, 12.34m, 95.5m], ct);
                await writer.InsertRowAsync(TableName, [2, 0.75m, -0.0001m], ct);
                await writer.InsertRowAsync(TableName, [3, 12_345_678.9m, 99_999_999_999_999.9999m], ct);
                await writer.InsertRowAsync(TableName, [4, 12.345m, DBNull.Value], ct);
                await writer.InsertRowAsync(TableName, [5, -1m, -99_999_999_999_999.9999m], ct);
            },
            ct);

        IReadOnlyList<ColumnInfo> descriptors = await ReadColumnDescriptorsAsync(ms, ct);
        Assert.All(descriptors.Skip(1), column => Assert.Equal(ColumnType.MoneyType, column.Type));

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        foreach (ColumnMetadata column in (await reader.GetColumnMetadataAsync(TableName, ct)).Skip(1))
        {
            Assert.Equal("Currency", column.TypeName);
            Assert.True(column.IsCurrency);
            Assert.Equal(typeof(decimal), column.ClrType);
        }

        object[][] expected =
        [
            [1, 12.34m, 95.5m],
            [2, 0.75m, -0.0001m],
            [3, 12_345_678.9m, 99_999_999_999_999.9999m],
            [4, 12.345m, DBNull.Value],
            [5, -1m, -99_999_999_999_999.9999m],
        ];

        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(expected, table.Rows.Cast<DataRow>().Select(row => row.ItemArray).OrderBy(row => (int)row[0]!));
        Assert.Equal(expected, (await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct)).OrderBy(row => (int)row[0]));
    }

    /// <summary>
    /// Currency keeps four decimal places and its largest value is
    /// 922,337,203,685,477.5807, so it holds every value of a declaration
    /// with at most fourteen integer digits
    /// (<c>NumericPrecision - NumericScale</c>). A declared scale above four,
    /// or more integer digits, throws <see cref="NotSupportedException"/>
    /// naming the column before anything is written. That includes fifteen
    /// integer digits, such as Decimal(15,0) and Decimal(19,4), whose valid
    /// values from 922,337,203,685,477.5808 up would not fit, and the
    /// default Decimal(18,0).
    /// </summary>
    /// <param name="precision">The declared precision; 0 leaves the default.</param>
    /// <param name="scale">The declared scale.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(0, 0)]
    [InlineData(18, 0)]
    [InlineData(16, 0)]
    [InlineData(15, 0)]
    [InlineData(16, 1)]
    [InlineData(19, 4)]
    [InlineData(20, 4)]
    [InlineData(10, 5)]
    [InlineData(28, 28)]
    public async Task CreateTable_DecimalCurrencyCannotHold_ThrowsAndCreatesNothing(int precision, int scale)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);
        ColumnDefinition amount = precision == 0
            ? new ColumnDefinition("Amt", typeof(decimal))
            : new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = (byte)precision, NumericScale = (byte)scale };
        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await writer.CreateTableAsync(TableName, [new ColumnDefinition("Id", typeof(int)), amount], ct));
            Assert.Contains("'Amt'", ex.Message, StringComparison.Ordinal);
            Assert.Contains("Currency", ex.Message, StringComparison.Ordinal);
        }

        Assert.Equal(before, ms.ToArray());

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 18, NumericScale = 4 },
                    new ColumnDefinition("Whole", typeof(decimal)) { NumericPrecision = 14 },
                ],
                ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal(TableName, Assert.Single(await reader.ListTablesAsync(ct)));
    }

    /// <summary>
    /// Each widest declaration Jet3 accepts, fourteen integer digits with a
    /// scale of 0 to 4, stores its largest and smallest values exactly.
    /// </summary>
    /// <param name="scale">The declared scale; the precision is fourteen more.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task CreateTable_WidestAcceptedDecimal_StoresItsExtremeValues(int scale)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);
        decimal step = 1m;
        for (int i = 0; i < scale; i++)
        {
            step /= 10m;
        }

        // The declaration's largest value: fourteen nines, then `scale` nines after the point.
        decimal largest = 100_000_000_000_000m - step;

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = (byte)(14 + scale), NumericScale = (byte)scale },
                ],
                ct);
            await writer.InsertRowsAsync(TableName, [[1, largest], [2, -largest], [3, step]], ct);
        }

        Assert.Equal(ColumnType.MoneyType, (await ReadColumnDescriptorsAsync(ms, ct))[1].Type);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(
            [largest, -largest, step],
            table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"]).Select(row => (decimal)row["Amt"]));
    }

    /// <summary>
    /// A precision or scale that no format accepts throws the same
    /// <see cref="ArgumentOutOfRangeException"/> on Jet3 as on Jet4.
    /// </summary>
    /// <param name="precision">The declared precision.</param>
    /// <param name="scale">The declared scale.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(29, 0)]
    [InlineData(5, 6)]
    public async Task CreateTable_DecimalInvalidPrecisionOrScale_ThrowsArgumentOutOfRange(int precision, int scale)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb })
        {
            await using MemoryStream ms = await CreateDatabaseAsync(format, ct);
            await using AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct);
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await writer.CreateTableAsync(
                    TableName,
                    [new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = (byte)precision, NumericScale = (byte)scale }],
                    ct));
        }
    }

    /// <summary>
    /// <c>AddColumnAsync</c> adds a decimal column as Currency in each write
    /// mode, and an update stores its fractions.
    /// </summary>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task AddColumn_Decimal_IsStoredAsCurrency(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateTableWithRowsAsync(ct);

        await WriteInModeAsync(
            ms,
            mode,
            async writer =>
            {
                await writer.AddColumnAsync(TableName, new ColumnDefinition("Fee", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 }, ct);
                Assert.Equal(1, await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Fee"] = 0.05m }, ct));
            },
            ct);

        Assert.Equal(ColumnType.MoneyType, (await ReadColumnDescriptorsAsync(ms, ct))[2].Type);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal("Currency", Assert.Single(await reader.GetColumnMetadataAsync(TableName, ct), c => c.Name == "Fee").TypeName);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        DataRow[] rows = [.. table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"])];
        Assert.Equal(0.05m, rows[0]["Fee"]);
        Assert.Equal(DBNull.Value, rows[1]["Fee"]);
        Assert.Equal([12.34m, 0.75m], rows.Select(row => (decimal)row["Amt"]));
    }

    /// <summary>
    /// <c>AddColumnAsync</c> refuses a decimal column Currency cannot hold
    /// before it reads or writes anything, with and without
    /// <see cref="AccessWriterOptions.UseTransactionalWrites"/>.
    /// </summary>
    /// <param name="useTransactionalWrites">Whether the writer journals each operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AddColumn_DecimalCurrencyCannotHold_ThrowsAndLeavesTableUnchanged(bool useTransactionalWrites)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateTableWithRowsAsync(ct);
        byte[] before = ms.ToArray();

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = useTransactionalWrites }, ct))
        {
            foreach (ColumnDefinition column in new[]
            {
                new ColumnDefinition("Fee", typeof(decimal)) { NumericPrecision = 10, NumericScale = 5 },
                new ColumnDefinition("Fee", typeof(decimal)) { NumericPrecision = 15 },
                new ColumnDefinition("Fee", typeof(decimal)),
            })
            {
                NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                    await writer.AddColumnAsync(TableName, column, ct));
                Assert.Contains("'Fee'", ex.Message, StringComparison.Ordinal);
            }
        }

        Assert.Equal(before, ms.ToArray());
    }

    /// <summary>
    /// A Jet3 decimal column keeps its Currency type and values through a rename.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task RenameColumn_Jet3Decimal_KeepsCurrencyAndValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateTableWithRowsAsync(ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.RenameColumnAsync(TableName, "Amt", "Amount", ct);
            await writer.InsertRowAsync(TableName, [3, 1.5m], ct);
        }

        Assert.Equal(ColumnType.MoneyType, (await ReadColumnDescriptorsAsync(ms, ct))[1].Type);
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Equal("Currency", Assert.Single(await reader.GetColumnMetadataAsync(TableName, ct), c => c.Name == "Amount").TypeName);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal([12.34m, 0.75m, 1.5m], table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"]).Select(row => (decimal)row["Amount"]));
    }

    /// <summary>
    /// Currency keeps four decimal places, so a fifth is rounded half to even.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task Insert_MoreThanFourDecimalPlaces_RoundsHalfToEvenAtFourPlaces()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateTableWithRowsAsync(ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.InsertRowAsync(TableName, [3, 12.34565m], ct);
            await writer.InsertRowAsync(TableName, [4, 12.34575m], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(
            [12.34m, 0.75m, 12.3456m, 12.3458m],
            table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"]).Select(row => (decimal)row["Amt"]));
    }

    /// <summary>
    /// A value outside the Currency range, which is also outside every
    /// declaration Jet3 accepts, throws <see cref="OverflowException"/>, and a
    /// batch holding one writes no row.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task Insert_ValueOutsideCurrencyRange_ThrowsAndWritesNothing()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 14 }],
                ct);

            await Assert.ThrowsAsync<OverflowException>(async () =>
                await writer.InsertRowAsync(TableName, [1, 999_999_999_999_999m], ct));
            await Assert.ThrowsAsync<OverflowException>(async () =>
                await writer.InsertRowsAsync(TableName, [[1, 1m], [2, -999_999_999_999_999m]], ct));
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        Assert.Empty(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct));
    }

    /// <summary>
    /// An update to a value outside the Currency range throws the same
    /// <see cref="OverflowException"/> before it changes anything, in each
    /// write mode. The update rewrites the row by deleting it and inserting
    /// the new version, and it used to encode the new version only after the
    /// delete, so without a transaction the row was lost; an explicit
    /// transaction committed after the failure wrote the delete.
    /// </summary>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task Update_ValueOutsideCurrencyRange_ThrowsAndKeepsTheRow(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);
        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 14 }],
                ct);
            await writer.InsertRowAsync(TableName, [1, 12m], ct);
        }

        byte[] before = ms.ToArray();
        await WriteInModeAsync(
            ms,
            mode,
            async writer => await Assert.ThrowsAsync<OverflowException>(async () =>
                await writer.UpdateRowsAsync(TableName, "Id", 1, new Dictionary<string, object?> { ["Amt"] = 999_999_999_999_999m }, ct)),
            ct);

        Assert.Equal(before, ms.ToArray());
        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        object[] expected = [1, 12m];
        Assert.Equal(expected, Assert.Single(await reader.Rows(TableName, cancellationToken: ct).ToListAsync(ct)));
        Assert.Equal(1, Assert.Single(await reader.GetTableStatsAsync(ct), stat => stat.Name == TableName).RowCount);
    }

    /// <summary>
    /// A unique index on a Jet3 decimal column rejects a duplicate value in
    /// each write mode.
    /// </summary>
    /// <param name="mode">The <see cref="WriteMode"/>.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [MemberData(nameof(WriteModes))]
    public async Task UniqueIndex_OnJet3DecimalColumn_RejectsDuplicate(string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);

        await WriteInModeAsync(
            ms,
            mode,
            async writer =>
            {
                await writer.CreateTableAsync(
                    TableName,
                    [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 }],
                    [new IndexDefinition("UX_Amt", "Amt") { IsUnique = true }],
                    ct);
                await writer.InsertRowAsync(TableName, [1, 12.34m], ct);
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.InsertRowAsync(TableName, [2, 12.34m], ct));
                await writer.InsertRowAsync(TableName, [3, 12.35m], ct);
            },
            ct);

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal([12.34m, 12.35m], table.Rows.Cast<DataRow>().OrderBy(row => (int)row["Id"]).Select(row => (decimal)row["Amt"]));
    }

    /// <summary>
    /// A Jet3 Numeric column written by an earlier build (precision and scale
    /// bytes zero) is left as it is: schema rewrites keep it Numeric, and its
    /// values are unchanged.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task LegacyJet3NumericColumn_SurvivesSchemaRewrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            // The internal override reproduces what earlier builds wrote for a decimal column.
            await writer.CreateTableAsync(
                TableName,
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Legacy", typeof(decimal)) { ColumnTypeOverride = ColumnType.NumericType }],
                ct);
            await writer.InsertRowAsync(TableName, [1, 7m], ct);
            await writer.AddColumnAsync(TableName, new ColumnDefinition("Note", typeof(string), maxLength: 20), ct);
            await writer.RenameColumnAsync(TableName, "Legacy", "Old", ct);
        }

        ColumnInfo legacy = (await ReadColumnDescriptorsAsync(ms, ct))[1];
        Assert.Equal(ColumnType.NumericType, legacy.Type);
        Assert.Equal("Old", legacy.Name);

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        Assert.Equal(7m, Assert.Single(table.Rows.Cast<DataRow>())["Old"]);
    }

    /// <summary>
    /// Jet4 and ACCDB have a Decimal type, so the Currency mapping does not
    /// apply there.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateTable_Decimal_OnJet4AndAccdb_StaysDecimal(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream ms = await CreateDatabaseAsync(format, ct);

        await using (AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, ct))
        {
            await writer.CreateTableAsync(
                TableName,
                [
                    new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 },
                    new ColumnDefinition("Plain", typeof(decimal)),
                ],
                ct);
            await writer.InsertRowAsync(TableName, [12.34m, 123_456_789_012_345_678m], ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms, ct);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(TableName, ct);
        Assert.All(metadata, column => Assert.Equal("Decimal", column.TypeName));
        Assert.All(metadata, column => Assert.False(column.IsCurrency));
        Assert.Equal(10, metadata[0].NumericPrecision);
        Assert.Equal(2, metadata[0].NumericScale);
        Assert.Equal(18, metadata[1].NumericPrecision);
        Assert.Equal(0, metadata[1].NumericScale);
        using DataTable table = await reader.ReadDataTableAsync(TableName, cancellationToken: ct);
        DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
        Assert.Equal(12.34m, row["Amt"]);
        Assert.Equal(123_456_789_012_345_678m, row["Plain"]);
    }

    private static async Task<MemoryStream> CreateTableWithRowsAsync(CancellationToken cancellationToken)
    {
        MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.Jet3Mdb, cancellationToken);
        await using AccessWriter writer = await OpenWriterAsync(ms, new AccessWriterOptions { UseLockFile = false }, cancellationToken);
        await writer.CreateTableAsync(
            TableName,
            [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Amt", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 }],
            cancellationToken);
        await writer.InsertRowAsync(TableName, [1, 12.34m], cancellationToken);
        await writer.InsertRowAsync(TableName, [2, 0.75m], cancellationToken);
        return ms;
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

    private static async Task WriteInModeAsync(MemoryStream ms, string mode, Func<AccessWriter, Task> work, CancellationToken cancellationToken)
    {
        WriteMode writeMode = Enum.Parse<WriteMode>(mode);
        var options = new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = writeMode == WriteMode.TransactionalWrites };
        await using AccessWriter writer = await OpenWriterAsync(ms, options, cancellationToken);
        if (writeMode == WriteMode.ExplicitTransaction)
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(cancellationToken);
            await work(writer);
            await transaction.CommitAsync(cancellationToken);
        }
        else
        {
            await work(writer);
        }
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream ms, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessWriter.OpenAsync(ms, options, leaveOpen: true, cancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms, CancellationToken cancellationToken)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }
}
