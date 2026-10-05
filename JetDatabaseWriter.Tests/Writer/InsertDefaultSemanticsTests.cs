namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1812 // POCOs are instantiated by the tests and the row mapper.

/// <summary>
/// How an insert treats a column that has a default, as an Access SQL <c>INSERT</c>
/// does. A value the insert supplies is stored as given, so <see langword="null"/> and
/// <see cref="DBNull.Value"/> store NULL even when the column has a default, and a NOT
/// NULL column rejects them. A column the insert leaves out (a <see cref="RowValues"/>
/// key it does not name, a column no POCO property maps to) or sets to
/// <see cref="DbDefault.Value"/> gets its default. An AutoNumber column generates its
/// next value and a calculated column is computed in every case. Each case runs in the
/// writer that declared the default and in a later writer, which reads it from the file.
/// </summary>
public sealed class InsertDefaultSemanticsTests
{
    private const string Table = "Defaults";

    /// <summary>Gets each shape of an explicit null, in every format, with a persisted or a CLR default.</summary>
    public static TheoryData<DatabaseFormat, string, bool> ExplicitNullCases =>
        Cases("PositionalNull", "PositionalDbNull", "RowsNull", "RowValuesNull", "RowValuesDbNull", "PocoNull");

    /// <summary>Gets each shape of a column left out or set to <see cref="DbDefault.Value"/>, in every format, with a persisted or a CLR default.</summary>
    public static TheoryData<DatabaseFormat, string, bool> DefaultRequestCases =>
        Cases("RowValuesOmitted", "PocoWithoutProperty", "PocoNotMapped", "PositionalDbDefault", "RowValuesDbDefault", "RowsDbDefault");

    /// <summary>Gets every format.</summary>
    public static TheoryData<DatabaseFormat> FormatCases => [.. Formats];

    /// <summary>Gets every format with each write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, bool> WriteModeCases
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, WriteMode, bool>();
            foreach (DatabaseFormat format in Formats)
            {
                foreach ((WriteMode mode, bool rollback) in new[] { (WriteMode.Direct, false), (WriteMode.AutoCommit, false), (WriteMode.ExplicitCommit, false), (WriteMode.ExplicitCommit, true) })
                {
                    data.Add(format, mode, rollback);
                }
            }

            return data;
        }
    }

    private static DatabaseFormat[] Formats => [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    /// <summary>
    /// An explicit null stores NULL in a column with a default. It used to store the
    /// default, so no insert could store NULL in such a column.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="shape">How the insert supplies the null; see <see cref="InsertAsync"/>.</param>
    /// <param name="clrDefault">Whether the default is a CLR <see cref="ColumnDefinition.DefaultValue"/> rather than a <see cref="ColumnDefinition.DefaultValueExpression"/>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(ExplicitNullCases))]
    public async Task ExplicitNull_InColumnWithDefault_StoresNull(DatabaseFormat format, string shape, bool clrDefault)
    {
        await using MemoryStream stream = await CreateTableAsync(format, ScoreColumns(clrDefault, isNullable: true), insert: writer => InsertAsync(writer, shape, 1));

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await InsertAsync(writer, shape, 2);
        }

        Dictionary<int, object> scores = await ReadScoresAsync(stream);
        Assert.Equal(DBNull.Value, scores[1]);
        Assert.Equal(DBNull.Value, scores[2]);
    }

    /// <summary>
    /// A column the insert leaves out, or sets to <see cref="DbDefault.Value"/>, gets its
    /// default in the declaring and a later writer.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="shape">How the insert leaves the column out; see <see cref="InsertAsync"/>.</param>
    /// <param name="clrDefault">Whether the default is a CLR <see cref="ColumnDefinition.DefaultValue"/> rather than a <see cref="ColumnDefinition.DefaultValueExpression"/>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(DefaultRequestCases))]
    public async Task OmittedColumn_GetsDefault(DatabaseFormat format, string shape, bool clrDefault)
    {
        await using MemoryStream stream = await CreateTableAsync(format, ScoreColumns(clrDefault, isNullable: true), insert: writer => InsertAsync(writer, shape, 1));

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await InsertAsync(writer, shape, 2);
        }

        Dictionary<int, object> scores = await ReadScoresAsync(stream);
        Assert.Equal(42, scores[1]);
        Assert.Equal(42, scores[2]);
    }

    /// <summary>
    /// A NOT NULL column with a default rejects an explicit null, as Access does
    /// (error 3314), instead of storing the default; <see cref="DbDefault.Value"/> and an
    /// omitted column still store the default.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task ExplicitNull_InNotNullColumnWithDefault_IsRejected(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, ScoreColumns(clrDefault: false, isNullable: false), insert: null);

        for (int session = 0; session < 2; session++)
        {
            await using AccessWriter writer = await OpenWriterAsync(stream);
            JetConstraintException ex = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowAsync(Table, [10 + session, null], ct));
            Assert.Contains("cannot be set to null", ex.Message, StringComparison.Ordinal);
            await Assert.ThrowsAsync<JetConstraintException>(async () =>
                await writer.InsertRowAsync(Table, new RowValues { ["Id"] = 20 + session, ["Score"] = DBNull.Value }, ct));

            await writer.InsertRowAsync(Table, [1 + (2 * session), DbDefault.Value], ct);
            await writer.InsertRowAsync(Table, new RowValues { ["Id"] = 2 + (2 * session) }, ct);
        }

        Dictionary<int, object> scores = await ReadScoresAsync(stream);
        Assert.Equal([1, 2, 3, 4], scores.Keys.Order());
        Assert.All(scores.Values, score => Assert.Equal(42, score));
    }

    /// <summary>
    /// An AutoNumber column generates its next value for null, <see cref="DBNull.Value"/>,
    /// <see cref="DbDefault.Value"/>, an omitted <see cref="RowValues"/> key and a POCO with no
    /// property for it, as appending Null to an AutoNumber does in Access.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task AutoNumber_NullDbNullDbDefaultAndOmitted_AllGenerate(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Auto",
                [new("Id", typeof(int)) { IsAutoIncrement = true }, new("Name", typeof(string), maxLength: 20)],
                ct);
            await writer.InsertRowAsync("Auto", [null, "a"], ct);
            await writer.InsertRowAsync("Auto", [DBNull.Value, "b"], ct);
            await writer.InsertRowAsync("Auto", [DbDefault.Value, "c"], ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("Auto", new RowValues { ["Name"] = "d" }, ct);
            await writer.InsertRowAsync("Auto", new NameOnlyRow { Name = "e" }, ct);
            await writer.InsertRowAsync("Auto", new RowValues { ["Id"] = DbDefault.Value, ["Name"] = "f" }, ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("Auto", cancellationToken: ct);
        Assert.Equal(
            ["1|a", "2|b", "3|c", "4|d", "5|e", "6|f"],
            dt.AsEnumerable().Select(row => $"{row["Id"]}|{row["Name"]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// A calculated column is computed for <see cref="DbDefault.Value"/>, as for null and
    /// an omitted column.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task CalculatedColumn_DbDefault_IsComputed()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Calc",
                [new("Score", typeof(int)), new("Doubled", typeof(int)) { IsCalculated = true, CalculationExpression = "[Score] * 2" }],
                ct);
            await writer.InsertRowAsync("Calc", [3, DbDefault.Value], ct);
            await writer.InsertRowAsync("Calc", [4, null], ct);
            await writer.InsertRowAsync("Calc", new RowValues { ["Score"] = 5 }, ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("Calc", cancellationToken: ct);
        Assert.Equal(["3|6", "4|8", "5|10"], dt.AsEnumerable().Select(row => $"{row["Score"]}|{row["Doubled"]}").Order(StringComparer.Ordinal));
    }

    /// <summary>
    /// An update never applies a default, so both <c>UpdateRowsAsync</c> overloads reject
    /// <see cref="DbDefault.Value"/> before any row changes.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task Update_DbDefault_IsRejected(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(
            format,
            ScoreColumns(clrDefault: false, isNullable: true),
            insert: writer => writer.InsertRowAsync(Table, [1, 5], ct).AsTask());

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            ArgumentException criteriaEx = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(Table, RowCriteria.All(), new RowValues { ["Score"] = DbDefault.Value }, ct));
            Assert.Equal("updatedValues", criteriaEx.ParamName);
            Assert.Contains("'Score'", criteriaEx.Message, StringComparison.Ordinal);

            ArgumentException dictionaryEx = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(Table, "Id", 1, new Dictionary<string, object?> { ["Score"] = DbDefault.Value }, ct));
            Assert.Equal("updatedValues", dictionaryEx.ParamName);
        }

        Dictionary<int, object> scores = await ReadScoresAsync(stream);
        Assert.Equal(5, Assert.Single(scores).Value);
    }

    /// <summary>
    /// An explicit null and <see cref="DbDefault.Value"/> behave the same with no
    /// transaction, with <see cref="AccessWriterOptions.UseTransactionalWrites"/>, and in an
    /// explicit transaction that commits; a rolled-back transaction writes neither row.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">The shared write mode.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    /// <param name="rollback">Whether to roll back the explicit transaction.</param>
    [Theory]
    [MemberData(nameof(WriteModeCases))]
    public async Task ExplicitNullAndDefault_InEveryWriteMode(DatabaseFormat format, WriteMode mode, bool rollback)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, ScoreColumns(clrDefault: false, isNullable: true), insert: null);

        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = mode == WriteMode.AutoCommit }))
        {
            JetTransaction? transaction = mode == WriteMode.ExplicitCommit ? await writer.BeginTransactionAsync(ct) : null;
            await writer.InsertRowAsync(Table, [1, null], ct);
            await writer.InsertRowAsync(Table, [2, DbDefault.Value], ct);
            _ = await writer.InsertRowsAsync(Table, new List<RowValues> { new() { ["Id"] = 3, ["Score"] = null }, new() { ["Id"] = 4 } }, ct);
            if (transaction is not null)
            {
                if (!rollback)
                {
                    await transaction.CommitAsync(ct);
                }
                else
                {
                    await transaction.RollbackAsync(ct);
                }

                await transaction.DisposeAsync();
            }
        }

        Dictionary<int, object> scores = await ReadScoresAsync(stream);
        if (rollback)
        {
            Assert.Empty(scores);
            return;
        }

        Assert.Equal(DBNull.Value, scores[1]);
        Assert.Equal(42, scores[2]);
        Assert.Equal(DBNull.Value, scores[3]);
        Assert.Equal(42, scores[4]);
    }

    /// <summary>
    /// Older versions of Access gave every Number column a default of <c>0</c>. In those
    /// Access-authored tables an explicit null now stores NULL, as Access SQL does, and a
    /// Number column the insert leaves out still stores 0. Jackcess's test.mdb (Access 97)
    /// and test.accdb (Access 2007) give Table1's C, D, E, F and H that default.
    /// </summary>
    /// <param name="fixture"><c>TestV1997</c> or <c>TestV2007</c>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [InlineData("TestV1997")]
    [InlineData("TestV2007")]
    public async Task AccessAuthoredNumberDefault_ExplicitNullStoresNull_OmittedStoresZero(string fixture)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CopyFixtureAsync(fixture == "TestV1997" ? TestDatabases.TestV1997 : TestDatabases.TestV2007);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("Table1", new RowValues { ["A"] = "probe", ["C"] = DBNull.Value, ["F"] = null }, ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("Table1", cancellationToken: ct);
        DataRow probe = Assert.Single(dt.AsEnumerable(), row => Equals(row["A"], "probe"));
        Assert.Equal(DBNull.Value, probe["C"]);
        Assert.Equal(DBNull.Value, probe["F"]);
        Assert.Equal(0, Convert.ToInt32(probe["D"], System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(probe["E"], System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(probe["H"], System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The mdbtools Northwind sample (Jet4): <c>Products.UnitsInStock</c> has the default
    /// <c>0</c>. An explicit null stores NULL; the omitted <c>UnitsOnOrder</c> and
    /// <c>Discontinued</c> still store their defaults.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task AccessAuthoredNumberDefault_Jet4_ExplicitNullStoresNull()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.MdbtoolsNwind);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("Products", new RowValues { ["ProductName"] = "probe", ["UnitsInStock"] = null }, ct);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable products = await reader.ReadDataTableAsync("Products", cancellationToken: ct);
        DataRow probe = Assert.Single(products.AsEnumerable(), row => Equals(row["ProductName"], "probe"));
        Assert.Equal(DBNull.Value, probe["UnitsInStock"]);
        Assert.Equal((short)0, probe["UnitsOnOrder"]);
        Assert.Equal(false, probe["Discontinued"]);
    }

    private static TheoryData<DatabaseFormat, string, bool> Cases(params string[] shapes)
    {
        var data = new TheoryData<DatabaseFormat, string, bool>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (string shape in shapes)
            {
                data.Add(format, shape, false);
                data.Add(format, shape, true);
            }
        }

        return data;
    }

    private static ColumnDefinition[] ScoreColumns(bool clrDefault, bool isNullable) =>
    [
        new("Id", typeof(int)),
        clrDefault
            ? new("Score", typeof(int)) { DefaultValue = 42, IsNullable = isNullable }
            : new("Score", typeof(int)) { DefaultValueExpression = "42", IsNullable = isNullable },
    ];

    /// <summary>
    /// Inserts row <paramref name="id"/> into the <c>Id</c>, <c>Score</c> table, supplying
    /// <c>Score</c> the way <paramref name="shape"/> names.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="shape">
    /// An explicit null: <c>PositionalNull</c>, <c>PositionalDbNull</c>, <c>RowsNull</c>
    /// (<c>InsertRowsAsync</c>), <c>RowValuesNull</c>, <c>RowValuesDbNull</c> or <c>PocoNull</c>.
    /// The column left out: <c>RowValuesOmitted</c>, <c>PocoWithoutProperty</c>,
    /// <c>PocoNotMapped</c>, <c>PositionalDbDefault</c>, <c>RowValuesDbDefault</c> or
    /// <c>RowsDbDefault</c>.
    /// </param>
    /// <param name="id">The row's Id.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous insert.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="shape"/> is none of these.</exception>
    private static async Task InsertAsync(AccessWriter writer, string shape, int id)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        switch (shape)
        {
            case "PositionalNull":
                await writer.InsertRowAsync(Table, [id, null], ct);
                break;
            case "PositionalDbNull":
                await writer.InsertRowAsync(Table, [id, DBNull.Value], ct);
                break;
            case "RowsNull":
                _ = await writer.InsertRowsAsync(Table, Rows([id, null]), ct);
                break;
            case "RowValuesNull":
                await writer.InsertRowAsync(Table, new RowValues { ["Id"] = id, ["Score"] = null }, ct);
                break;
            case "RowValuesDbNull":
                await writer.InsertRowAsync(Table, new RowValues { ["Id"] = id, ["Score"] = DBNull.Value }, ct);
                break;
            case "PocoNull":
                await writer.InsertRowAsync(Table, new ScoreRow { Id = id, Score = null }, ct);
                break;
            case "RowValuesOmitted":
                await writer.InsertRowAsync(Table, new RowValues { ["Id"] = id }, ct);
                break;
            case "PocoWithoutProperty":
                await writer.InsertRowAsync(Table, new IdOnlyRow { Id = id }, ct);
                break;
            case "PocoNotMapped":
                await writer.InsertRowAsync(Table, new NotMappedScoreRow { Id = id, Score = 7 }, ct);
                break;
            case "PositionalDbDefault":
                await writer.InsertRowAsync(Table, [id, DbDefault.Value], ct);
                break;
            case "RowValuesDbDefault":
                await writer.InsertRowAsync(Table, new RowValues { ["Id"] = id, ["Score"] = DbDefault.Value }, ct);
                break;
            case "RowsDbDefault":
                _ = await writer.InsertRowsAsync(Table, Rows([id, DbDefault.Value]), ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(shape), shape, null);
        }
    }

    private static List<object?[]> Rows(object?[] row) => [row];

    private static async Task<MemoryStream> CreateTableAsync(DatabaseFormat format, ColumnDefinition[] columns, Func<AccessWriter, Task>? insert)
    {
        MemoryStream stream = await CreateFreshStreamAsync(format);
        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(Table, columns, TestContext.Current.CancellationToken);
        if (insert is not null)
        {
            await insert(writer);
        }

        return stream;
    }

    private static async Task<Dictionary<int, object>> ReadScoresAsync(MemoryStream stream)
    {
        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(Table, cancellationToken: TestContext.Current.CancellationToken);
        return dt.AsEnumerable().ToDictionary(row => (int)row["Id"], row => row["Score"]);
    }

    private static async Task<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async Task<MemoryStream> CopyFixtureAsync(string path)
    {
        Assert.True(File.Exists(path), $"Fixture not found: {path}");
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            options ?? new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private sealed class ScoreRow
    {
        public int Id { get; set; }

        public int? Score { get; set; }
    }

    private sealed class IdOnlyRow
    {
        public int Id { get; set; }
    }

    private sealed class NotMappedScoreRow
    {
        public int Id { get; set; }

        [NotMapped]
        public int? Score { get; set; }
    }

    private sealed class NameOnlyRow
    {
        public string? Name { get; set; }
    }
}
