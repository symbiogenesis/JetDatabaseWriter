namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for column-level constraints: DefaultValue, IsNullable, and
/// ValidationRule behaviour during insert and across writer reopens.
/// Tests run against both Jet3 and ACE formats via <c>[Theory]</c> parameters.
/// </summary>
public sealed class ColumnConstraintTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DefaultValue_IsAppliedOnInsert(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Defaults";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValue = 42 },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 7], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(42, dt.Rows[0]["Score"]);
        Assert.Equal(7, dt.Rows[1]["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NullablePublicValueShapes_TreatNullAsDatabaseNull(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "NullableShapes";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                    new("Score", typeof(int)) { DefaultValue = 42 },
                ],
                TestContext.Current.CancellationToken);

            object?[] single = [1, null, null];
            await writer.InsertRowAsync(table, single, TestContext.Current.CancellationToken);

            List<object?[]> rows =
            [
                [2, "Bob", 12],
                [3, null, 7],
            ];
            await writer.InsertRowsAsync(table, rows, TestContext.Current.CancellationToken);

            IReadOnlyDictionary<string, object?> updates = new Dictionary<string, object?>
            {
                ["Name"] = null,
                ["Score"] = null,
            };
            int updated = await writer.UpdateRowsAsync(table, "Id", 2, updates, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, dt.Rows.Count);

        DataRow first = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 1);
        DataRow second = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 2);
        DataRow third = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 3);

        Assert.Equal(DBNull.Value, first["Name"]);
        Assert.Equal(42, first["Score"]);

        Assert.Equal(DBNull.Value, second["Name"]);
        Assert.Equal(DBNull.Value, second["Score"]);

        Assert.Equal(DBNull.Value, third["Name"]);
        Assert.Equal(7, third["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NotNull_RejectsMissingValue(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Required";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [
                new("Id", typeof(int)),
                new("Name", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

        await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task NotNull_PersistsAcrossWriterReopen(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Required";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

            await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
        Assert.True(meta[0].IsNullable);
        Assert.False(meta[1].IsNullable);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ValidationRule_RejectsBadValues(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Validated";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [
                new("Id", typeof(int)),
                new("Score", typeof(int)) { ValidationRule = v => v is int i && i is >= 0 and <= 100 },
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(table, [2, 250], TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Update_NullIntoNotNullColumn_IsRejected(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "RequiredUpdate";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [1, "x"], TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Name"] = null }, TestContext.Current.CancellationToken));
        }

        // A later writer hydrates NOT NULL from the persisted Required property.
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Name"] = DBNull.Value }, TestContext.Current.CancellationToken));

            Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Name"] = "y" }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(dt.AsEnumerable());
        Assert.Equal("y", row["Name"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Update_NullIntoAutoNumberColumn_IsRejected(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "AutoUpdate";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true },
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [DBNull.Value, "a"], TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(table, "Name", "a", new Dictionary<string, object?> { ["Id"] = null }, TestContext.Current.CancellationToken));
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(table, "Name", "a", new Dictionary<string, object?> { ["Id"] = null }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(dt.AsEnumerable());
        Assert.Equal(1, row["Id"]);
    }

    /// <summary>
    /// Like an insert, an update that assigns an explicit AutoNumber value raises
    /// the TDEF high-water so Access never issues that number again.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Update_ExplicitAutoNumberValue_RaisesTdefHighWater(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "AutoHighWater";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true },
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(table, [[DBNull.Value, "a"], [DBNull.Value, "b"], [DBNull.Value, "c"]], TestContext.Current.CancellationToken);
        }

        Assert.Equal(3u, await ReadTdefAutoNumberAsync(stream, table));

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync(table, "Name", "c", new Dictionary<string, object?> { ["Id"] = 50 }, TestContext.Current.CancellationToken));
        }

        Assert.Equal(50u, await ReadTdefAutoNumberAsync(stream, table));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task Update_ValidationRule_RejectsAssignedValue(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "ValidatedUpdate";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { ValidationRule = v => v is int i && i is >= 0 and <= 100 },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Score"] = 250 }, TestContext.Current.CancellationToken));

            Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Score"] = 75 }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(dt.AsEnumerable());
        Assert.Equal(75, row["Score"]);
    }

    /// <summary>
    /// The update constraint pass runs while rows are staged, before any page is
    /// written, so a rejection leaves every matching row untouched, including rows
    /// on later data pages, with or without a transaction around the call.
    /// </summary>
    /// <param name="useTransactionalWrites">Whether the writer wraps each call in an auto-commit transaction.</param>
    /// <param name="explicitTransaction">"none", "commit" or "rollback".</param>
    [Theory]
    [InlineData(false, "none")]
    [InlineData(true, "none")]
    [InlineData(false, "commit")]
    [InlineData(false, "rollback")]
    public async Task Update_NullIntoNotNullColumn_LeavesEveryRowUnchanged(bool useTransactionalWrites, string explicitTransaction)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "RequiredMultiPage";
        const int rowCount = 300;
        string padding = new('p', 200);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 255) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(
                table,
                Enumerable.Range(1, rowCount).Select(i => new object?[] { i, padding + i }),
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = useTransactionalWrites }))
        {
            JetTransaction? transaction = explicitTransaction == "none"
                ? null
                : await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.UpdateRowsAsync(table, RowCriteria.All(), new RowValues { ["Name"] = null }, TestContext.Current.CancellationToken));

            if (transaction is not null)
            {
                if (explicitTransaction == "commit")
                {
                    await transaction.CommitAsync(TestContext.Current.CancellationToken);
                }
                else
                {
                    await transaction.RollbackAsync(TestContext.Current.CancellationToken);
                }

                await transaction.DisposeAsync();
            }
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(rowCount, dt.Rows.Count);
        Assert.All(dt.AsEnumerable(), row => Assert.Equal(padding + (int)row["Id"], row["Name"]));
    }

    /// <summary>
    /// Update checks only the columns it assigns: a row that already holds null in a
    /// NOT NULL column (here, one added after the row was written) can still have
    /// its other columns updated.
    /// </summary>
    [Fact]
    public async Task Update_OtherColumn_DoesNotRecheckUnassignedNotNullColumn()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "LegacyNulls";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [1, "a"], TestContext.Current.CancellationToken);
            await writer.AddColumnAsync(table, new ColumnDefinition("Code", typeof(string), 10) { IsNullable = false }, TestContext.Current.CancellationToken);

            Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Name"] = "b" }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(dt.AsEnumerable());
        Assert.Equal("b", row["Name"]);
        Assert.Equal(DBNull.Value, row["Code"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ValidationRuleExpression_IsEnforcedOnInsertAndUpdate_InEveryWriter(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "RuleExpr";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { ValidationRuleExpression = ">=0 And <=100", ValidationText = "Score must be 0..100." },
                ],
                TestContext.Current.CancellationToken);

            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync(table, [1, -5], TestContext.Current.CancellationToken));
            Assert.Contains("Score must be 0..100.", ex.Message, StringComparison.Ordinal);

            await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);

            // Like Access, a rule that does not test for Null lets Null through.
            await writer.InsertRowAsync(table, [2, DBNull.Value], TestContext.Current.CancellationToken);
        }

        // A later writer reads the rule from MSysObjects.LvProp.
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync(table, [3, 500], TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Score"] = 101 }, TestContext.Current.CancellationToken));

            Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Score"] = 99 }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(99, Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 1)["Score"]);
        Assert.Equal(DBNull.Value, Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 2)["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DefaultValueExpression_IsAppliedOnInsert_InEveryWriter(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "DefaultExpr";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValueExpression = "7" },
                    new("Label", typeof(string), maxLength: 20) { DefaultValueExpression = "\"none\"" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, DBNull.Value, null], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 3, "given"], TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, new RowValues { ["Id"] = 3 }, TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, dt.Rows.Count);
        DataRow first = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 1);
        DataRow second = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 2);
        DataRow third = Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 3);
        Assert.Equal(7, first["Score"]);
        Assert.Equal("none", first["Label"]);
        Assert.Equal(3, second["Score"]);
        Assert.Equal("given", second["Label"]);
        Assert.Equal(7, third["Score"]);
        Assert.Equal("none", third["Label"]);
    }

    /// <summary>
    /// A CLR <see cref="ColumnDefinition.DefaultValue"/> is persisted as a literal
    /// <c>DefaultValue</c> expression, so a writer that did not declare it still
    /// applies it.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DefaultValue_IsAppliedByALaterWriter(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "ClrDefault";
        var when = new DateTime(2024, 2, 29, 8, 30, 0);

        List<ColumnDefinition> columns =
        [
            new("Id", typeof(int)),
            new("Score", typeof(int)) { DefaultValue = 7 },
            new("Name", typeof(string), maxLength: 20) { DefaultValue = "a \"b\"" },
        ];

        // Jet3 has no Numeric type, and the Jet3 row encoder cannot yet write an
        // MSysObjects row whose LvProp blob pushes the variable area past 255 bytes.
        if (format != DatabaseFormat.Jet3Mdb)
        {
            columns.Add(new("Flag", typeof(bool)) { DefaultValue = true });
            columns.Add(new("When", typeof(DateTime)) { DefaultValue = when });
            columns.Add(new("Amount", typeof(decimal)) { DefaultValue = 12.34m, NumericPrecision = 10, NumericScale = 2 });
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(table, columns, TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, new RowValues { ["Id"] = 1 }, TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(dt.AsEnumerable());
        Assert.Equal(7, row["Score"]);
        Assert.Equal("a \"b\"", row["Name"]);
        if (format != DatabaseFormat.Jet3Mdb)
        {
            Assert.Equal(true, row["Flag"]);
            Assert.Equal(when, row["When"]);
            Assert.Equal(12.34m, row["Amount"]);
        }
    }

    /// <summary>
    /// Persisted rules are hydrated and enforced inside an explicit transaction
    /// and under <see cref="AccessWriterOptions.UseTransactionalWrites"/>.
    /// </summary>
    /// <param name="useTransactionalWrites">Whether the writer wraps each call in an auto-commit transaction.</param>
    /// <param name="commit">Whether the explicit transaction commits or rolls back.</param>
    [Theory]
    [InlineData(false, true)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public async Task PersistedRules_AreEnforcedInsideTransactions(bool useTransactionalWrites, bool commit)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "RuleTx";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValueExpression = "7", ValidationRuleExpression = "Between 0 And 10" },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [1, 1], TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = useTransactionalWrites }))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync(table, [2, 11], TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["Score"] = -1 }, TestContext.Current.CancellationToken));
            await writer.InsertRowAsync(table, [3, DBNull.Value], TestContext.Current.CancellationToken);

            if (commit)
            {
                await transaction.CommitAsync(TestContext.Current.CancellationToken);
            }
            else
            {
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(1, Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 1)["Score"]);
        if (commit)
        {
            Assert.Equal(2, dt.Rows.Count);
            Assert.Equal(7, Assert.Single(dt.AsEnumerable(), row => (int)row["Id"] == 3)["Score"]);
        }
        else
        {
            Assert.Equal(1, dt.Rows.Count);
        }
    }

    /// <summary>
    /// Rules and defaults Microsoft Access wrote into the mdbtools Northwind sample
    /// (Jet4): <c>Products.UnitPrice</c> has default <c>0</c> and rule <c>&gt;=0</c>,
    /// <c>Products.Discontinued</c> has default <c>=No</c>, and
    /// <c>Order Details.Discount</c> has rule <c>Between 0 And 1</c>.
    /// </summary>
    [Fact]
    public async Task AccessAuthoredRulesAndDefaults_Jet4_AreEnforced()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.MdbtoolsNwind);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("Products", new RowValues { ["ProductName"] = "Default probe" }, TestContext.Current.CancellationToken);

            ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.InsertRowAsync("Products", new RowValues { ["ProductName"] = "Bad price", ["UnitPrice"] = -1m }, TestContext.Current.CancellationToken));
            Assert.Contains("You must enter a positive number.", ex.Message, StringComparison.Ordinal);

            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await writer.UpdateRowsAsync("Order Details", RowCriteria.All(), new RowValues { ["Discount"] = 2f }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable products = await reader.ReadDataTableAsync("Products", cancellationToken: TestContext.Current.CancellationToken);
        DataRow probe = Assert.Single(products.AsEnumerable(), row => Equals(row["ProductName"], "Default probe"));
        Assert.Equal(0m, probe["UnitPrice"]);
        Assert.Equal((short)0, probe["UnitsInStock"]);
        Assert.Equal(false, probe["Discontinued"]);
        Assert.DoesNotContain(products.AsEnumerable(), row => Equals(row["ProductName"], "Bad price"));
    }

    /// <summary>
    /// <c>OrderDetails.Quantity</c> in the Access-authored NorthwindTraders ACCDB
    /// carries the rule <c>&gt;0</c> with its own validation text.
    /// </summary>
    [Fact]
    public async Task AccessAuthoredRule_Accdb_RejectsUpdate()
    {
        await using MemoryStream stream = await CopyFixtureAsync(TestDatabases.NorthwindTraders);

        await using AccessWriter writer = await OpenWriterAsync(stream);
        ArgumentException ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.UpdateRowsAsync("OrderDetails", RowCriteria.All(), new RowValues { ["Quantity"] = 0 }, TestContext.Current.CancellationToken));
        Assert.Contains("Quantity should be greater than zero.", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A rule written with syntax or functions this library cannot evaluate is not
    /// enforced by the writer, rather than blocking every insert into the table.
    /// </summary>
    [Fact]
    public async Task ValidationRuleExpression_ThisLibraryCannotEvaluate_IsNotEnforced()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "RuleUnsupported";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { ValidationRuleExpression = "DLookUp(\"Id\",\"Other\") > 0" },
                ],
                TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, [1, -1], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(-1, Assert.Single(dt.AsEnumerable())["Score"]);
    }

    [Fact]
    public async Task ValidationRule_IsNotPersistedAcrossReopen()
    {
        // ValidationRule is a Func<> delegate — it cannot be serialized to the
        // database, so it binds only the writer that declared it. On reopen, no
        // validation fires for previously-guarded columns. A rule that must
        // survive reopen belongs in ValidationRuleExpression (see
        // ValidationRuleExpression_IsEnforcedOnInsertAndUpdate_InEveryWriter).
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "ValidNoPersist";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { ValidationRule = v => v is int i && i is >= 0 and <= 100 },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 50], TestContext.Current.CancellationToken);
        }

        // Reopen: the delegate-based rule is lost; out-of-range values are accepted.
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, [2, 250], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(250, dt.Rows[1]["Score"]);
    }

    /// <summary>
    /// Regression guard for the 2026-05-05 nullability fix: the writer must NOT
    /// stamp the legacy private 0x08 NOT-NULL bit (or any unknown bit) into the
    /// TDEF column-flags byte. DAO refuses to open tables whose column flags
    /// carry unknown bits with "Unrecognized database format". Nullability is
    /// now persisted via the Boolean <c>Required</c> property in
    /// <c>MSysObjects.LvProp</c> instead. Allowed bits: 0x01 (FIXED), 0x02
    /// (UNKNOWN_FF — always set on non-complex cols), 0x04 (AUTO_LONG),
    /// 0x07 (complex cols), 0x80 (HYPERLINK).
    /// </summary>
    [Fact]
    public async Task NotNull_DoesNotStampPrivate0x08BitInTdefColumnFlags()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "FlagGuard",
                [
                    new("Id", typeof(int)) { IsNullable = false },
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                    new("Optional", typeof(int)) { IsNullable = true },
                ],
                TestContext.Current.CancellationToken);
        }

        byte[] disk = stream.ToArray();
        const byte allowedFlagsMask = Constants.ColumnDescriptorFlags.Fixed
            | Constants.ColumnDescriptorFlags.Unknown
            | Constants.ColumnDescriptorFlags.AutoNumber
            | Constants.ColumnDescriptorFlags.Hyperlink;
        bool foundTable = false;

        for (int p = 1; p < disk.Length / Constants.PageSizes.Jet4; p++)
        {
            int off = p * Constants.PageSizes.Jet4;
            if (disk[off] != 0x02)
            {
                continue;
            }

            int numCols = disk[off + 45] | (disk[off + 46] << 8);
            if (numCols != 3)
            {
                continue;
            }

            int numRealIdx = disk[off + 51] | (disk[off + 52] << 8) | (disk[off + 53] << 16) | (disk[off + 54] << 24);
            int colStart = off + 63 + (numRealIdx * 12);

            for (int c = 0; c < numCols; c++)
            {
                int co = colStart + (c * 25);
                byte flags = disk[co + 15]; // descriptor-relative flags offset
                Assert.Equal(0, flags & ~allowedFlagsMask);
                Assert.Equal(0, flags & 0x08); // explicit guard against the removed NOT-NULL bit
            }

            foundTable = true;
        }

        Assert.True(foundTable, "Did not find the FlagGuard TDEF page in the writer-produced file.");
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
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

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            options ?? new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static async ValueTask<MemoryStream> CopyFixtureAsync(string path)
    {
        Assert.True(File.Exists(path), $"Fixture not found: {path}");
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }

    private static async ValueTask<uint> ReadTdefAutoNumberAsync(MemoryStream stream, string table)
    {
        long tdefPage;
        int autoNumberOffset;
        stream.Position = 0;
        await using (ReaderHarness pages = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            CatalogEntry? entry = await pages.GetCatalogEntryAsync(table, TestContext.Current.CancellationToken);
            Assert.NotNull(entry);
            tdefPage = entry.TDefPage;
            autoNumberOffset = pages.Database.TDef.AutoNumber;
        }

        // Jet4 and ACE pages are both 4 KB; the counter is a uint32 in the TDEF header.
        byte[] file = stream.ToArray();
        return BinaryPrimitives.ReadUInt32LittleEndian(
            file.AsSpan(checked((int)(tdefPage * Constants.PageSizes.Jet4)) + autoNumberOffset));
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
}
