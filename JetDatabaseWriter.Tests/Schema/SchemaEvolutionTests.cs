namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// End-to-end tests for schema evolution operations: AddColumnAsync,
/// DropColumnAsync, RenameColumnAsync, and related validation.
/// Tests run against both Jet3 and ACE formats via <c>[Theory]</c> parameters.
/// </summary>
public sealed class SchemaEvolutionTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task AddColumnAsync_AppendsColumn_ExistingRowsBecomeNull(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, "Alice"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, "Bob"], TestContext.Current.CancellationToken);

            await writer.AddColumnAsync(
                table,
                new ColumnDefinition("Score", typeof(int)),
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [3, "Carol", 99], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(3, dt.Columns.Count);
        Assert.Equal("Score", dt.Columns[2].ColumnName);
        Assert.Equal(3, dt.Rows.Count);

        Assert.Equal("Alice", dt.Rows[0]["Name"]);
        Assert.Equal(DBNull.Value, dt.Rows[0]["Score"]);
        Assert.Equal("Bob", dt.Rows[1]["Name"]);
        Assert.Equal(DBNull.Value, dt.Rows[1]["Score"]);
        Assert.Equal("Carol", dt.Rows[2]["Name"]);
        Assert.Equal(99, dt.Rows[2]["Score"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DropColumnAsync_RemovesColumn_AndPreservesOtherData(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                    new("Score", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, "Alice", 90], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, "Bob", 80], TestContext.Current.CancellationToken);

            await writer.DropColumnAsync(table, "Score", TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, dt.Columns.Count);
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal("Alice", dt.Rows[0]["Name"]);
        Assert.Equal("Bob", dt.Rows[1]["Name"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task RenameColumnAsync_RenamesColumn_AndPreservesAllData(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Contacts";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 95], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 88], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Score", "Rating", TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, dt.Columns.Count);
        Assert.True(dt.Columns.Contains("Rating"));
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(95, dt.Rows[0]["Rating"]);
        Assert.Equal(88, dt.Rows[1]["Rating"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task DropColumnAsync_LastColumn_Throws(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Solo";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Only", typeof(int))],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetOperationException>(async () =>
            await writer.DropColumnAsync(table, "Only", TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task AddColumnAsync_DuplicateName_Throws(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Dup";

        await using AccessWriter writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 20)],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetObjectExistsException>(async () =>
            await writer.AddColumnAsync(table, new ColumnDefinition("name", typeof(string), 20), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AddColumnAsync_PreservesMoneyDescriptorType()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "Ledger";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new ColumnDefinition("Amount", typeof(decimal)) { IsCurrency = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 12.3456m], TestContext.Current.CancellationToken);

            await writer.AddColumnAsync(
                table,
                new ColumnDefinition("Note", typeof(string), maxLength: 20),
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
        ColumnMetadata amount = Assert.Single(metadata, column => column.Name == "Amount");
        Assert.Equal("Currency", amount.TypeName);

        DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(12.3456m, rows.Rows[0]["Amount"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumnAsync_KeepsDecimalPrecisionAndScale(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Money";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Amt", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 12.34m], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [2, 0.75m], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Amt", "Amount", TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        ColumnMetadata amount = Assert.Single(
            await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken),
            column => column.Name == "Amount");
        Assert.Equal(10, amount.NumericPrecision);
        Assert.Equal(2, amount.NumericScale);

        DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(12.34m, rows.Rows[0]["Amount"]);
        Assert.Equal(0.75m, rows.Rows[1]["Amount"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumnAsync_KeepsMoneyDescriptorType(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Ledger";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new ColumnDefinition("Amount", typeof(decimal)) { IsCurrency = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, 12.3456m], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Amount", "Total", TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        ColumnMetadata total = Assert.Single(
            await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken),
            column => column.Name == "Total");
        Assert.Equal("Currency", total.TypeName);

        DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(12.3456m, rows.Rows[0]["Total"]);
    }

    [Fact]
    public async Task RenameColumnAsync_KeepsCalculatedColumn()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        const string table = "Calc";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Score", typeof(int)),
                    new("Label", typeof(string), maxLength: 40),
                    new("Doubled", typeof(int)) { IsCalculated = true, CalculationExpression = "[Score] * 2" },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [21, "a", DBNull.Value], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Doubled", "Twice", TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync(table, "Label", "Caption", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [5, "b", DBNull.Value], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        ColumnMetadata twice = Assert.Single(
            await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken),
            column => column.Name == "Twice");
        Assert.True(twice.IsCalculated);
        Assert.Equal("[Score] * 2", twice.CalculationExpression);
        Assert.Equal((byte)ColumnType.LongIntegerType, twice.CalculatedResultType);

        DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(42, rows.Rows[0]["Twice"]);
        Assert.Equal(10, rows.Rows[1]["Twice"]);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumnAsync_KeepsUnicodeCompressionFlag(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Notes";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Plain", typeof(string), maxLength: 40) { IsCompressedUnicode = false },
                    new("Packed", typeof(string), maxLength: 40),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [1, "plain text", "packed text"], TestContext.Current.CancellationToken);

            await writer.RenameColumnAsync(table, "Plain", "Plain2", TestContext.Current.CancellationToken);
            await writer.RenameColumnAsync(table, "Packed", "Packed2", TestContext.Current.CancellationToken);
        }

        IReadOnlyList<ColumnInfo> columns = await ReadColumnDescriptorsAsync(stream, table);
        Assert.False(Assert.Single(columns, column => column.Name == "Plain2").IsCompressedUnicode);
        Assert.True(Assert.Single(columns, column => column.Name == "Packed2").IsCompressedUnicode);

        await using AccessReader reader = await OpenReaderAsync(stream);
        DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("plain text", rows.Rows[0]["Plain2"]);
        Assert.Equal("packed text", rows.Rows[0]["Packed2"]);
    }

    /// <summary>
    /// Renames every column of a table that uses each column property the
    /// writer can author, then checks that each column's descriptor bytes,
    /// reader metadata, and values match the original apart from the name.
    /// </summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameColumnAsync_ChangesOnlyTheColumnName(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Everything";

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)) { IsAutoIncrement = true },
            new("Flag", typeof(bool)),
            new("Tiny", typeof(byte)),
            new("Small", typeof(short)),
            new("Real", typeof(float)),
            new("Ratio", typeof(double)),
            new("When", typeof(DateTime)),
            new("Price", typeof(decimal)) { IsCurrency = true },
            new("Name", typeof(string), maxLength: 30) { IsNullable = false },
            new("Memo", typeof(string)),
            new("Link", typeof(string)) { IsHyperlink = true },
            new("Bytes", typeof(byte[]), maxLength: 16),
            new("Blob", typeof(byte[])),
            new("Key", typeof(Guid)),
        };

        object[] row =
        [
            DBNull.Value,
            true,
            (byte)7,
            (short)-3,
            1.5f,
            2.25,
            new DateTime(2024, 5, 6, 7, 8, 9),
            12.3456m,
            "name",
            new string('m', 40),
            "site#http://example.com#",
            new byte[] { 1, 2, 3 },
            new byte[] { 9, 8, 7, 6 },
            new Guid("6f9619ff-8b86-d011-b42d-00c04fc964ff"),
        ];

        // Jet3 is left without the persisted text properties: its row encoder
        // cannot yet write the MSysObjects row once the LvProp blob pushes it
        // past 255 bytes.
        if (format != DatabaseFormat.Jet3Mdb)
        {
            columns[8] = columns[8] with { Description = "the name", DefaultValueExpression = "\"x\"" };
            columns.Add(new("Amount", typeof(decimal)) { NumericPrecision = 10, NumericScale = 2 });
            columns.Add(new("Plain", typeof(string), maxLength: 20) { IsCompressedUnicode = false });
            row = [.. row, 12.34m, "plain"];
        }

        if (format == DatabaseFormat.AceAccdb)
        {
            columns.Add(new("Big", typeof(long)));
            columns.Add(new("Stamp", typeof(DateTime)) { IsDateTimeExtended = true });
            columns.Add(new("Twice", typeof(int)) { IsCalculated = true, CalculationExpression = "[Small] * 2" });
            row = [.. row, 1L << 40, new DateTime(2024, 5, 6, 7, 8, 9, 123), -6];
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(table, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, row, TestContext.Current.CancellationToken);
        }

        IReadOnlyList<ColumnInfo> descriptorsBefore = await ReadColumnDescriptorsAsync(stream, table);
        IReadOnlyList<ColumnMetadata> metadataBefore;
        object?[] valuesBefore;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            metadataBefore = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            valuesBefore = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken)).Rows[0].ItemArray;
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            foreach (ColumnDefinition column in columns)
            {
                await writer.RenameColumnAsync(table, column.Name, column.Name + "2", TestContext.Current.CancellationToken);
            }
        }

        IReadOnlyList<ColumnInfo> descriptorsAfter = await ReadColumnDescriptorsAsync(stream, table);
        Assert.Equal(descriptorsBefore.Count, descriptorsAfter.Count);
        for (int i = 0; i < descriptorsBefore.Count; i++)
        {
            ColumnInfo before = descriptorsBefore[i];
            ColumnInfo after = descriptorsAfter[i];
            Assert.Equal(before.Name + "2", after.Name);
            Assert.Equal(
                (before.Type, before.Size, before.Flags, before.ExtraFlags, before.Misc, before.NumericPrecision, before.NumericScale, before.CalculatedResultType),
                (after.Type, after.Size, after.Flags, after.ExtraFlags, after.Misc, after.NumericPrecision, after.NumericScale, after.CalculatedResultType));
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IReadOnlyList<ColumnMetadata> metadataAfter = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(metadataBefore.Count, metadataAfter.Count);
            for (int i = 0; i < metadataBefore.Count; i++)
            {
                // Renaming Small also renames it in the expression of Twice.
                ColumnMetadata expected = metadataBefore[i] with { Name = metadataBefore[i].Name + "2" };
                if (expected.CalculationExpression == "[Small] * 2")
                {
                    expected = expected with { CalculationExpression = "[Small2] * 2" };
                }

                Assert.Equal(expected, metadataAfter[i]);
            }

            object?[] valuesAfter = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken)).Rows[0].ItemArray;
            Assert.Equal(valuesBefore, valuesAfter);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "add")]
    [InlineData(DatabaseFormat.Jet3Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet3Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "add")]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename")]
    [InlineData(DatabaseFormat.AceAccdb, "add")]
    [InlineData(DatabaseFormat.AceAccdb, "drop")]
    [InlineData(DatabaseFormat.AceAccdb, "rename")]
    public async Task SchemaRewrite_KeepsIndexFlags(DatabaseFormat format, string operation)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Flags";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Code", typeof(int)),
                    new("Tag", typeof(int)),
                    new("Note", typeof(string), maxLength: 20),
                ],
                [
                    new IndexDefinition("PK", "Id") { IsPrimaryKey = true, IgnoreNulls = true },
                    new IndexDefinition("IX_Code", "Code") { IsUnique = true, IgnoreNulls = true, IsRequired = true },
                    new IndexDefinition("IX_Tag", "Tag") { IgnoreNulls = true },
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [1, 10, 100, "a"], TestContext.Current.CancellationToken);
        }

        IReadOnlyList<IndexMetadata> before;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            before = await reader.ListIndexesAsync(table, TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            switch (operation)
            {
                case "add":
                    await writer.AddColumnAsync(table, new ColumnDefinition("Extra", typeof(int)), TestContext.Current.CancellationToken);
                    break;
                case "drop":
                    await writer.DropColumnAsync(table, "Note", TestContext.Current.CancellationToken);
                    break;
                default:
                    await writer.RenameColumnAsync(table, "Note", "Remark", TestContext.Current.CancellationToken);
                    break;
            }
        }

        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            IReadOnlyList<IndexMetadata> after = await reader.ListIndexesAsync(table, TestContext.Current.CancellationToken);
            Assert.Equal(before.Count, after.Count);
            foreach (IndexMetadata expected in before)
            {
                IndexMetadata actual = Assert.Single(after, index => index.Name == expected.Name);
                Assert.Equal(
                    (expected.Kind, expected.HasUniqueFlag, expected.IgnoreNulls, expected.IsRequired),
                    (actual.Kind, actual.HasUniqueFlag, actual.IgnoreNulls, actual.IsRequired));
            }
        }
    }

    /// <summary>
    /// Dropping a column drops every index it is a key column of, single and
    /// composite, and carries every other index with its settings; a schema
    /// rewrite refuses only an index whose key column it cannot name.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropColumn_OfIndexKeyColumn_StillDropsThatIndex(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);
        const string table = "Keys";

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Code", typeof(int)),
                    new("Tag", typeof(int)),
                ],
                [
                    new IndexDefinition("PK", "Id") { IsPrimaryKey = true },
                    new IndexDefinition("UX_Code", "Code") { IsUnique = true, IgnoreNulls = true },
                    new IndexDefinition("IX_Tag", "Tag"),
                    new IndexDefinition("IX_CodeTag", ["Code", "Tag"]) { DescendingColumns = ["Tag"] },
                ],
                TestContext.Current.CancellationToken);
            _ = await writer.InsertRowsAsync(table, [[1, 10, 100], [2, 20, 200]], TestContext.Current.CancellationToken);

            await writer.DropColumnAsync(table, "Tag", TestContext.Current.CancellationToken);

            // The carried unique index still refuses a duplicate.
            _ = await Assert.ThrowsAsync<JetConstraintException>(
                async () => await writer.InsertRowAsync(table, [3, 10], TestContext.Current.CancellationToken));
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(table, TestContext.Current.CancellationToken);
        Assert.Equal(["PK", "UX_Code"], indexes.Select(index => index.Name).Order(StringComparer.Ordinal));

        IndexMetadata primaryKey = Assert.Single(indexes, index => index.Name == "PK");
        Assert.Equal(IndexKind.PrimaryKey, primaryKey.Kind);
        Assert.Equal("Id", Assert.Single(primaryKey.Columns).Name);

        IndexMetadata unique = Assert.Single(indexes, index => index.Name == "UX_Code");
        Assert.True(unique.HasUniqueFlag);
        Assert.True(unique.IgnoreNulls);
        Assert.Equal("Code", Assert.Single(unique.Columns).Name);

        using DataTable rows = await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(["Id", "Code"], rows.Columns.Cast<DataColumn>().Select(column => column.ColumnName));
        Assert.Equal([10, 20], rows.Rows.Cast<DataRow>().Select(row => (int)row["Code"]).Order());
    }

    [Fact]
    public async Task FreshlyCreatedTable_HasNoUserDefinedIndexEntries()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);
        string tableName = $"NoIdx_{Guid.NewGuid():N}"[..18];

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        DataTable? msysIndexes = await reader.ReadDataTableAsync("MSysIndexes", cancellationToken: TestContext.Current.CancellationToken);
        if (msysIndexes is not null)
        {
            int matches = msysIndexes.AsEnumerable()
                .Count(r => MentionsTable(r, tableName));
            Assert.Equal(0, matches);
        }

        DataTable? msysRels = await reader.ReadDataTableAsync("MSysRelationships", cancellationToken: TestContext.Current.CancellationToken);
        if (msysRels is not null)
        {
            int matches = msysRels.AsEnumerable()
                .Count(r => MentionsTable(r, tableName));
            Assert.Equal(0, matches);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static bool MentionsTable(DataRow row, string tableName)
    {
        foreach (DataColumn col in row.Table.Columns)
        {
            object val = row[col];
            if (val is string s && string.Equals(s, tableName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static async ValueTask<IReadOnlyList<ColumnInfo>> ReadColumnDescriptorsAsync(MemoryStream stream, string tableName)
    {
        stream.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry? entry = await harness.GetCatalogEntryAsync(tableName, TestContext.Current.CancellationToken);
        Assert.NotNull(entry);
        TableDef? definition = await harness.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken);
        Assert.NotNull(definition);
        return definition.Columns;
    }

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

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
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
}
