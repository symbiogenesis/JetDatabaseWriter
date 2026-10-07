namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

/// <summary>
/// Access gives every complex column a unique, Required index named
/// <c>&lt;column&gt;_&lt;32 hex&gt;</c> over the parent row's per-row complex
/// reference, keyed like a Long Integer. The writer must encode those keys,
/// or every keyed write to such a table (update, delete, an insert that
/// supplies a reference) throws <see cref="NotSupportedException"/>, and a
/// delete is left half applied.
/// </summary>
public sealed class ComplexColumnIndexFixtureTests
{
    private const string VersionHistoryColumn = "VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880";

    /// <summary>Gets the Access-authored complex-column indexes: fixture, table and index name.</summary>
    public static TheoryData<string, string, string> AccessComplexIndexes => new()
    {
        { TestDatabases.ComplexFields, "Documents", "Attachments_CFC7F98A63064F2DBDB6822D653AB763" },
        { TestDatabases.ComplexDataTestV2007, "Table1", "attach-data_071D71EDD53D45A1A9089929F06857D9" },
        { TestDatabases.ComplexDataTestV2007, "Table1", "multi-value-data_F4C67B0F60124C1989D5583C00CF76E2" },
        { TestDatabases.ComplexDataTestV2007, "Table1", "VersionHistory_F5F8918F-0A3F-4D_6E54CCBB170741DD8FD837271ED8B90C" },
        { TestDatabases.ComplexDataTestV2010, "Table1", "attach-data_071D71EDD53D45A1A9089929F06857D9" },
        { TestDatabases.ComplexDataTestV2010, "Table1", "multi-value-data_F4C67B0F60124C1989D5583C00CF76E2" },
        { TestDatabases.ComplexDataTestV2010, "Table1", "VersionHistory_F5F8918F-0A3F-4D_6E54CCBB170741DD8FD837271ED8B90C" },
        { TestDatabases.NorthwindTraders, "ProductCategories", "ProductCategoryImage_5C9A6A17CF9D4E1CA64DB2DECECEBB16" },
    };

    /// <summary>Gets the Access-authored tables with complex columns that the update test changes, in every write mode.</summary>
    public static TheoryData<string, WriteMode> UpdateCases()
    {
        var data = new TheoryData<string, WriteMode>();
        foreach (string fixture in new[] { TestDatabases.NorthwindTraders, TestDatabases.ComplexDataTestV2007, TestDatabases.ComplexDataTestV2010 })
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(fixture, mode);
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(AccessComplexIndexes))]
    public async Task ComplexColumnIndexLeafKeys_MatchEncoder(string fixture, string tableName, string indexName)
    {
        await using MemoryStream ms = await CopyFixtureAsync(fixture);

        IndexMetadata index;
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            index = Assert.Single(await reader.ListIndexesAsync(tableName, Ct), i => i.Name == indexName);
        }

        Assert.True(index.HasUniqueFlag);
        Assert.True(index.IsRequired);
        IndexColumnReference key = Assert.Single(index.Columns);

        RawTable table = await ReadRawTableAsync(ms, tableName);
        ColumnType columnType = table.Definition.FindColumn(key.Name)!.Type;
        Assert.True(columnType is ColumnType.ComplexType);

        List<byte[]> expected = [.. table.Rows
            .Select(row => IndexKeyEncoder.EncodeEntry(columnType, Slot(table, row, key.Name), key.IsAscending))
            .OrderBy(k => k, ByteArrayComparer.Instance)];
        List<byte[]> onDisk = await ReadIndexLeafKeysAsync(ms, tableName, indexName);

        Assert.NotEmpty(expected);
        Assert.Equal(expected, onDisk);
    }

    [Theory]
    [MemberData(nameof(UpdateCases))]
    public async Task UpdateRows_AccessTableWithComplexColumns_Succeeds(string fixture, WriteMode mode)
    {
        (string table, string keyColumn, object key, string column) = fixture == TestDatabases.NorthwindTraders
            ? ("ProductCategories", "ProductCategoryID", (object)1, "ProductCategoryDesc")
            : ("Table1", "id", "row1", "memo-data");
        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        Dictionary<string, object[]> before = await ReadRowsByKeyAsync(ms, table, keyColumn);

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
                Assert.Equal(1, await writer.UpdateRowsAsync(table, keyColumn, key, new Dictionary<string, object?> { [column] = "changed by the writer" }, Ct)));
        }

        Dictionary<string, object[]> after = await ReadRowsByKeyAsync(ms, table, keyColumn);
        IReadOnlyList<ColumnMetadata> columns = await ReadColumnsAsync(ms, table);
        Assert.Equal(before.Keys.Order(), after.Keys.Order());
        foreach ((string rowKey, object[] row) in after)
        {
            foreach (ColumnMetadata meta in columns)
            {
                object expected = rowKey == Convert.ToString(key, System.Globalization.CultureInfo.InvariantCulture) && meta.Name == column
                    ? "changed by the writer"
                    : before[rowKey][meta.Ordinal];
                Assert.Equal(expected, row[meta.Ordinal]);
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task DeleteRows_AccessTableWithComplexColumns_RemovesRowAndFlatChildren(WriteMode mode)
    {
        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.ComplexDataTestV2007);
        RawTable original = await ReadRawTableAsync(ms, "Table1");
        object[] row2 = Assert.Single(original.Rows, r => (string)r[0] == "row2");
        int reference = Slot(original, row2, "attach-data")!.Value;

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () => Assert.Equal(1, await writer.DeleteRowsAsync("Table1", "id", "row2", Ct)));
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        Assert.DoesNotContain(await reader.GetAttachmentsAsync("Table1", "attach-data", Ct), a => a.ConceptualTableId == reference);
        Assert.DoesNotContain(await reader.GetMultiValueItemsAsync("Table1", "multi-value-data", Ct), i => i.ConceptualTableId == reference);
        Assert.DoesNotContain(await reader.GetMultiValueItemsAsync("Table1", VersionHistoryColumn, Ct), i => i.ConceptualTableId == reference);
        Assert.Equal(["row1", "row3", "row4"], (await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct)).Select(r => (string)r[0]).Order());

        IndexMetadata primaryKey = Assert.Single(await reader.ListIndexesAsync("Table1", Ct), i => i.Kind == IndexKind.PrimaryKey);
        Assert.Empty(await reader.SeekRowsAsync("Table1", primaryKey.Name, ["row2"], Ct).ToListAsync(Ct));
        Assert.Single(await reader.SeekRowsAsync("Table1", primaryKey.Name, ["row3"], Ct).ToListAsync(Ct));

        // Every complex index still holds exactly the remaining rows' references.
        RawTable remaining = await ReadRawTableAsync(ms, "Table1");
        foreach (string column in new[] { "attach-data", "multi-value-data", VersionHistoryColumn })
        {
            IndexMetadata index = Assert.Single(await reader.ListIndexesAsync("Table1", Ct), i => i.Columns.Count == 1 && i.Columns[0].Name == column);
            List<byte[]> expected = [.. remaining.Rows
                .Select(r => IndexKeyEncoder.EncodeEntry(ColumnType.ComplexType, Slot(remaining, r, column)))
                .OrderBy(k => k, ByteArrayComparer.Instance)];
            Assert.Equal(expected, await ReadIndexLeafKeysAsync(ms, "Table1", index.Name));
        }
    }

    [Fact]
    public async Task DeleteRows_TestOleFixtureWithComplexColumn_DeletesOneRow()
    {
        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.TestOleV2007);
        List<object[]> rows;
        string keyColumn;
        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            Assert.NotEmpty(await reader.GetComplexColumnsAsync("Table1", Ct));
            keyColumn = (await reader.GetColumnMetadataAsync("Table1", Ct))[0].Name;
            rows = await reader.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms))
        {
            Assert.Equal(1, await writer.DeleteRowsAsync("Table1", keyColumn, rows[0][0], Ct));
        }

        await using AccessReader after = await OpenReaderAsync(ms);
        Assert.Equal(rows.Skip(1).Select(r => r[0]), (await after.Rows("Table1", cancellationToken: Ct).ToListAsync(Ct)).Select(r => r[0]));
    }

    [Theory]
    [MemberData(nameof(UpdateCases))]
    public async Task SchemaEdits_PreserveAccessComplexIndexesAndRows(string fixture, WriteMode mode)
    {
        string tableName = fixture == TestDatabases.NorthwindTraders ? "ProductCategories" : "Table1";
        await using MemoryStream stream = await CopyFixtureAsync(fixture);
        RawTable original = await ReadRawTableAsync(stream, tableName);
        IReadOnlyList<IndexMetadata> before;
        await using (AccessReader reader = await OpenReaderAsync(stream))
        {
            before = await reader.ListIndexesAsync(tableName, Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                await writer.AddColumnAsync(tableName, new ColumnDefinition("AddedMarker", typeof(int)), Ct);
                await writer.RenameColumnAsync(tableName, "AddedMarker", "RenamedMarker", Ct);
                await writer.DropColumnAsync(tableName, "RenamedMarker", Ct);
            });
        }

        RawTable rewritten = await ReadRawTableAsync(stream, tableName);
        Assert.Equal(original.Rows, rewritten.Rows);
        await using AccessReader after = await OpenReaderAsync(stream);
        IReadOnlyList<IndexMetadata> indexes = await after.ListIndexesAsync(tableName, Ct);
        foreach (IndexMetadata index in before.Where(i => i.Kind is IndexKind.Normal or IndexKind.PrimaryKey))
        {
            IndexMetadata kept = Assert.Single(indexes, i => i.Name == index.Name);
            Assert.Equal(index.HasUniqueFlag, kept.HasUniqueFlag);
            Assert.Equal(index.IsRequired, kept.IsRequired);
            Assert.Equal(index.Columns.Select(c => c.Name), kept.Columns.Select(c => c.Name));
            IndexColumnReference first = index.Columns[0];
            ColumnType type = original.Definition.FindColumn(first.Name)!.Type;
            if (type is ColumnType.ComplexType)
            {
                List<byte[]> expected = [.. rewritten.Rows
                    .Select(row => IndexKeyEncoder.EncodeEntry(type, Slot(rewritten, row, first.Name), first.IsAscending))
                    .OrderBy(key => key, ByteArrayComparer.Instance)];
                Assert.Equal(expected, await ReadIndexLeafKeysAsync(stream, tableName, index.Name));
            }
        }
    }

    private static async Task<Dictionary<string, object[]>> ReadRowsByKeyAsync(MemoryStream ms, string table, string keyColumn)
    {
        await using AccessReader reader = await OpenReaderAsync(ms);
        int keyOrdinal = (await reader.GetColumnMetadataAsync(table, Ct)).Single(c => c.Name == keyColumn).Ordinal;
        return (await reader.Rows(table, cancellationToken: Ct).ToListAsync(Ct))
            .ToDictionary(r => Convert.ToString(r[keyOrdinal], System.Globalization.CultureInfo.InvariantCulture)!);
    }

    private static async Task<IReadOnlyList<ColumnMetadata>> ReadColumnsAsync(MemoryStream ms, string table)
    {
        await using AccessReader reader = await OpenReaderAsync(ms);
        return await reader.GetColumnMetadataAsync(table, Ct);
    }

    private sealed class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);
    }
}
