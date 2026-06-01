namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Interfaces;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class AccessReaderIndexSeekTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SeekRowsAsync_UniqueIndex_ReturnsSingleRowAndEmptyMissingKey(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(format);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("UQ_Id", "Id") { IsUnique = true }],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, "alpha"],
                    [2, "beta"],
                    [3, "gamma"],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> rows = await SeekAsync(reader, "T", "UQ_Id", [2]);
        object[] row = Assert.Single(rows);
        Assert.Equal([2, "beta"], row);

        List<object[]> missing = await SeekAsync(reader, "T", "UQ_Id", [99]);
        Assert.Empty(missing);
    }

    [Fact]
    public async Task SeekRowsAsync_CompositeIndex_MatchesFullTableScan()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Orders",
                [
                    new ColumnDefinition("TenantId", typeof(int)),
                    new ColumnDefinition("Code", typeof(string), maxLength: 16),
                    new ColumnDefinition("Amount", typeof(int)),
                ],
                [new IndexDefinition("IX_Tenant_Code", ["TenantId", "Code"])],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "Orders",
                [
                    [1, "A", 10],
                    [1, "B", 11],
                    [2, "B", 20],
                    [1, "B", 12],
                    [1, "C", 13],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> expected = await ScanAsync(reader, "Orders", row => (int)row[0] == 1 && (string)row[1] == "B");
        List<object[]> actual = await SeekAsync(reader, "Orders", "IX_Tenant_Code", [1, "B"]);

        Assert.Equal(RowIds(expected, 2), RowIds(actual, 2));
    }

    [Fact]
    public async Task SeekRowsAsync_NonUniqueIndex_WalksSiblingLeavesAndMatchesFullTableScan()
    {
        const int rowCount = 700;
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Dupes",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bucket", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 16),
                ],
                [new IndexDefinition("IX_Bucket", "Bucket")],
                TestContext.Current.CancellationToken);

            var rows = new List<object[]>(rowCount);
            for (int id = 0; id < rowCount; id++)
            {
                rows.Add([id, 7, FormattableString.Invariant($"N{id:D3}")]);
            }

            await writer.InsertRowsAsync("Dupes", rows, TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> expected = await ScanAsync(reader, "Dupes", row => (int)row[1] == 7);
        List<object[]> actual = await SeekAsync(reader, "Dupes", "IX_Bucket", [7]);

        Assert.Equal(rowCount, actual.Count);
        Assert.Equal(RowIds(expected, 0), RowIds(actual, 0));
    }

    [Fact]
    public async Task SeekRowsAsync_AppendedTailKey_UsesTailPageFallThrough()
    {
        const int initialRows = 700;
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);

            var rows = new List<object[]>(initialRows);
            for (int id = 0; id < initialRows; id++)
            {
                rows.Add([id, FormattableString.Invariant($"row-{id:D3}")]);
            }

            await writer.InsertRowsAsync("T", rows, TestContext.Current.CancellationToken);
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("T", [initialRows, "tail"], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);
        List<object[]> rowsFound = await SeekAsync(reader, "T", "IX_Id", [initialRows]);

        object[] row = Assert.Single(rowsFound);
        Assert.Equal([initialRows, "tail"], row);
    }

    [Fact]
    public async Task FromIndex_WhereEquals_ReturnsExactKeyMatch()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "T",
                [
                    [1, "alpha"],
                    [2, "beta"],
                    [3, "gamma"],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> rows = await QueryAsync(reader.FromIndex("T", "IX_Id").WhereEquals(2));

        object[] row = Assert.Single(rows);
        Assert.Equal([2, "beta"], row);
    }

    [Fact]
    public async Task FromIndex_WhereBetween_ReturnsRowsInIndexOrder()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Numbers",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Label", typeof(string), maxLength: 16),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "Numbers",
                [
                    [5, "five"],
                    [1, "one"],
                    [3, "three"],
                    [2, "two"],
                    [4, "four"],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> rows = await QueryAsync(reader
            .FromIndex("Numbers", "IX_Id")
            .WhereBetween(2, 4, lowerInclusive: true, upperInclusive: false));

        Assert.Equal([2, 3], rows.Select(row => (int)row[0]).ToArray());
    }

    [Fact]
    public async Task FromIndex_WhereBetween_OnCompositeLeadingColumn_IncludesUpperPrefix()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Orders",
                [
                    new ColumnDefinition("TenantId", typeof(int)),
                    new ColumnDefinition("Code", typeof(string), maxLength: 16),
                    new ColumnDefinition("Id", typeof(int)),
                ],
                [new IndexDefinition("IX_Tenant_Code", ["TenantId", "Code"])],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "Orders",
                [
                    [1, "A", 1],
                    [2, "C", 2],
                    [3, "B", 3],
                    [2, "A", 4],
                    [4, "A", 5],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> rows = await QueryAsync(reader
            .FromIndex("Orders", "IX_Tenant_Code")
            .WhereBetween(2, 3));

        Assert.Equal([4, 2, 3], rows.Select(row => (int)row[2]).ToArray());
    }

    [Fact]
    public async Task FromIndex_WhereKeyPrefix_FiltersCompositeLeadingKey()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "People",
                [
                    new ColumnDefinition("LastName", typeof(string), maxLength: 32),
                    new ColumnDefinition("FirstName", typeof(string), maxLength: 32),
                    new ColumnDefinition("Id", typeof(int)),
                ],
                [new IndexDefinition("IX_Name", ["LastName", "FirstName"])],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "People",
                [
                    ["Smith", "Zoe", 1],
                    ["Jones", "Ada", 2],
                    ["Smith", "Ada", 3],
                    ["Smith", "Bob", 4],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<PersonRow> rows = await QueryAsync(reader
            .FromIndex<PersonRow>("People", "IX_Name")
            .WhereKeyPrefix("Smith"));

        Assert.Equal(["Ada", "Bob", "Zoe"], rows.Select(row => row.FirstName).ToArray());
        Assert.Equal([3, 4, 1], rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task FromIndex_ToRowsAsync_WithoutPredicate_ReturnsAllRowsInIndexOrder()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "Numbers",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);

            await writer.InsertRowsAsync(
                "Numbers",
                [
                    [3],
                    [1],
                    [2],
                ],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        List<object[]> rows = await QueryAsync(reader.FromIndex("Numbers", "IX_Id"));

        Assert.Equal([1, 2, 3], rows.Select(row => (int)row[0]).ToArray());
    }

    [Fact]
    public async Task SeekRowsAsync_Jet3Index_ThrowsNotSupported()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.Jet3Mdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (object[] ignored in reader.SeekRowsAsync("T", "IX_Id", [1], TestContext.Current.CancellationToken)
                .WithCancellation(TestContext.Current.CancellationToken))
            {
            }
        });
    }

    [Fact]
    public async Task FromIndex_Jet3Index_ThrowsNotSupportedOnEnumeration()
    {
        await using MemoryStream stream = await CreateFreshStreamAsync(DatabaseFormat.Jet3Mdb);

        await using (AccessWriter writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await OpenReaderAsync(stream);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (object[] ignored in reader.FromIndex("T", "IX_Id")
                .WhereEquals(1)
                .ToRowsAsync(TestContext.Current.CancellationToken)
                .WithCancellation(TestContext.Current.CancellationToken))
            {
            }
        });
    }

    private static async ValueTask<List<object[]>> SeekAsync(
        AccessReader reader,
        string tableName,
        string indexName,
        IReadOnlyList<object?> keyValues)
    {
        var rows = new List<object[]>();
        await foreach (object[] row in reader.SeekRowsAsync(tableName, indexName, keyValues, TestContext.Current.CancellationToken)
            .WithCancellation(TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static async ValueTask<List<T>> QueryAsync<T>(IAccessIndexQuery<T> query)
    {
        var rows = new List<T>();
        await foreach (T row in query.ToRowsAsync(TestContext.Current.CancellationToken)
            .WithCancellation(TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static async ValueTask<List<object[]>> ScanAsync(AccessReader reader, string tableName, Func<object[], bool> predicate)
    {
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken)
            .WithCancellation(TestContext.Current.CancellationToken))
        {
            if (predicate(row))
            {
                rows.Add(row);
            }
        }

        return rows;
    }

    private static int[] RowIds(List<object[]> rows, int columnIndex) => rows
        .Select(row => (int)row[columnIndex])
        .Order()
        .ToArray();

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
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

    private sealed class PersonRow
    {
        public string FirstName { get; set; } = string.Empty;

        public int Id { get; set; }
    }
}
