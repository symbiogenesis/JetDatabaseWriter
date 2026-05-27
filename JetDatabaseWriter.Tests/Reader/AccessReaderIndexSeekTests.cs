namespace JetDatabaseWriter.Tests.Reader;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

public sealed class AccessReaderIndexSeekTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SeekRowsAsync_UniqueIndex_ReturnsSingleRowAndEmptyMissingKey(DatabaseFormat format)
    {
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);

        var rows = await SeekAsync(reader, "T", "UQ_Id", [2]);
        object[] row = Assert.Single(rows);
        Assert.Equal([2, "beta"], row);

        var missing = await SeekAsync(reader, "T", "UQ_Id", [99]);
        Assert.Empty(missing);
    }

    [Fact]
    public async Task SeekRowsAsync_CompositeIndex_MatchesFullTableScan()
    {
        await using var stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);

        var expected = await ScanAsync(reader, "Orders", row => (int)row[0] == 1 && (string)row[1] == "B");
        var actual = await SeekAsync(reader, "Orders", "IX_Tenant_Code", [1, "B"]);

        Assert.Equal(RowIds(expected, 2), RowIds(actual, 2));
    }

    [Fact]
    public async Task SeekRowsAsync_NonUniqueIndex_WalksSiblingLeavesAndMatchesFullTableScan()
    {
        const int RowCount = 700;
        await using var stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (var writer = await OpenWriterAsync(stream))
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

            var rows = new List<object[]>(RowCount);
            for (int id = 0; id < RowCount; id++)
            {
                rows.Add([id, 7, FormattableString.Invariant($"N{id:D3}")]);
            }

            await writer.InsertRowsAsync("Dupes", rows, TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        var expected = await ScanAsync(reader, "Dupes", row => (int)row[1] == 7);
        var actual = await SeekAsync(reader, "Dupes", "IX_Bucket", [7]);

        Assert.Equal(RowCount, actual.Count);
        Assert.Equal(RowIds(expected, 0), RowIds(actual, 0));
    }

    [Fact]
    public async Task SeekRowsAsync_AppendedTailKey_UsesTailPageFallThrough()
    {
        const int InitialRows = 700;
        await using var stream = await CreateFreshStreamAsync(DatabaseFormat.AceAccdb);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Name", typeof(string), maxLength: 32),
                ],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);

            var rows = new List<object[]>(InitialRows);
            for (int id = 0; id < InitialRows; id++)
            {
                rows.Add([id, FormattableString.Invariant($"row-{id:D3}")]);
            }

            await writer.InsertRowsAsync("T", rows, TestContext.Current.CancellationToken);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync("T", [InitialRows, "tail"], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rowsFound = await SeekAsync(reader, "T", "IX_Id", [InitialRows]);

        object[] row = Assert.Single(rowsFound);
        Assert.Equal([InitialRows, "tail"], row);
    }

    [Fact]
    public async Task SeekRowsAsync_Jet3Index_ThrowsNotSupported()
    {
        await using var stream = await CreateFreshStreamAsync(DatabaseFormat.Jet3Mdb);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_Id", "Id")],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (object[] ignored in reader.SeekRowsAsync("T", "IX_Id", [1], TestContext.Current.CancellationToken)
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
        .OrderBy(id => id)
        .ToArray();

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
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
}
