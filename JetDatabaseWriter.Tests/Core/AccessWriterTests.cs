namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// End-to-end tests for AccessWriter write operations.
/// Each test copies a test database to a temp file, writes via AccessWriter,
/// then reads back via AccessReader to verify correctness.
/// All stubs currently throw <see cref="NotImplementedException"/>.
/// </summary>
public sealed class AccessWriterTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    // ── Open / Dispose ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Open_WithValidPath_ReturnsNonNullWriter(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        Assert.NotNull(writer);
    }

    [Fact]
    public async Task Open_WithMissingFile_ThrowsFileNotFoundException()
    {
        await Assert.ThrowsAsync<FileNotFoundException>(async () => await AccessWriter.OpenAsync(@"C:\nonexistent\fake.mdb", cancellationToken: TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Open_WithNullPath_ThrowsArgumentException()
    {
        await Assert.ThrowsAsync<ArgumentException>(async () => await AccessWriter.OpenAsync((string)null!, cancellationToken: TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task OpenAsync_ReturnedWriter_ImplementsIAsyncDisposable(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        Assert.IsAssignableFrom<IAsyncDisposable>(writer);
    }

    [Fact]
    public async Task OpenAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            AccessWriter.OpenAsync(@"C:\cancel\me.mdb", cancellationToken: cts.Token).AsTask());
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task Dispose_CalledTwice_DoesNotThrow(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await writer.DisposeAsync();
        var ex = await Record.ExceptionAsync(() => writer.DisposeAsync().AsTask());

        Assert.Null(ex);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_AfterDisposeAsync_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await writer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.InsertRowAsync("AnyTable", [1], TestContext.Current.CancellationToken));
    }

    // ── InsertRow ─────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_SingleRow_IncreasesRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        long originalCount = await cachedReader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);

        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        object[] newRow = BuildDummyRow(columns);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync(tableName, newRow, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long newCount = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(originalCount + 1, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_NullValues_ThrowsArgumentNullException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await writer.InsertRowAsync(tableName, null!, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_InsertedData_IsReadableBack(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        object[] newRow = BuildDummyRow(columns);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync(tableName, newRow, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.True(dt.Rows.Count > 0);

            // The last row should contain our inserted data
            DataRow lastRow = dt.Rows[dt.Rows.Count - 1];
            Assert.NotNull(lastRow);
        }
    }

    // ── InsertRows (bulk) ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRows_MultiplRows_ReturnsCorrectInsertCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        var rows = Enumerable.Range(0, 5).Select(_ => BuildDummyRow(columns));

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        int inserted = await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);

        Assert.Equal(5, inserted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRows_MultipleRows_IncreasesRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        long originalCount = await cachedReader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        var rows = Enumerable.Range(0, 3).Select(_ => BuildDummyRow(columns)).ToList();

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowsAsync(tableName, rows, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long newCount = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(originalCount + 3, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_UpdatesTDefRowCountMetadata(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync(tableName, [4, "Delta"], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long real = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            long tdef = await GetStatsRowCountAsync(reader, tableName);

            Assert.Equal(4, real);
            Assert.Equal(real, tdef);
        }
    }

    // ── UpdateRows ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_MatchingRows_ReturnsNonZeroCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["Label"] = "UPDATED_VALUE" };

        int updated = await writer.UpdateRowsAsync(tableName, "Id", 1, updates, TestContext.Current.CancellationToken);
        Assert.True(updated > 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_MatchingRows_ChangesAreReadableBack(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);
        const string sentinel = "WRITE_TEST_SENTINEL";

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            var updates = new Dictionary<string, object> { ["Label"] = sentinel };
            await writer.UpdateRowsAsync(tableName, "Id", 1, updates, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            bool found = dt.AsEnumerable().Any(row =>
                row["Label"] is string s && s == sentinel);
            Assert.True(found);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_DoesNotChangeRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            var updates = new Dictionary<string, object> { ["Label"] = "NO_COUNT_CHANGE" };
            await writer.UpdateRowsAsync(tableName, "Id", 1, updates, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long newCount = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(3, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_PreservesTDefRowCountMetadata(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            var updates = new Dictionary<string, object> { ["Label"] = "UPDATED" };
            int updated = await writer.UpdateRowsAsync(tableName, "Id", 1, updates, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long real = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            long tdef = await GetStatsRowCountAsync(reader, tableName);

            Assert.Equal(3, real);
            Assert.Equal(real, tdef);
        }
    }

    // ── DeleteRows ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_MatchingRows_DecreasesRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        long originalCount = await cachedReader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        DataTable dt = (await cachedReader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        if (dt.Rows.Count == 0)
        {
            return;
        }

        string predicateCol = columns[0].Name;
        object predicateVal = dt.Rows[0][0];

        int deleted;
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            deleted = await writer.DeleteRowsAsync(tableName, predicateCol, predicateVal, TestContext.Current.CancellationToken);
        }

        Assert.True(deleted > 0);

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long newCount = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(originalCount - deleted, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_NonExistentColumn_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.DeleteRowsAsync(tableName, "NONEXISTENT_COLUMN_XYZ", "IMPOSSIBLE_VALUE_12345", TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_DeletedRows_AreNotReadableBack(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        DataTable originalDt = (await cachedReader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        if (originalDt.Rows.Count == 0)
        {
            return;
        }

        string predicateCol = columns[0].Name;
        object predicateVal = originalDt.Rows[0][0];

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.DeleteRowsAsync(tableName, predicateCol, predicateVal, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            bool stillPresent = dt.AsEnumerable().Any(row =>
            {
                object val = row[predicateCol];
                if (val is DBNull)
                {
                    return predicateVal == null;
                }

                return val.Equals(predicateVal);
            });
            Assert.False(stillPresent);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_UpdatesTDefRowCountMetadata(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            int deleted = await writer.DeleteRowsAsync(tableName, "Id", 2, TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long real = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            long tdef = await GetStatsRowCountAsync(reader, tableName);

            Assert.Equal(2, real);
            Assert.Equal(real, tdef);
        }
    }

    // ── CreateTable ───────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_NewTable_AppearsInListTables(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 100),
            new("Created", typeof(DateTime)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.Contains(newTableName, tables);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_NewTable_HasCorrectColumnCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
            new("Amount", typeof(decimal)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            var meta = await reader.GetColumnMetadataAsync(newTableName, TestContext.Current.CancellationToken);
            Assert.Equal(3, meta.Count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_NewTable_StartsEmpty(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Value", typeof(string), maxLength: 255),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long count = await reader.GetRealRowCountAsync(newTableName, TestContext.Current.CancellationToken);
            Assert.Equal(0, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_ThenInsert_DataIsReadable(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(newTableName, [1, "Hello"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(newTableName, [2, "World"], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long count = await reader.GetRealRowCountAsync(newTableName, TestContext.Current.CancellationToken);
            Assert.Equal(2, count);

            DataTable dt = (await reader.ReadDataTableAsync(newTableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(2, dt.Rows.Count);
        }
    }

    // ── DropTable ─────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_ExistingTable_RemovesFromListTables(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        // Create then drop
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        // Verify it exists
        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            Assert.Contains(newTableName, await reader.ListTablesAsync(TestContext.Current.CancellationToken));
        }

        // Drop it
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.DropTableAsync(newTableName, TestContext.Current.CancellationToken);
        }

        // Verify it's gone
        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            Assert.DoesNotContain(newTableName, await reader.ListTablesAsync(TestContext.Current.CancellationToken));
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_DoesNotAffectOtherTables(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        List<string> originalTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.DropTableAsync(newTableName, TestContext.Current.CancellationToken);
        }

        await using (var tempReader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            var tables = await tempReader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.Equivalent(originalTables, tables);
        }
    }

    // ── Guard clauses (already wired in stubs) ────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_NullTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.CreateTableAsync(null!, [], TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_NullColumns_ThrowsArgumentNullException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await writer.CreateTableAsync("Test", null!, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_NullTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.DeleteRowsAsync(null!, "Col", "Val", TestContext.Current.CancellationToken));
    }

    // ── Roundtrip: multiple data types ────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_WithVariousTypes_ColumnsHaveCorrectClrTypes(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TypeTest_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("IntCol", typeof(int)),
            new("TextCol", typeof(string), maxLength: 255),
            new("DateCol", typeof(DateTime)),
            new("DoubleCol", typeof(double)),
            new("BoolCol", typeof(bool)),
            new("DecimalCol", typeof(decimal)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            var meta = await reader.GetColumnMetadataAsync(newTableName, TestContext.Current.CancellationToken);
            Assert.Equal(6, meta.Count);
            Assert.Equal(typeof(int), meta[0].ClrType);
            Assert.Equal(typeof(string), meta[1].ClrType);
            Assert.Equal(typeof(DateTime), meta[2].ClrType);
            Assert.Equal(typeof(double), meta[3].ClrType);
            Assert.Equal(typeof(bool), meta[4].ClrType);
            Assert.Equal(typeof(decimal), meta[5].ClrType);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_WithVariousTypes_ValuesRoundtrip(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string newTableName = $"TypeRT_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("IntCol", typeof(int)),
            new("TextCol", typeof(string), maxLength: 100),
            new("DateCol", typeof(DateTime)),
            new("DoubleCol", typeof(double)),
            new("BoolCol", typeof(bool)),
        };

        var date = new DateTime(2025, 6, 15, 10, 30, 0);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(newTableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(newTableName, [42, "Test Value", date, 3.14, true], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(newTableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(1, dt.Rows.Count);

            DataRow row = dt.Rows[0];
            Assert.Equal(42, Convert.ToInt32(row["IntCol"], System.Globalization.CultureInfo.InvariantCulture));
            Assert.Equal("Test Value", row["TextCol"]);
            Assert.Equal(date, Convert.ToDateTime(row["DateCol"], System.Globalization.CultureInfo.InvariantCulture));
            Assert.Equal(3.14, Convert.ToDouble(row["DoubleCol"], System.Globalization.CultureInfo.InvariantCulture), 3);
            Assert.True(Convert.ToBoolean(row["BoolCol"], System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    // ── InsertRow<T> (generic POCO) ────────────────────────────────────

    private sealed class WriterPoco
    {
        public int Id { get; set; }

        public string Label { get; set; } = string.Empty;
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowGeneric_SingleRow_IncreasesRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"GenIns_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, new WriterPoco { Id = 1, Label = "Hello" }, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long count = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(1, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowGeneric_DataIsReadableBack(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"GenRT_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, new WriterPoco { Id = 42, Label = "Roundtrip" }, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            List<WriterPoco> items = await reader.ReadTableAsync<WriterPoco>(tableName, 100, TestContext.Current.CancellationToken);
            Assert.Single(items);
            Assert.Equal(42, items[0].Id);
            Assert.Equal("Roundtrip", items[0].Label);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowGeneric_NullItem_ThrowsArgumentNullException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await writer.InsertRowAsync<WriterPoco>(tableName, null!, TestContext.Current.CancellationToken));
    }

    // ── InsertRows<T> (generic bulk) ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowsGeneric_ReturnsCorrectInsertCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"GenBulk_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        var items = Enumerable.Range(1, 5).Select(i => new WriterPoco { Id = i, Label = $"Item{i}" });

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
        int inserted = await writer.InsertRowsAsync(tableName, items, TestContext.Current.CancellationToken);

        Assert.Equal(5, inserted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowsGeneric_IncreasesRowCount(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"GenCnt_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        var items = Enumerable.Range(1, 3).Select(i => new WriterPoco { Id = i, Label = $"Row{i}" }).ToList();

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, items, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long count = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(3, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowsGeneric_DataIsReadableBack(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"GenRB_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        var items = new List<WriterPoco>
        {
            new() { Id = 10, Label = "Alpha" },
            new() { Id = 20, Label = "Beta" },
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowsAsync(tableName, items, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            List<WriterPoco> readBack = await reader.ReadTableAsync<WriterPoco>(tableName, 100, TestContext.Current.CancellationToken);
            Assert.Equal(2, readBack.Count);
            Assert.Contains(readBack, p => p.Id == 10 && p.Label == "Alpha");
            Assert.Contains(readBack, p => p.Id == 20 && p.Label == "Beta");
        }
    }

    // ── Silent data corruption guards ─────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_NonExistentTable_ThrowsInvalidOperationException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.DropTableAsync("NoSuchTable_XYZ_999", TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_NonExistentPredicateColumn_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["SomeCol"] = "value" };

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.UpdateRowsAsync(tableName, "NONEXISTENT_COL_XYZ", "anything", updates, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_NonExistentTargetColumn_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        var columns = await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        DataTable dt = (await cachedReader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        if (dt.Rows.Count == 0)
        {
            return;
        }

        string predicateCol = columns[0].Name;
        object predicateVal = dt.Rows[0][0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["NONEXISTENT_COL_XYZ"] = "value" };

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.UpdateRowsAsync(tableName, predicateCol, predicateVal, updates, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_ValidColumn_NoMatchingValue_ReturnsZero(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var cachedReader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await cachedReader.ListTablesAsync(TestContext.Current.CancellationToken))[0];
        string firstCol = (await cachedReader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken))[0].Name;

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        int deleted = await writer.DeleteRowsAsync(tableName, firstCol, "IMPOSSIBLE_VALUE_THAT_WONT_MATCH_12345", TestContext.Current.CancellationToken);

        Assert.Equal(0, deleted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_WrongColumnCount_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"ColCnt_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
        };

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        // Provide 3 values for a 2-column table
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(tableName, [1, "Hello", "Extra"], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InsertRow_MemoAtLimit_RoundtripsCorrectly()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var temp = await CopyToStreamAsync(path);
        string tableName = $"Memo_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Content", typeof(string)), // no maxLength → MEMO
        };

        // 512 Unicode chars = 1024 bytes = MaxInlineMemoBytes exactly
        string memoValue = new('A', 512);

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, memoValue], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(1, dt.Rows.Count);
            Assert.Equal(memoValue, dt.Rows[0]["Content"]);
        }
    }

    [Fact]
    public async Task InsertRow_MemoOverLimit_ThrowsJetLimitationException()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var temp = await CopyToStreamAsync(path);
        string tableName = $"MemoOv_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Content", typeof(string)), // MEMO
        };

        // 513 Unicode chars = 1026 bytes > MaxInlineMemoBytes
        string memoValue = new('B', 513);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.InsertRowAsync(tableName, [1, memoValue], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InsertRow_OleBytesOverLimit_ThrowsJetLimitationException()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var temp = await CopyToStreamAsync(path);
        string tableName = $"OleOv_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Blob", typeof(byte[])), // OLE
        };

        byte[] oversized = new byte[257]; // > MaxInlineOleBytes (256)

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await writer.InsertRowAsync(tableName, [1, oversized], TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task InsertRow_OleBytesAtLimit_RoundtripsCorrectly()
    {
        string path = TestDatabases.NorthwindTraders;
        if (!File.Exists(path))
        {
            return;
        }

        var temp = await CopyToStreamAsync(path);
        string tableName = $"OleLim_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Blob", typeof(byte[])), // OLE
        };

        byte[] data = new byte[256]; // exactly MaxInlineOleBytes
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 256);
        }

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, data], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(1, dt.Rows.Count);
            Assert.NotNull(dt.Rows[0]["Blob"]);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_DuplicateName_ThrowsInvalidOperationException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"Dup_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
        };

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_DoesNotCorruptRemainingRows(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"DelChk_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, "Keep"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [2, "Delete"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [3, "Keep"], TestContext.Current.CancellationToken);
        }

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            int deleted = await writer.DeleteRowsAsync(tableName, "Name", "Delete", TestContext.Current.CancellationToken);
            Assert.Equal(1, deleted);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(2, dt.Rows.Count);

            var ids = dt.AsEnumerable()
                .Select(r => Convert.ToInt32(r["Id"], System.Globalization.CultureInfo.InvariantCulture))
                .OrderBy(x => x)
                .ToList();
            int[] expected = [1, 3];
            Assert.Equal(expected, ids);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_PreservesNonUpdatedColumns(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"UpdPrv_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
            new("Score", typeof(int)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, "Alice", 100], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [2, "Bob", 200], TestContext.Current.CancellationToken);
        }

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            var updates = new Dictionary<string, object> { ["Score"] = 999 };
            int updated = await writer.UpdateRowsAsync(tableName, "Id", 1, updates, TestContext.Current.CancellationToken);
            Assert.Equal(1, updated);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            DataRow aliceRow = dt.AsEnumerable()
                .First(r => Convert.ToInt32(r["Id"], System.Globalization.CultureInfo.InvariantCulture) == 1);

            Assert.Equal(999, Convert.ToInt32(aliceRow["Score"], System.Globalization.CultureInfo.InvariantCulture));

            // Name was NOT updated — must be preserved
            Assert.Equal("Alice", aliceRow["Name"]);
        }
    }

    // ── CreateTable round-trip: GUID column ───────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_GuidColumn_RoundtripsCorrectly(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"Guid_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("UniqueKey", typeof(Guid)),
        };

        var guid = Guid.NewGuid();

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, guid], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(1, dt.Rows.Count);
            Assert.Equal(guid, (Guid)dt.Rows[0]["UniqueKey"]);
        }
    }

    // ── CreateTable round-trip: byte[] (OLE) column ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_ByteArrayColumn_RoundtripsCorrectly(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"Blob_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Data", typeof(byte[])),
        };

        byte[] payload = [0xCA, 0xFE, 0xBA, 0xBE];

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, payload], TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(1, dt.Rows.Count);
            Assert.NotNull(dt.Rows[0]["Data"]);
        }
    }

    // ── CreateTable round-trip: multiple inserts then query ───────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_InsertMany_AllRowsReadable(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"Multi_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            for (int i = 1; i <= 10; i++)
            {
                await writer.InsertRowAsync(tableName, [i, $"Row{i}"], TestContext.Current.CancellationToken);
            }
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            long count = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(10, count);

            DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
            Assert.Equal(10, dt.Rows.Count);
        }
    }

    // ── CreateTable: empty columns list ───────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_EmptyColumnsList_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync("EmptyCol", [], TestContext.Current.CancellationToken));
    }

    // ── DropTable: null/empty name ────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_NullTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.DropTableAsync(null!, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_EmptyTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () => await writer.DropTableAsync(string.Empty, TestContext.Current.CancellationToken));
    }

    // ── DropTable: after dispose ──────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await writer.DropTableAsync("AnyTable", TestContext.Current.CancellationToken));
    }

    // ── DropTable: re-create same name ────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DropTable_ThenRecreate_Succeeds(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = $"Recr_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Value", typeof(string), maxLength: 50),
        };

        // Create
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
        }

        // Drop
        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.DropTableAsync(tableName, TestContext.Current.CancellationToken);
        }

        // Re-create with different columns
        var newColumns = new List<ColumnDefinition>
        {
            new("Key", typeof(int)),
            new("Description", typeof(string), maxLength: 200),
            new("Active", typeof(bool)),
        };

        await using (var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, newColumns, TestContext.Current.CancellationToken);
        }

        await using (var reader = await OpenReaderAsync(temp, TestContext.Current.CancellationToken))
        {
            var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.Contains(tableName, tables);

            var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
            Assert.Equal(3, meta.Count);
        }
    }

    // ── Writer negative: InsertRow to non-existent table ──────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_NonExistentTable_ThrowsInvalidOperationException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("NoSuchTable_XYZ_999", [1], TestContext.Current.CancellationToken));
    }

    // ── Writer negative: InsertRows null/empty args ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRows_NullTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowsAsync(null!, new[] { new object[] { 1 } }, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRows_NullRows_ThrowsArgumentNullException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var reader = await db.GetReaderAsync(path, TestContext.Current.CancellationToken);
        string tableName = (await reader.ListTablesAsync(TestContext.Current.CancellationToken))[0];

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await writer.InsertRowsAsync(tableName, null!, TestContext.Current.CancellationToken));
    }

    // ── Writer negative: InsertRows after dispose ─────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRows_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.InsertRowsAsync("AnyTable", new[] { new object[] { 1 } }, TestContext.Current.CancellationToken));
    }

    // ── Writer negative: UpdateRows args ──────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_NullTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["Col"] = "val" };

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.UpdateRowsAsync(null!, "Col", "val", updates, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        var updates = new Dictionary<string, object> { ["Col"] = "val" };

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.UpdateRowsAsync("AnyTable", "Col", "val", updates, TestContext.Current.CancellationToken));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_NonExistentTable_ThrowsInvalidOperationException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["Col"] = "val" };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.UpdateRowsAsync("NoSuchTable_XYZ_999", "Col", "val", updates, TestContext.Current.CancellationToken));
    }

    // ── Writer negative: DeleteRows after dispose ─────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.DeleteRowsAsync("AnyTable", "Col", "val", TestContext.Current.CancellationToken));
    }

    // ── Writer negative: CreateTable after dispose ────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task CreateTable_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        var columns = new List<ColumnDefinition> { new("Id", typeof(int)) };

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.CreateTableAsync("AnyTable", columns, TestContext.Current.CancellationToken));
    }

    // ── Writer negative: InsertRow empty table name ───────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRow_EmptyTableName_ThrowsArgumentException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.InsertRowAsync(string.Empty, [1], TestContext.Current.CancellationToken));
    }

    // ── Writer negative: DeleteRows non-existent table ────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task DeleteRows_NonExistentTable_ThrowsInvalidOperationException(string path)
    {
        var temp = await CopyToStreamAsync(path);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DeleteRowsAsync("NoSuchTable_XYZ_999", "Col", "val", TestContext.Current.CancellationToken));
    }

    // ── Writer negative: InsertRowGeneric after dispose ───────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowGeneric_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.InsertRowAsync("AnyTable", new WriterPoco { Id = 1, Label = "X" }, TestContext.Current.CancellationToken));
    }

    // ── Writer negative: InsertRowsGeneric after dispose ──────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task InsertRowsGeneric_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        var temp = await CopyToStreamAsync(path);
        var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        await writer.DisposeAsync();

        var items = new[] { new WriterPoco { Id = 1, Label = "X" } };

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await writer.InsertRowsAsync("AnyTable", items, TestContext.Current.CancellationToken));
    }

    // ── UpdateRows: no matching value returns zero ────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public async Task UpdateRows_NoMatchingValue_ReturnsZero(string path)
    {
        var temp = await CopyToStreamAsync(path);
        string tableName = await SeedUpdateTableAsync(temp);

        await using var writer = await OpenWriterAsync(temp, TestContext.Current.CancellationToken);
        var updates = new Dictionary<string, object> { ["Label"] = "NOTHING" };

        int updated = await writer.UpdateRowsAsync(tableName, "Id", 999999, updates, TestContext.Current.CancellationToken);

        Assert.Equal(0, updated);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a dummy row with plausible values for each column type.
    /// </summary>
    private static object[] BuildDummyRow(List<ColumnMetadata> columns)
    {
        var values = new object[columns.Count];
        for (int i = 0; i < columns.Count; i++)
        {
            values[i] = GetDummyValue(columns[i].ClrType);
        }

        return values;
    }

    private static object GetDummyValue(Type clrType)
    {
        if (clrType == typeof(string))
        {
            return "TestWrite";
        }

        if (clrType == typeof(int))
        {
            return 999;
        }

        if (clrType == typeof(short))
        {
            return (short)99;
        }

        if (clrType == typeof(long))
        {
            return 99999L;
        }

        if (clrType == typeof(byte))
        {
            return (byte)1;
        }

        if (clrType == typeof(bool))
        {
            return true;
        }

        if (clrType == typeof(DateTime))
        {
            return new DateTime(2025, 1, 1);
        }

        if (clrType == typeof(double))
        {
            return 1.23;
        }

        if (clrType == typeof(float))
        {
            return 1.23f;
        }

        if (clrType == typeof(decimal))
        {
            return 1.23m;
        }

        if (clrType == typeof(Guid))
        {
            return Guid.NewGuid();
        }

        if (clrType == typeof(byte[]))
        {
            return new byte[] { 0x01, 0x02 };
        }

        return DBNull.Value;
    }

    private static async ValueTask<long> GetStatsRowCountAsync(AccessReader reader, string tableName)
    {
        var stats = await reader.GetTableStatsAsync(TestContext.Current.CancellationToken);
        TableStat stat = stats.Single(s => string.Equals(s.Name, tableName, StringComparison.OrdinalIgnoreCase));
        return stat.RowCount;
    }

    /// <summary>Creates a table with known text data for UpdateRows tests and returns the table name.</summary>
    private static async Task<string> SeedUpdateTableAsync(MemoryStream stream)
    {
        string tableName = $"UpdTest_{Guid.NewGuid():N}"[..20];
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        await using var writer = await OpenWriterAsync(stream, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(tableName, [1, "Alpha"], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(tableName, [2, "Beta"], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync(tableName, [3, "Gamma"], TestContext.Current.CancellationToken);

        return tableName;
    }

    /// <summary>Opens a writer asynchronously with lockfile disabled.</summary>
    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    /// <summary>Opens a reader asynchronously with lockfile disabled.</summary>
    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken);
    }

    /// <summary>Creates a writable in-memory copy of the given database from the source cache.</summary>
    private async ValueTask<MemoryStream> CopyToStreamAsync(string sourcePath)
    {
        byte[] bytes = await db.GetFileAsync(sourcePath, TestContext.Current.CancellationToken);
        var ms = new MemoryStream();
        await ms.WriteAsync(bytes, TestContext.Current.CancellationToken);
        ms.Position = 0;
        return ms;
    }
}
