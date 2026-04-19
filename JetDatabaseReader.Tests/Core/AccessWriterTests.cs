namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// End-to-end tests for AccessWriter write operations.
/// Each test copies a test database to a temp file, writes via AccessWriter,
/// then reads back via AccessReader to verify correctness.
/// All stubs currently throw <see cref="NotImplementedException"/>.
/// </summary>
public sealed class AccessWriterTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    // ── Open / Dispose ────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Open_WithValidPath_ReturnsNonNullWriter(string path)
    {
        string temp = CopyToTemp(path);

        using var writer = AccessWriter.Open(temp);

        Assert.NotNull(writer);
    }

    [Fact]
    public void Open_WithMissingFile_ThrowsFileNotFoundException()
    {
        Assert.Throws<FileNotFoundException>(() => AccessWriter.Open(@"C:\nonexistent\fake.mdb"));
    }

    [Fact]
    public void Open_WithNullPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => AccessWriter.Open(null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Dispose_CalledTwice_DoesNotThrow(string path)
    {
        string temp = CopyToTemp(path);
        var writer = AccessWriter.Open(temp);

        writer.Dispose();
        var ex = Record.Exception(() => writer.Dispose());

        Assert.Null(ex);
    }

    // ── InsertRow ─────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRow_SingleRow_IncreasesRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        long originalCount;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            originalCount = reader.GetRealRowCount(tableName);
        }

        var columns = GetColumnMetadataForTable(temp, tableName);
        object[] newRow = BuildDummyRow(columns);

        using (var writer = AccessWriter.Open(temp))
        {
            writer.InsertRow(tableName, newRow);
        }

        using (var reader = AccessReader.Open(temp))
        {
            long newCount = reader.GetRealRowCount(tableName);
            Assert.Equal(originalCount + 1, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRow_NullValues_ThrowsArgumentNullException(string path)
    {
        string temp = CopyToTemp(path);

        using var writer = AccessWriter.Open(temp);
        string tableName;

        using (var reader = AccessReader.Open(temp, new AccessReaderOptions { FileShare = FileShare.ReadWrite }))
        {
            tableName = reader.ListTables()[0];
        }

        Assert.Throws<ArgumentNullException>(() => writer.InsertRow(tableName, null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRow_InsertedData_IsReadableBack(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        List<ColumnMetadata> columns;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            columns = reader.GetColumnMetadata(tableName);
        }

        object[] newRow = BuildDummyRow(columns);

        using (var writer = AccessWriter.Open(temp))
        {
            writer.InsertRow(tableName, newRow);
        }

        using (var reader = AccessReader.Open(temp))
        {
            DataTable dt = reader.ReadTable(tableName)!;
            Assert.True(dt.Rows.Count > 0);

            // The last row should contain our inserted data
            DataRow lastRow = dt.Rows[dt.Rows.Count - 1];
            Assert.NotNull(lastRow);
        }
    }

    // ── InsertRows (bulk) ─────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRows_MultiplRows_ReturnsCorrectInsertCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        List<ColumnMetadata> columns;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            columns = reader.GetColumnMetadata(tableName);
        }

        var rows = Enumerable.Range(0, 5).Select(_ => BuildDummyRow(columns));

        using var writer = AccessWriter.Open(temp);
        int inserted = writer.InsertRows(tableName, rows);

        Assert.Equal(5, inserted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRows_MultipleRows_IncreasesRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        long originalCount;
        List<ColumnMetadata> columns;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            originalCount = reader.GetRealRowCount(tableName);
            columns = reader.GetColumnMetadata(tableName);
        }

        var rows = Enumerable.Range(0, 3).Select(_ => BuildDummyRow(columns)).ToList();

        using (var writer = AccessWriter.Open(temp))
        {
            writer.InsertRows(tableName, rows);
        }

        using (var reader = AccessReader.Open(temp))
        {
            long newCount = reader.GetRealRowCount(tableName);
            Assert.Equal(originalCount + 3, newCount);
        }
    }

    // ── UpdateRows ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void UpdateRows_MatchingRows_ReturnsNonZeroCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        string predicateCol;
        object predicateVal;
        string targetCol;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            var columns = reader.GetColumnMetadata(tableName);

            // Find a text column to update and a column with a known value to filter on
            var textCol = columns.FirstOrDefault(c => c.ClrType == typeof(string));
            if (textCol == null)
            {
                return; // skip databases with no text columns
            }

            targetCol = textCol.Name;

            // Read first row to get a real predicate value
            DataTable dt = reader.ReadTable(tableName)!;
            if (dt.Rows.Count == 0)
            {
                return; // skip empty tables
            }

            predicateCol = columns[0].Name;
            predicateVal = dt.Rows[0][0];
        }

        using var writer = AccessWriter.Open(temp);
        var updates = new Dictionary<string, object> { [targetCol] = "UPDATED_VALUE" };

        int updated = writer.UpdateRows(tableName, predicateCol, predicateVal, updates);

        Assert.True(updated > 0);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void UpdateRows_MatchingRows_ChangesAreReadableBack(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        string predicateCol;
        object predicateVal;
        string targetCol;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            var columns = reader.GetColumnMetadata(tableName);

            var textCol = columns.FirstOrDefault(c => c.ClrType == typeof(string));
            if (textCol == null)
            {
                return;
            }

            targetCol = textCol.Name;

            DataTable dt = reader.ReadTable(tableName)!;
            if (dt.Rows.Count == 0)
            {
                return;
            }

            predicateCol = columns[0].Name;
            predicateVal = dt.Rows[0][0];
        }

        const string sentinel = "WRITE_TEST_SENTINEL";

        using (var writer = AccessWriter.Open(temp))
        {
            var updates = new Dictionary<string, object> { [targetCol] = sentinel };
            writer.UpdateRows(tableName, predicateCol, predicateVal, updates);
        }

        using (var reader = AccessReader.Open(temp))
        {
            DataTable dt = reader.ReadTable(tableName)!;
            bool found = dt.AsEnumerable().Any(row =>
                row[targetCol] is string s && s == sentinel);
            Assert.True(found);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void UpdateRows_DoesNotChangeRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        long originalCount;
        string predicateCol;
        object predicateVal;
        string targetCol;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            originalCount = reader.GetRealRowCount(tableName);
            var columns = reader.GetColumnMetadata(tableName);

            var textCol = columns.FirstOrDefault(c => c.ClrType == typeof(string));
            if (textCol == null)
            {
                return;
            }

            targetCol = textCol.Name;

            DataTable dt = reader.ReadTable(tableName)!;
            if (dt.Rows.Count == 0)
            {
                return;
            }

            predicateCol = columns[0].Name;
            predicateVal = dt.Rows[0][0];
        }

        using (var writer = AccessWriter.Open(temp))
        {
            var updates = new Dictionary<string, object> { [targetCol] = "NO_COUNT_CHANGE" };
            writer.UpdateRows(tableName, predicateCol, predicateVal, updates);
        }

        using (var reader = AccessReader.Open(temp))
        {
            long newCount = reader.GetRealRowCount(tableName);
            Assert.Equal(originalCount, newCount);
        }
    }

    // ── DeleteRows ────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DeleteRows_MatchingRows_DecreasesRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        long originalCount;
        string predicateCol;
        object predicateVal;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            originalCount = reader.GetRealRowCount(tableName);
            var columns = reader.GetColumnMetadata(tableName);

            DataTable dt = reader.ReadTable(tableName)!;
            if (dt.Rows.Count == 0)
            {
                return;
            }

            predicateCol = columns[0].Name;
            predicateVal = dt.Rows[0][0];
        }

        int deleted;
        using (var writer = AccessWriter.Open(temp))
        {
            deleted = writer.DeleteRows(tableName, predicateCol, predicateVal);
        }

        Assert.True(deleted > 0);

        using (var reader = AccessReader.Open(temp))
        {
            long newCount = reader.GetRealRowCount(tableName);
            Assert.Equal(originalCount - deleted, newCount);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DeleteRows_NoMatchingRows_ReturnsZero(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
        }

        using var writer = AccessWriter.Open(temp);

        // Use a value that won't exist in any table
        int deleted = writer.DeleteRows(tableName, "NONEXISTENT_COLUMN_XYZ", "IMPOSSIBLE_VALUE_12345");

        Assert.Equal(0, deleted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DeleteRows_DeletedRows_AreNotReadableBack(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;
        string predicateCol;
        object predicateVal;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
            var columns = reader.GetColumnMetadata(tableName);

            DataTable dt = reader.ReadTable(tableName)!;
            if (dt.Rows.Count == 0)
            {
                return;
            }

            predicateCol = columns[0].Name;
            predicateVal = dt.Rows[0][0];
        }

        using (var writer = AccessWriter.Open(temp))
        {
            writer.DeleteRows(tableName, predicateCol, predicateVal);
        }

        using (var reader = AccessReader.Open(temp))
        {
            DataTable dt = reader.ReadTable(tableName)!;
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

    // ── CreateTable ───────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_NewTable_AppearsInListTables(string path)
    {
        string temp = CopyToTemp(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 100),
            new("Created", typeof(DateTime)),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        using (var reader = AccessReader.Open(temp))
        {
            var tables = reader.ListTables();
            Assert.Contains(newTableName, tables);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_NewTable_HasCorrectColumnCount(string path)
    {
        string temp = CopyToTemp(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 50),
            new("Amount", typeof(decimal)),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        using (var reader = AccessReader.Open(temp))
        {
            var meta = reader.GetColumnMetadata(newTableName);
            Assert.Equal(3, meta.Count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_NewTable_StartsEmpty(string path)
    {
        string temp = CopyToTemp(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Value", typeof(string), maxLength: 255),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        using (var reader = AccessReader.Open(temp))
        {
            long count = reader.GetRealRowCount(newTableName);
            Assert.Equal(0, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_ThenInsert_DataIsReadable(string path)
    {
        string temp = CopyToTemp(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
            writer.InsertRow(newTableName, new object[] { 1, "Hello" });
            writer.InsertRow(newTableName, new object[] { 2, "World" });
        }

        using (var reader = AccessReader.Open(temp))
        {
            long count = reader.GetRealRowCount(newTableName);
            Assert.Equal(2, count);

            DataTable dt = reader.ReadTable(newTableName)!;
            Assert.Equal(2, dt.Rows.Count);
        }
    }

    // ── DropTable ─────────────────────────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DropTable_ExistingTable_RemovesFromListTables(string path)
    {
        string temp = CopyToTemp(path);
        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);

        // Create then drop
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        // Verify it exists
        using (var reader = AccessReader.Open(temp))
        {
            Assert.Contains(newTableName, reader.ListTables());
        }

        // Drop it
        using (var writer = AccessWriter.Open(temp))
        {
            writer.DropTable(newTableName);
        }

        // Verify it's gone
        using (var reader = AccessReader.Open(temp))
        {
            Assert.DoesNotContain(newTableName, reader.ListTables());
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DropTable_DoesNotAffectOtherTables(string path)
    {
        string temp = CopyToTemp(path);
        List<string> originalTables;

        using (var reader = AccessReader.Open(temp))
        {
            originalTables = reader.ListTables();
        }

        string newTableName = $"TestTable_{Guid.NewGuid():N}".Substring(0, 20);
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        using (var writer = AccessWriter.Open(temp))
        {
            writer.DropTable(newTableName);
        }

        using (var reader = AccessReader.Open(temp))
        {
            var tables = reader.ListTables();
            Assert.Equivalent(originalTables, tables);
        }
    }

    // ── Guard clauses (already wired in stubs) ────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRow_AfterDispose_ThrowsObjectDisposedException(string path)
    {
        string temp = CopyToTemp(path);
        var writer = AccessWriter.Open(temp);
        writer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => writer.InsertRow("AnyTable", new object[] { 1 }));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_NullTableName_ThrowsArgumentException(string path)
    {
        string temp = CopyToTemp(path);

        using var writer = AccessWriter.Open(temp);

        Assert.Throws<ArgumentException>(() => writer.CreateTable(null!, new List<ColumnDefinition>()));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_NullColumns_ThrowsArgumentNullException(string path)
    {
        string temp = CopyToTemp(path);

        using var writer = AccessWriter.Open(temp);

        Assert.Throws<ArgumentNullException>(() => writer.CreateTable("Test", null!));
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void DeleteRows_NullTableName_ThrowsArgumentException(string path)
    {
        string temp = CopyToTemp(path);

        using var writer = AccessWriter.Open(temp);

        Assert.Throws<ArgumentException>(() => writer.DeleteRows(null!, "Col", "Val"));
    }

    // ── Roundtrip: multiple data types ────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void CreateTable_WithVariousTypes_ColumnsHaveCorrectClrTypes(string path)
    {
        string temp = CopyToTemp(path);
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
        }

        using (var reader = AccessReader.Open(temp))
        {
            var meta = reader.GetColumnMetadata(newTableName);
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
    public void InsertRow_WithVariousTypes_ValuesRoundtrip(string path)
    {
        string temp = CopyToTemp(path);
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(newTableName, columns);
            writer.InsertRow(newTableName, new object[] { 42, "Test Value", date, 3.14, true });
        }

        using (var reader = AccessReader.Open(temp))
        {
            DataTable dt = reader.ReadTable(newTableName)!;
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
    public void InsertRowGeneric_SingleRow_IncreasesRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName = $"GenIns_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRow(tableName, new WriterPoco { Id = 1, Label = "Hello" });
        }

        using (var reader = AccessReader.Open(temp))
        {
            long count = reader.GetRealRowCount(tableName);
            Assert.Equal(1, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRowGeneric_DataIsReadableBack(string path)
    {
        string temp = CopyToTemp(path);
        string tableName = $"GenRT_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRow(tableName, new WriterPoco { Id = 42, Label = "Roundtrip" });
        }

        using (var reader = AccessReader.Open(temp))
        {
            List<WriterPoco> items = reader.ReadTable<WriterPoco>(tableName, 100);
            Assert.Single(items);
            Assert.Equal(42, items[0].Id);
            Assert.Equal("Roundtrip", items[0].Label);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRowGeneric_NullItem_ThrowsArgumentNullException(string path)
    {
        string temp = CopyToTemp(path);
        string tableName;

        using (var reader = AccessReader.Open(temp))
        {
            tableName = reader.ListTables()[0];
        }

        using var writer = AccessWriter.Open(temp);

        Assert.Throws<ArgumentNullException>(() => writer.InsertRow<WriterPoco>(tableName, null!));
    }

    // ── InsertRows<T> (generic bulk) ──────────────────────────────────

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRowsGeneric_ReturnsCorrectInsertCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName = $"GenBulk_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        var items = Enumerable.Range(1, 5).Select(i => new WriterPoco { Id = i, Label = $"Item{i}" });

        using var writer = AccessWriter.Open(temp);
        writer.CreateTable(tableName, columns);
        int inserted = writer.InsertRows(tableName, items);

        Assert.Equal(5, inserted);
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRowsGeneric_IncreasesRowCount(string path)
    {
        string temp = CopyToTemp(path);
        string tableName = $"GenCnt_{Guid.NewGuid():N}".Substring(0, 18);

        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        var items = Enumerable.Range(1, 3).Select(i => new WriterPoco { Id = i, Label = $"Row{i}" }).ToList();

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRows(tableName, items);
        }

        using (var reader = AccessReader.Open(temp))
        {
            long count = reader.GetRealRowCount(tableName);
            Assert.Equal(3, count);
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void InsertRowsGeneric_DataIsReadableBack(string path)
    {
        string temp = CopyToTemp(path);
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

        using (var writer = AccessWriter.Open(temp))
        {
            writer.CreateTable(tableName, columns);
            writer.InsertRows(tableName, items);
        }

        using (var reader = AccessReader.Open(temp))
        {
            List<WriterPoco> readBack = reader.ReadTable<WriterPoco>(tableName, 100);
            Assert.Equal(2, readBack.Count);
            Assert.Contains(readBack, p => p.Id == 10 && p.Label == "Alpha");
            Assert.Contains(readBack, p => p.Id == 20 && p.Label == "Beta");
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────

    public void Dispose()
    {
        foreach (string path in _tempFiles)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                /* best-effort cleanup */
            }
        }
    }

    private static List<ColumnMetadata> GetColumnMetadataForTable(string path, string tableName)
    {
        using var reader = AccessReader.Open(path);
        return reader.GetColumnMetadata(tableName);
    }

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

    /// <summary>Creates a writable temp copy of the given database and tracks it for cleanup.</summary>
    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetWriteTest_{Guid.NewGuid():N}{ext}");
        File.Copy(sourcePath, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }
}
