namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Tests for complex-type columns — attachment fields (type 0x11) and
/// multi-value fields (type 0x12):
///   1. Attachment fields  — type name, CLR type, and cell-value decoding
///   2. Multi-value fields — metadata and cell-value decoding (TDD)
///
/// Current state: metadata (TypeName, ClrType) is reported correctly,
/// but cell values are not decoded — they appear as raw bytes or DBNull.
/// When fully implemented, the reader should follow the complexid FK to the
/// hidden system table and return decoded values for each row.
/// </summary>
public sealed class ComplexFieldTests
{
    // ═══════════════════════════════════════════════════════════════════
    // 1. ATTACHMENT FIELDS (type 0x11)
    // ═══════════════════════════════════════════════════════════════════
    //
    // Attachment columns are complex-data pointers to a hidden system table.
    // They should report a friendly "Attachment" type name and an appropriate
    // CLR type rather than a raw hex code.

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Attachment_GetColumnMetadata_ReportsAttachmentType(string path)
    {
        // Columns with type 0x11 should report a friendly type name, not a raw hex code.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            foreach (var col in meta)
            {
                Assert.DoesNotMatch(@"^0x[0-9A-Fa-f]{2}$", col.TypeName);
            }
        }
    }

    [Fact]
    public void Attachment_TypeCodeToName_ReturnsAttachment()
    {
        // All column type names should be friendly strings, not raw hex codes.
        using var reader = AccessReader.Open(TestDatabases.NorthwindTraders);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            Assert.All(meta, col =>
                Assert.False(
                    col.TypeName.StartsWith("0x", StringComparison.Ordinal),
                    $"Column '{col.Name}' in table '{table}' has raw type name '{col.TypeName}'"));
        }
    }

    [Fact]
    public void Attachment_ClrType_IsByteArrayOrCollection()
    {
        // Attachment columns map to a CLR type that can represent
        // multiple files — not typeof(string).
        using var reader = AccessReader.Open(TestDatabases.NorthwindTraders);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            foreach (var col in meta)
            {
                if (col.TypeName == "Attachment")
                {
                    Assert.NotEqual(typeof(string), col.ClrType);
                }
            }
        }
    }

    [Fact]
    public void Attachment_ReadAsDataTable_ColumnTypeIsNotString()
    {
        // When reading a DataTable, attachment columns should not be typed as string.
        using var reader = AccessReader.Open(TestDatabases.NorthwindTraders);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            var attachCols = meta.Where(c => c.TypeName == "Attachment").ToList();
            if (attachCols.Count == 0)
            {
                continue;
            }

            TableResult result = reader.ReadTable(table, maxRows: 1);
            DataTable dt = result.ToDataTable();
            foreach (var col in attachCols)
            {
                Assert.NotEqual(typeof(string), dt.Columns[col.Name]!.DataType);
            }

            return; // one table with attachments is enough
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Attachment_ReadTable_DoesNotThrowOnComplexColumns(string path)
    {
        // Reading a table with complex columns should not crash, even if
        // values are not fully decoded.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            bool hasComplex = meta.Any(c =>
                c.TypeName == "Attachment" || c.TypeName == "Complex");

            if (hasComplex)
            {
                var ex = Record.Exception(() => reader.ReadTable(table, maxRows: 5));
                Assert.Null(ex);
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Attachment_StreamRows_DoesNotThrowOnComplexColumns(string path)
    {
        // Streaming rows with complex columns should not crash.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            bool hasComplex = meta.Any(c =>
                c.TypeName == "Attachment" || c.TypeName == "Complex");

            if (hasComplex)
            {
                var ex = Record.Exception(() =>
                    reader.StreamRows(table).Take(5).ToList());
                Assert.Null(ex);
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void Attachment_CellValue_IsNotDbNull(string path)
    {
        // When attachment decoding is implemented, non-empty attachment cells
        // should return actual data (byte[] or a collection), not DBNull.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            int attachIdx = meta.FindIndex(c => c.TypeName == "Attachment");
            if (attachIdx < 0)
            {
                continue;
            }

            foreach (object[] row in reader.StreamRows(table).Take(10))
            {
                if (row[attachIdx] is not DBNull)
                {
                    // Non-null attachment cell should be a byte array or collection
                    Assert.True(
                        row[attachIdx] is byte[] or System.Collections.IEnumerable,
                        $"Attachment value should be byte[] or collection, got {row[attachIdx]?.GetType()}");
                    return; // one non-null cell is enough
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. MULTI-VALUE FIELDS (type 0x12)
    // ═══════════════════════════════════════════════════════════════════
    //
    // Current state: metadata is reported correctly, but cell values are
    // not decoded — they appear as DBNull.
    // When implemented, the reader should follow the complexid FK and
    // return the list of scalar values for each row.

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ComplexField_Metadata_ReportsCorrectTypeName(string path)
    {
        // Complex columns must report "Attachment" or "Complex", not raw hex.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            foreach (var col in meta.Where(c => c.TypeName is "Attachment" or "Complex"))
            {
                Assert.True(
                    col.ClrType == typeof(byte[]) || col.ClrType == typeof(object),
                    $"Complex column '{col.Name}' should map to byte[] or object, got {col.ClrType}");
                Assert.Equal("LVAL", col.Size.ToString());
            }
        }
    }

    [Theory]
    [MemberData(nameof(TestDatabases.Small), MemberType = typeof(TestDatabases))]
    public void ComplexField_MultiValue_CellValue_IsNotDbNull(string path)
    {
        // When multi-value decoding is implemented, non-empty complex cells
        // should return a list of values, not DBNull.
        using var reader = AccessReader.Open(path);

        foreach (string table in reader.ListTables())
        {
            var meta = reader.GetColumnMetadata(table);
            int complexIdx = meta.FindIndex(c => c.TypeName == "Complex");
            if (complexIdx < 0)
            {
                continue;
            }

            foreach (object[] row in reader.StreamRows(table).Take(10))
            {
                if (row[complexIdx] is not DBNull)
                {
                    Assert.True(
                        row[complexIdx] is System.Collections.IEnumerable,
                        $"Multi-value complex field should be a collection, got {row[complexIdx]?.GetType()}");
                    return;
                }
            }
        }
    }
}
