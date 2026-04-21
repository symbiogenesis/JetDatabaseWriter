namespace JetDatabaseReader.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// TDD tests targeting <c>ComplexFields.accdb</c> — an ACCDB fixture created by
/// Access 16 COM automation. It contains:
///   • Table <c>Documents</c> — columns ID (LONG), Title (TEXT), Attachments (ATTACHMENT / 0x12).
///     Two rows inserted, each with one attached .txt file.
///   • Table <c>Tags</c> — plain text column (created for future multi-value testing).
///
/// README limitation: "Complex fields (0x11/0x12) — Metadata reported correctly;
/// cell values not yet decoded."
///
/// ── Important ACE format finding ────────────────────────────────────────────
/// Access 2007+ ACCDB stores Attachment columns as column_type = 0x12 (T_COMPLEX)
/// in the TDEF column descriptor, NOT as 0x11 (T_ATTACHMENT).
/// The specific subtype (Attachment vs. Multi-Value vs. Version History) is
/// stored in the system table <c>MSysComplexColumns</c>.
/// Until the reader consults MSysComplexColumns, it reports TypeName = "Complex"
/// for all complex columns rather than the subtype-specific "Attachment".
///
/// ── What passes today ───────────────────────────────────────────────────────
///   • Safety: file opens, ReadTable / StreamRows don't throw, row count correct.
///   • ClrType = byte[] and Size = LVAL are correctly inferred from type 0x12.
///   • Tags table exists and is readable.
///
/// ── What fails today (TDD red) ──────────────────────────────────────────────
///   • TypeName — reader returns "Complex" (needs MSysComplexColumns lookup).
///   • Cell-value tests — attachment cells return DBNull because the library reads
///     the raw variable-length slot but does not yet resolve the complex_id
///     against the hidden MSysComplexType_Attachment system table.
///
/// When the feature is implemented, all tests in this file should pass green.
/// </summary>
public sealed class ComplexFieldFixtureTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string DocumentsTable = "Documents";
    private const string AttachmentsColumn = "Attachments";

    // ═══════════════════════════════════════════════════════════════════
    // 1. SCHEMA
    // ═══════════════════════════════════════════════════════════════════
    //
    // Most schema tests pass today. The exception is TypeNameIsAttachment:
    // Access stores Attachment columns as 0x12 (Complex) in the TDEF, not
    // 0x11 (Attachment). The reader currently returns TypeName="Complex".
    // Reading MSysComplexColumns is required to return "Attachment".

    [Fact]
    public async Task ComplexFields_DocumentsTable_Exists()
    {
        // The fixture contains a "Documents" table.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains(DocumentsTable, tables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ComplexFields_AttachmentColumn_TypeNameIsAttachment()
    {
        // TDD: Access stores Attachment columns as type 0x12 (T_COMPLEX) in the TDEF.
        // The specific subtype (attachment vs multi-value) is determined by MSysComplexColumns.
        // Until that lookup is implemented, the reader returns TypeName="Complex" (0x12),
        // not "Attachment".  This test will turn green when MSysComplexColumns is consulted.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        ColumnMetadata? col = meta.Find(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        Assert.NotNull(col);
        Assert.Equal("Attachment", col!.TypeName); // TDD red: currently returns "Complex"
    }

    [Fact]
    public async Task ComplexFields_AttachmentColumn_ClrTypeIsByteArray()
    {
        // Attachment columns must map to byte[], not string or object.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        ColumnMetadata? col = meta.Find(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        Assert.NotNull(col);
        Assert.Equal(typeof(byte[]), col!.ClrType);
    }

    [Fact]
    public async Task ComplexFields_AttachmentColumn_SizeIsLval()
    {
        // Attachment (complex) columns have no fixed byte size — they report as LVAL.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        ColumnMetadata? col = meta.Find(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        Assert.NotNull(col);
        Assert.Equal("LVAL", col!.Size.ToString());
    }

    [Fact]
    public async Task ComplexFields_DocumentsTable_HasTwoRows()
    {
        // The fixture was created with exactly two rows.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        int count = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).CountAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, count);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 2. CELL-VALUE DECODING — TDD red until complex-field support is implemented
    // ═══════════════════════════════════════════════════════════════════
    //
    // How attachment storage works (mdbtools HACKING.md, Jackcess docs):
    //   • The variable-length column slot stores a 4-byte "complex_id" integer.
    //   • Actual attachment rows live in a hidden system table named
    //     MSysCM_<tablename>_<fieldname> (e.g. MSysCM_Documents_Attachments).
    //   • Each row in that table holds: complex_id (FK), FileName, FileType, FileData.
    //
    // To decode attachment cells the reader must:
    //   1. Read the 4-byte complex_id from the variable slot.
    //   2. Scan the relevant MSysCM_ table for matching rows.
    //   3. Return a non-null, non-empty value per attachment (byte[], struct, or IList<>).
    //
    // Current behaviour: ReadVar falls through to default (returns ""), which
    // ConvertRowToTyped converts to DBNull.Value. All assertions below therefore
    // currently FAIL (expected TDD red).

    [Fact]
    public async Task ComplexFields_Attachment_Row1_CellValueIsNotDbNull()
    {
        // After decoding, row 1 has one attachment — its cell must not be DBNull.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));
        Assert.True(attachIdx >= 0, $"Column '{AttachmentsColumn}' not found in {DocumentsTable}");

        object[] row1 = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);

        // TDD: currently DBNull; should be decoded attachment data once implemented.
        Assert.True(
            row1[attachIdx] is not DBNull,
            $"Row 1 attachment cell should not be DBNull (complex_id decoding not yet implemented). Got: {row1[attachIdx]}");
    }

    [Fact]
    public async Task ComplexFields_Attachment_Row2_CellValueIsNotDbNull()
    {
        // Row 2 also has one attachment — the decoder must handle multiple rows.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        List<object[]> rows = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).Take(2).ToListAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Count);

        object[] row2 = rows[1];

        // TDD: currently DBNull.
        Assert.True(
            row2[attachIdx] is not DBNull,
            $"Row 2 attachment cell should not be DBNull (complex_id decoding not yet implemented). Got: {row2[attachIdx]}");
    }

    [Fact]
    public async Task ComplexFields_Attachment_AllRows_NonNullCellValues()
    {
        // Every row in the Documents table has an attachment; all cells must be non-null.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        List<object[]> rows = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        Assert.NotEmpty(rows);

        // TDD: currently all cells are DBNull.
        var nullRows = rows.Where(r => r[attachIdx] is DBNull).ToList();
        Assert.Empty(nullRows);
    }

    [Fact]
    public async Task ComplexFields_Attachment_DecodedValue_IsNonEmptyByteArray()
    {
        // When decoded, the attachment value must be a non-empty byte[] (raw LVAL data
        // for the attachment sub-record), OR a richer type that is non-null and non-empty.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        object[] row1 = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);
        object cell = row1[attachIdx];

        // TDD: currently DBNull.
        Assert.NotEqual(DBNull.Value, cell);
        Assert.True(
            cell is byte[] data && data.Length > 0,
            $"Attachment value should be a non-empty byte[]. Got: {cell?.GetType()?.Name ?? "null"}");
    }

    [Fact]
    public async Task ComplexFields_Attachment_Row1_FileNameIsHelloTxt()
    {
        // The fixture was created with "hello.txt" as the attachment filename on row 1.
        // When decoded via the MSysCM_ table lookup, the filename must be recoverable.
        // The expected return type is byte[] containing the raw attachment sub-record,
        // from which the library (or caller) can extract the FileName.
        //
        // For now the test asserts the raw decoded bytes contain the UTF-16 or UTF-8
        // bytes of "hello.txt", as a proxy for correct filename resolution.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        object[] row1 = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);
        object cell = row1[attachIdx];

        // TDD: currently DBNull — will assert filename encoding when byte[] is returned.
        Assert.True(cell is byte[], $"Expected byte[], got {cell?.GetType()?.Name ?? "DBNull"}");
        byte[] decoded = (byte[])cell!;

        // The filename "hello.txt" encoded as UTF-16LE
        byte[] expectedName = System.Text.Encoding.Unicode.GetBytes("hello.txt");

        bool containsFilename =
            ContainsSequence(decoded!, System.Text.Encoding.UTF8.GetBytes("hello.txt")) ||
            ContainsSequence(decoded!, expectedName);

        Assert.True(containsFilename, "Decoded attachment bytes should contain the filename 'hello.txt'");
    }

    [Fact]
    public async Task ComplexFields_Attachment_Row1_FileDataContainsExpectedContent()
    {
        // The attached file "hello.txt" was written with content:
        //   "Hello from attachment fixture!"
        // Once decoded, the raw bytes of that file must be present in the cell value.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(DocumentsTable, TestContext.Current.CancellationToken);
        int attachIdx = meta.FindIndex(c => c.Name.Equals(AttachmentsColumn, StringComparison.OrdinalIgnoreCase));

        object[] row1 = await reader.StreamRowsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).FirstAsync(TestContext.Current.CancellationToken);
        object cell = row1[attachIdx];

        // TDD: currently DBNull.
        Assert.True(cell is byte[], $"Expected byte[], got {cell?.GetType()?.Name ?? "DBNull"}");
        byte[] decoded = (byte[])cell!;

        byte[] expectedContent = System.Text.Encoding.UTF8.GetBytes("Hello from attachment fixture!");
        Assert.True(
            ContainsSequence(decoded!, expectedContent),
            "Decoded attachment bytes should contain the expected file content.");
    }

    [Fact]
    public async Task ComplexFields_Attachment_DataTable_AttachmentColumnIsNotStringType()
    {
        // DataTable conversion must not coerce attachment columns to string.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        TableResult result = await reader.ReadTableAsync(DocumentsTable, 5, TestContext.Current.CancellationToken);
        DataTable dt = result.ToDataTable();

        Assert.NotNull(dt);
        Assert.True(
            dt.Columns.Contains(AttachmentsColumn),
            $"DataTable should contain column '{AttachmentsColumn}'");

        Assert.NotEqual(typeof(string), dt.Columns[AttachmentsColumn]!.DataType);
    }

    [Fact]
    public async Task ComplexFields_Attachment_StreamRowsAsStrings_DoesNotThrow()
    {
        // String streaming on tables with attachment columns must not crash.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        var ex = await Record.ExceptionAsync(async () => await reader.StreamRowsAsStringsAsync(DocumentsTable, cancellationToken: TestContext.Current.CancellationToken).Take(5).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Null(ex);
    }

    // ═══════════════════════════════════════════════════════════════════
    // 3. MULTI-VALUE FIELDS (0x12) — schema tests
    // ═══════════════════════════════════════════════════════════════════
    //
    // The Tags table in ComplexFields.accdb was created with a plain TEXT column
    // (multi-value DDL is not easily accessible via DAO). If a future version of
    // the fixture adds a genuine multi-value column (type 0x12), the tests below
    // will detect it and validate decoding.

    [Fact]
    public async Task ComplexFields_TagsTable_Exists()
    {
        // The Tags table created in the fixture must be visible in ListTables.
        var reader = await db.GetAsync(TestDatabases.ComplexFields);
        List<string> tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("Tags", tables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ComplexFields_AllComplexColumns_TypeNameIsNotRawHex()
    {
        // All complex columns (0x11 / 0x12) across all tables in the fixture must
        // report a friendly TypeName ("Attachment" or "Complex"), not "0x11" / "0x12".
        var reader = await db.GetAsync(TestDatabases.ComplexFields);

        foreach (string table in await reader.ListTablesAsync(TestContext.Current.CancellationToken))
        {
            List<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
            foreach (ColumnMetadata col in meta)
            {
                Assert.False(
                    col.TypeName.StartsWith("0x", StringComparison.Ordinal),
                    $"Column '{col.Name}' in '{table}' reports raw hex TypeName '{col.TypeName}'");
            }
        }
    }

    // ── Private helper ────────────────────────────────────────────────

    private static bool ContainsSequence(byte[] data, byte[] needle)
    {
        if (needle.Length == 0)
        {
            return true;
        }

        for (int i = 0; i <= data.Length - needle.Length; i++)
        {
            bool found = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (data[i + j] != needle[j])
                {
                    found = false;
                    break;
                }
            }

            if (found)
            {
                return true;
            }
        }

        return false;
    }
}
