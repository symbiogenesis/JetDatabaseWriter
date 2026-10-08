namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Tests for the complex-columns writer surface
/// (see <see href="docs/design/complex-columns-format-notes.md" />).
/// Fresh writer-created ACCDBs in this class are bootstrap/generated-output
/// subjects under test, not source-of-truth fixtures.
/// <list type="bullet">
///   <item><c>MSysComplexColumns</c> system table is scaffolded into every fresh ACCDB.</item>
///   <item><see cref="ColumnDefinition.IsAttachment"/> / <see cref="ColumnDefinition.IsMultiValue"/>
///         declarations are recognized by <c>CreateTableAsync</c>.</item>
/// </list>
/// </summary>
public sealed class ComplexColumnsWriterTests
{
    /// <summary>Gets the Access-authored ACCDB fixtures with their <c>MSysComplexColumns</c> ComplexID counter.</summary>
    /// <returns>The fixture paths and counters.</returns>
    public static TheoryData<string, int> AccessFixtureComplexIdCounters() => new()
    {
        { TestDatabases.ComplexFields, 1 },

        // Access handed out ComplexIDs up to 7; the largest still in MSysComplexColumns is 4.
        { TestDatabases.ComplexDataTestV2007, 7 },
        { TestDatabases.ComplexDataTestV2010, 7 },
        { TestDatabases.NorthwindTraders, 3 },
    };

    // ── MSysComplexColumns scaffold ────────────────────────────────────────────

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_EmitsMSysComplexColumns()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        string[] names = meta.Select(m => m.Name).ToArray();

        Assert.Contains("ColumnName", names);
        Assert.Contains("ComplexID", names);
        Assert.Contains("ConceptualTableID", names);
        Assert.Contains("FlatTableID", names);
        Assert.Contains("ComplexTypeObjectID", names);
    }

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_MSysComplexColumns_IsHiddenFromUserTables()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> userTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        // The scaffold must set the system flag (0x80000000) so MSysComplexColumns
        // does not appear in the user-table listing.
        Assert.DoesNotContain("MSysComplexColumns", userTables, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task CreateDatabaseAsync_Jet4Mdb_DoesNotEmitMSysComplexColumns()
    {
        // Complex columns are an Access 2007+ ACCDB feature — the system table
        // must not be added to .mdb scaffolds.
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        Assert.False(await reader.TryLookupTableAsync("MSysComplexColumns", TestContext.Current.CancellationToken));
    }

    // ── ColumnDefinition declaration surface ───────────────────────────────────

    [Fact]
    public void ColumnDefinition_Defaults_AreNonComplex()
    {
        var def = new ColumnDefinition("X", typeof(int));

        Assert.False(def.IsAttachment);
        Assert.False(def.IsMultiValue);
        Assert.Null(def.MultiValueElementType);
    }

    [Fact]
    public void ColumnDefinition_AsAttachment_FlagIsSet()
    {
        var def = new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true };

        Assert.True(def.IsAttachment);
        Assert.False(def.IsMultiValue);
    }

    [Fact]
    public void ColumnDefinition_AsMultiValue_CarriesElementType()
    {
        var def = new ColumnDefinition("Tags", typeof(object))
        {
            IsMultiValue = true,
            MultiValueElementType = typeof(string),
        };

        Assert.True(def.IsMultiValue);
        Assert.Equal(typeof(string), def.MultiValueElementType);
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_RoundTripsViaGetComplexColumns()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        ComplexColumnInfo attachment = Assert.Single(info);
        Assert.Equal("Files", attachment.ColumnName);
        Assert.Equal(ComplexColumnKind.Attachment, attachment.Kind);
        Assert.True(attachment.ComplexId > 0);
        Assert.True(attachment.FlatTableId > 0);
        Assert.StartsWith("f_", attachment.FlatTableName, StringComparison.Ordinal);
        Assert.EndsWith("_Files", attachment.FlatTableName, StringComparison.Ordinal);

        await using ReaderHarness pages = await ReaderHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken);
        CatalogEntry? entry = await pages.GetCatalogEntryAsync("Documents", TestContext.Current.CancellationToken);
        Assert.NotNull(entry);
        TableDef? tableDef = await pages.ReadTableDefAsync(entry.TDefPage, TestContext.Current.CancellationToken);
        Assert.NotNull(tableDef);
        ColumnInfo? files = tableDef.FindColumn("Files");
        Assert.NotNull(files);
        Assert.Equal(ColumnType.ComplexType, files.Type);
    }

    [Fact]
    public async Task CreateTableAsync_MultiValueColumn_C3_RoundTripsViaGetComplexColumns()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Things",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(string),
                    },
                ],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Things", TestContext.Current.CancellationToken);
        ComplexColumnInfo mv = Assert.Single(info);
        Assert.Equal("Tags", mv.ColumnName);
        Assert.True(mv.ComplexId > 0);
        Assert.True(mv.FlatTableId > 0);
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_RejectedOnJet4Mdb()
    {
        var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken));

        Assert.Contains(".accdb", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTableAsync_MultiValueColumn_C3_RejectsMissingElementType()
    {
        var ms = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await writer.CreateTableAsync(
                "Bad",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true },
                ],
                TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C3_HiddenFlatTablePresent()
    {
        // The hidden flat child table must carry MSysObjects.Flags = 0x800A0000 so
        // it is excluded from the user-table listing but reachable via direct lookup.
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> userTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("Documents", userTables, StringComparer.OrdinalIgnoreCase);
        Assert.DoesNotContain(userTables, t => t.StartsWith("f_", StringComparison.Ordinal));

        // The flat table has the per-kind value columns from the design doc §2.4.1.
        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Documents", TestContext.Current.CancellationToken);
        ComplexColumnInfo attachment = Assert.Single(info);
        IReadOnlyList<ColumnMetadata> flatMeta = await reader.GetColumnMetadataAsync(attachment.FlatTableName, TestContext.Current.CancellationToken);
        string[] names = flatMeta.Select(m => m.Name).ToArray();
        Assert.Contains("FileURL", names);
        Assert.Contains("FileName", names);
        Assert.Contains("FileType", names);
        Assert.Contains("FileFlags", names);
        Assert.Contains("FileTimeStamp", names);
        Assert.Contains("FileData", names);
    }

    [Fact]
    public async Task GetComplexColumns_OnFreshAccdb_ReturnsEmpty()
    {
        // Empty ACCDB has the MSysComplexColumns scaffold, but there are no
        // user-table complex columns yet.
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        await using AccessWriter writer = await AccessWriter.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            "Plain",
            [
                new ColumnDefinition("Id", typeof(int)),
            ],
            TestContext.Current.CancellationToken);

        IReadOnlyList<ComplexColumnInfo> info = await reader.GetComplexColumnsAsync("Plain", TestContext.Current.CancellationToken);
        Assert.Empty(info);
    }

    // ── MSysComplexType_* template tables ────────────────────────────────────────

    private static readonly string[] ExpectedTemplateNames =
    [
        "MSysComplexType_UnsignedByte",
        "MSysComplexType_Short",
        "MSysComplexType_Long",
        "MSysComplexType_IEEESingle",
        "MSysComplexType_IEEEDouble",
        "MSysComplexType_GUID",
        "MSysComplexType_Decimal",
        "MSysComplexType_Text",
        "MSysComplexType_Attachment",
    ];

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_FullCatalog_EmitsAllNineComplexTypeTemplates()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        foreach (string template in ExpectedTemplateNames)
        {
            IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync(template, TestContext.Current.CancellationToken);
            Assert.NotEmpty(meta);
        }
    }

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_ComplexTypeTemplates_AreHiddenFromUserTables()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<string> userTables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        foreach (string template in ExpectedTemplateNames)
        {
            Assert.DoesNotContain(template, userTables, StringComparer.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public async Task CreateDatabaseAsync_AceAccdb_ComplexTypeAttachmentTemplate_HasSixColumns()
    {
        // Per the docs/design appendix, MSysComplexType_Attachment has the same
        // six value columns the hidden flat-attachment-table carries.
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> meta = await reader.GetColumnMetadataAsync("MSysComplexType_Attachment", TestContext.Current.CancellationToken);
        string[] names = meta.Select(m => m.Name).ToArray();
        Assert.Contains("FileData", names);
        Assert.Contains("FileFlags", names);
        Assert.Contains("FileName", names);
        Assert.Contains("FileTimeStamp", names);
        Assert.Contains("FileType", names);
        Assert.Contains("FileURL", names);
    }

    [Fact]
    public async Task CreateDatabaseAsync_Jet4Mdb_DoesNotEmitComplexTypeTemplates()
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        foreach (string template in ExpectedTemplateNames)
        {
            Assert.False(await reader.TryLookupTableAsync(template, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task CreateTableAsync_AttachmentColumn_C10_ComplexTypeObjectIdIsNonZero()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        // The MSysComplexColumns row for "Files" must reference a real template id
        // (>0) instead of a placeholder 0.
        DataTable cx = await reader.ReadTableAsync("MSysComplexColumns", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        DataRow row = Assert.Single(
            cx.Rows.Cast<DataRow>(),
            r => string.Equals(
                Convert.ToString(r["ColumnName"], CultureInfo.InvariantCulture),
                "Files",
                StringComparison.OrdinalIgnoreCase));
        int actual = Convert.ToInt32(row["ComplexTypeObjectID"], CultureInfo.InvariantCulture);
        Assert.True(actual > 0, $"Expected ComplexTypeObjectID > 0, got {actual}.");

        // The id is a TDEF page; verify the page belongs to MSysComplexType_Attachment
        // by hitting the table by name (only matches if the template table exists at
        // that page).
        IReadOnlyList<ColumnMetadata> tplMeta = await reader.GetColumnMetadataAsync("MSysComplexType_Attachment", TestContext.Current.CancellationToken);
        Assert.NotEmpty(tplMeta);
    }

    [Fact]
    public async Task CreateTableAsync_MultiValueStringColumn_C10_ComplexTypeObjectIdIsNonZero()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Things",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Tags", typeof(object))
                    {
                        IsMultiValue = true,
                        MultiValueElementType = typeof(string),
                    },
                ],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        DataTable cx = await reader.ReadTableAsync("MSysComplexColumns", cancellationToken: TestContext.Current.CancellationToken);
        Assert.NotNull(cx);
        DataRow row = Assert.Single(
            cx.Rows.Cast<DataRow>(),
            r => string.Equals(
                Convert.ToString(r["ColumnName"], CultureInfo.InvariantCulture),
                "Tags",
                StringComparison.OrdinalIgnoreCase));
        int actual = Convert.ToInt32(row["ComplexTypeObjectID"], CultureInfo.InvariantCulture);
        Assert.True(actual > 0, $"Expected ComplexTypeObjectID > 0, got {actual}.");

        IReadOnlyList<ColumnMetadata> tplMeta = await reader.GetColumnMetadataAsync("MSysComplexType_Text", TestContext.Current.CancellationToken);
        Assert.NotEmpty(tplMeta);
    }

    // ── MSysComplexColumns.ComplexID AutoNumber ───────────────────────────────
    // ComplexID carries the AutoNumber flag (0x17), and its TDEF counter at
    // offset 20 is the last ComplexID handed out (1/7/7/3 in the Access
    // fixtures ComplexFields, complexDataTest V2007/V2010 and Northwind).
    // New ComplexIDs continue from it and keep it current.

    [Theory]
    [MemberData(nameof(ComplexColumnTestSupport.AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateTable_ComplexColumns_RaisesComplexIdCounter(WriteMode mode)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(ms, mode))
        {
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () => await writer.CreateTableAsync("Docs", DocsColumns(), TestContext.Current.CancellationToken));
        }

        int[] ids = await ReadComplexIdsAsync(ms, "Docs");
        Assert.Equal([1, 2], ids);
        Assert.Equal(2, await ReadComplexIdCounterAsync(ms));
    }

    [Theory]
    [MemberData(nameof(ComplexColumnTestSupport.AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task DropThenAddComplexColumn_DoesNotReuseComplexId(WriteMode mode)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(ms, mode))
        {
            await writer.CreateTableAsync("Docs", DocsColumns(), TestContext.Current.CancellationToken);
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () => await writer.DropColumnAsync("Docs", "Tags", TestContext.Current.CancellationToken));
        }

        // A later session continues from the persisted counter, not from the
        // largest ComplexID still in MSysComplexColumns.
        await using (AccessWriter writer = await ComplexColumnTestSupport.OpenWriterAsync(ms, mode))
        {
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () => await writer.AddColumnAsync("Docs", new ColumnDefinition("More", typeof(byte[])) { IsAttachment = true }, TestContext.Current.CancellationToken));
        }

        int[] ids = await ReadComplexIdsAsync(ms, "Docs");
        Assert.Equal([1, 3], ids);
        Assert.Equal(3, await ReadComplexIdCounterAsync(ms));
    }

    [Theory]
    [MemberData(nameof(AccessFixtureComplexIdCounters))]
    public async Task CreateTable_AccessFixture_ContinuesFromComplexIdCounter(string path, int counter)
    {
        await using MemoryStream ms = await ComplexColumnTestSupport.CopyFixtureAsync(path);
        Assert.Equal(counter, await ReadComplexIdCounterAsync(ms));

        await using (AccessWriter writer = await ComplexColumnTestSupport.OpenWriterAsync(ms))
        {
            await writer.CreateTableAsync(
                "Extra",
                [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                TestContext.Current.CancellationToken);
        }

        int[] ids = await ReadComplexIdsAsync(ms, "Extra");
        Assert.Equal([counter + 1], ids);
        Assert.Equal(counter + 1, await ReadComplexIdCounterAsync(ms));
    }

    [Theory]
    [InlineData(WriteMode.Direct, 0)]
    [InlineData(WriteMode.AutoCommit, 0)]
    [InlineData(WriteMode.ExplicitCommit, 0)]
    [InlineData(WriteMode.Direct, 1)]
    [InlineData(WriteMode.AutoCommit, 1)]
    [InlineData(WriteMode.ExplicitCommit, 1)]
    public async Task CreateTableAndAddColumn_StaleComplexIdCounter_RefuseWithoutRepair(WriteMode mode, int counter)
    {
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(ms))
        {
            await writer.CreateTableAsync("Docs", DocsColumns(), TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken))
        {
            long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
            byte[] tdef = await harness.Pager.ReadPageCopyAsync(page, TestContext.Current.CancellationToken);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(tdef.AsSpan(ComplexColumnTestSupport.AutoNumberOffset, 4), counter);
            await harness.Pager.WritePageAsync(page, tdef, TestContext.Current.CancellationToken);
        }

        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await ComplexColumnTestSupport.OpenWriterAsync(ms, mode))
        {
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException createError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.CreateTableAsync("Extra", DocsColumns(), TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, createError.ErrorCode);
                Assert.Equal("MSysComplexColumns", createError.ErrorInfo.TableName);
                Assert.Equal("ComplexID", createError.ErrorInfo.ColumnName);
                Assert.True(createError.ErrorInfo.PageNumber > 0);
                Assert.Contains("counter", createError.Message, StringComparison.OrdinalIgnoreCase);

                JetCorruptDataException addError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
                    await writer.AddColumnAsync("Docs", new ColumnDefinition("More", typeof(byte[])) { IsAttachment = true }, TestContext.Current.CancellationToken));
                Assert.Equal(JetErrorCode.CorruptComplexColumn, addError.ErrorCode);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
        int[] ids = await ReadComplexIdsAsync(ms, "Docs");
        Assert.Equal([1, 2], ids);
        Assert.Equal(counter, await ReadComplexIdCounterAsync(ms));
    }

    [Theory]
    [MemberData(nameof(ComplexColumnTestSupport.AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateTable_LastAvailableComplexId_IsAllocated(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (await ComplexColumnTestSupport.CreateWriterAsync(ms))
        {
        }

        await WriteComplexIdCounterAsync(ms, int.MaxValue - 1);
        await using (AccessWriter writer = await ComplexColumnTestSupport.OpenWriterAsync(ms, mode))
        {
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () =>
                await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], TestContext.Current.CancellationToken));
        }

        int[] ids = await ReadComplexIdsAsync(ms, "Docs");
        Assert.Equal([int.MaxValue], ids);
        Assert.Equal(int.MaxValue, await ReadComplexIdCounterAsync(ms));
    }

    [Theory]
    [MemberData(nameof(ComplexColumnTestSupport.AllModes), MemberType = typeof(ComplexColumnTestSupport))]
    public async Task CreateTable_ComplexIdBatchExceedsCapacity_RefusesWithoutWriting(WriteMode mode)
    {
        await using var ms = new MemoryStream();
        await using (await ComplexColumnTestSupport.CreateWriterAsync(ms))
        {
        }

        await WriteComplexIdCounterAsync(ms, int.MaxValue - 1);
        byte[] baseline = ms.ToArray();
        await using (AccessWriter writer = await ComplexColumnTestSupport.OpenWriterAsync(ms, mode))
        {
            await ComplexColumnTestSupport.RunAsync(writer, mode, async () =>
            {
                InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                    await writer.CreateTableAsync("Docs", DocsColumns(), TestContext.Current.CancellationToken));
                Assert.Contains("MSysComplexColumns", error.Message, StringComparison.Ordinal);
                Assert.Contains("ComplexID", error.Message, StringComparison.Ordinal);
            });
        }

        Assert.Equal(baseline, ms.ToArray());
        Assert.Equal(int.MaxValue - 1, await ReadComplexIdCounterAsync(ms));
    }

    [Fact]
    public async Task CreateTable_RolledBackTransaction_RestoresComplexIdCounter()
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Docs", DocsColumns(), TestContext.Current.CancellationToken);

            await using (JetTransaction tx = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Other", DocsColumns(), TestContext.Current.CancellationToken);
                await tx.RollbackAsync(TestContext.Current.CancellationToken);
            }

            await writer.CreateTableAsync("Third", DocsColumns(), TestContext.Current.CancellationToken);
        }

        int[] ids = await ReadComplexIdsAsync(ms, "Third");
        Assert.Equal([3, 4], ids);
        Assert.Equal(4, await ReadComplexIdCounterAsync(ms));
    }

    private static List<ColumnDefinition> DocsColumns() =>
    [
        new ColumnDefinition("Id", typeof(int)),
        new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
        new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(int) },
    ];

    private static async Task<int[]> ReadComplexIdsAsync(MemoryStream ms, string table)
    {
        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(ms, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        return [.. (await reader.GetComplexColumnsAsync(table, TestContext.Current.CancellationToken)).Select(c => c.ComplexId).Order()];
    }

    private static async Task WriteComplexIdCounterAsync(MemoryStream ms, int counter)
    {
        ms.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: TestContext.Current.CancellationToken);
        long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
        byte[] tdef = await harness.Pager.ReadPageCopyAsync(page, TestContext.Current.CancellationToken);
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(tdef.AsSpan(ComplexColumnTestSupport.AutoNumberOffset, 4), counter);
        await harness.Pager.WritePageAsync(page, tdef, TestContext.Current.CancellationToken);
    }

    private static async Task<int> ReadComplexIdCounterAsync(MemoryStream ms) =>
        ComplexColumnTestSupport.ReadInt32(
            await ComplexColumnTestSupport.ReadSystemTdefAsync(ms, "MSysComplexColumns"),
            ComplexColumnTestSupport.AutoNumberOffset);
}
