namespace JetDatabaseWriter.Tests;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated via reflection by RowMapper
#pragma warning disable SA1201 // Nested test POCOs before test methods is standard xUnit convention

/// <summary>
/// Pinning tests for the limitations documented in README "Limitations".
/// Each test asserts a documented limitation is in effect today. They are
/// expected to FAIL the day a limitation is lifted — at which point the
/// README, the test, and (for behavioural pins) the relevant write path
/// must be updated together.
/// Sections mirror the README headings: Schema evolution, Specialized column
/// kinds, Compression on write, Linked tables, Encryption, Forms/reports/
/// macros/queries/VBA, and Concurrency.
/// </summary>
public sealed class LimitationsTests : IDisposable
{
    private readonly List<string> _tempFiles = [];

    // ── Schema evolution ──────────────────────────────────────────────

    [Fact]
    public void SchemaEvolution_IAccessWriter_ExposesAlterTableMethods()
    {
        // Lifted limitation: AddColumnAsync, DropColumnAsync, and RenameColumnAsync are now
        // supported on IAccessWriter / AccessWriter. They are implemented as copy-and-swap.
        Assert.NotNull(typeof(IAccessWriter).GetMethod("AddColumnAsync"));
        Assert.NotNull(typeof(IAccessWriter).GetMethod("DropColumnAsync"));
        Assert.NotNull(typeof(IAccessWriter).GetMethod("RenameColumnAsync"));
        Assert.NotNull(typeof(AccessWriter).GetMethod("AddColumnAsync"));
        Assert.NotNull(typeof(AccessWriter).GetMethod("DropColumnAsync"));
        Assert.NotNull(typeof(AccessWriter).GetMethod("RenameColumnAsync"));
    }

    [Fact]
    public async Task SchemaEvolution_AddColumnAsync_AppendsColumn_ExistingRowsBecomeNull()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

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

    [Fact]
    public async Task SchemaEvolution_DropColumnAsync_RemovesColumn_AndPreservesOtherData()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(2, dt.Columns.Count);
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal("Alice", dt.Rows[0]["Name"]);
        Assert.Equal("Bob", dt.Rows[1]["Name"]);
    }

    [Fact]
    public async Task SchemaEvolution_RenameColumnAsync_RenamesColumn_AndPreservesAllData()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Contacts";

        await using (var writer = await OpenWriterAsync(stream))
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

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;

        Assert.Equal(2, dt.Columns.Count);
        Assert.True(dt.Columns.Contains("Rating"));
        Assert.False(dt.Columns.Contains("Score"));
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(95, dt.Rows[0]["Rating"]);
        Assert.Equal(88, dt.Rows[1]["Rating"]);
    }

    [Fact]
    public async Task SchemaEvolution_DropColumnAsync_LastColumn_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Solo";

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Only", typeof(int))],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.DropColumnAsync(table, "Only", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task SchemaEvolution_AddColumnAsync_DuplicateName_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Dup";

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [new("Id", typeof(int)), new("Name", typeof(string), maxLength: 20)],
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.AddColumnAsync(table, new ColumnDefinition("name", typeof(string), 20), TestContext.Current.CancellationToken));
    }

    [Fact]
    public void SchemaEvolution_IAccessWriter_DoesNotExposeIndexOrConstraintApis()
    {
        // README: "No index, primary-key, foreign-key, or relationship creation."
        AssertNoMethodMatching(typeof(IAccessWriter), "Index");
        AssertNoMethodMatching(typeof(IAccessWriter), "PrimaryKey");
        AssertNoMethodMatching(typeof(IAccessWriter), "ForeignKey");
        AssertNoMethodMatching(typeof(IAccessWriter), "Relationship");
        AssertNoMethodMatching(typeof(AccessWriter), "Index");
        AssertNoMethodMatching(typeof(AccessWriter), "PrimaryKey");
        AssertNoMethodMatching(typeof(AccessWriter), "ForeignKey");
        AssertNoMethodMatching(typeof(AccessWriter), "Relationship");
    }

    [Fact]
    public void SchemaEvolution_ColumnDefinition_ExposesConstraintProperties()
    {
        // Lifted limitation: ColumnDefinition exposes IsNullable, DefaultValue,
        // IsAutoIncrement, and ValidationRule on top of Name/ClrType/MaxLength,
        // plus four persisted properties (DefaultValueExpression, ValidationRuleExpression,
        // ValidationText, Description) round-tripped through MSysObjects.LvProp.
        var publicProps = typeof(ColumnDefinition)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        string[] expected =
        [
            "ClrType",
            "DefaultValue",
            "DefaultValueExpression",
            "Description",
            "IsAutoIncrement",
            "IsNullable",
            "MaxLength",
            "Name",
            "ValidationRule",
            "ValidationRuleExpression",
            "ValidationText",
        ];
        Assert.Equal(expected, publicProps);
    }

    [Fact]
    public void SchemaEvolution_ColumnDefinition_DefaultsAreBackwardCompatible()
    {
        // The new properties must default to "no constraint" so existing callers
        // observe identical behaviour to the pre-feature ColumnDefinition.
        var def = new ColumnDefinition("X", typeof(int));
        Assert.True(def.IsNullable);
        Assert.Null(def.DefaultValue);
        Assert.False(def.IsAutoIncrement);
        Assert.Null(def.ValidationRule);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_DefaultValue_IsAppliedOnInsert()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Defaults";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Score", typeof(int)) { DefaultValue = 42 },
                ],
                TestContext.Current.CancellationToken);

            // Caller passes DBNull for Score → writer should substitute the default.
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken);

            // Caller passes a value → default is NOT applied.
            await writer.InsertRowAsync(table, [2, 7], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(2, dt.Rows.Count);
        Assert.Equal(42, dt.Rows[0]["Score"]);
        Assert.Equal(7, dt.Rows[1]["Score"]);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_NotNull_RejectsMissingValue()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Required";

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            table,
            [
                new("Id", typeof(int)),
                new("Name", typeof(string), maxLength: 50) { IsNullable = false },
            ],
            TestContext.Current.CancellationToken);

        // Supplying DBNull for the required column must throw.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

        // A non-null value is accepted.
        await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_AutoIncrement_AssignsMonotonicValues()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "AutoInc";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            // Pass DBNull for Id → writer assigns 1, 2, 3.
            await writer.InsertRowAsync(table, [DBNull.Value, "A"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [DBNull.Value, "B"], TestContext.Current.CancellationToken);

            // Explicit value is honoured (and may exceed the running counter).
            await writer.InsertRowAsync(table, [100, "C"], TestContext.Current.CancellationToken);

            // Next auto-increment continues from where the counter left off (3 here).
            await writer.InsertRowAsync(table, [DBNull.Value, "D"], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(4, dt.Rows.Count);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal(2, dt.Rows[1]["Id"]);
        Assert.Equal(100, dt.Rows[2]["Id"]);
        Assert.Equal(3, dt.Rows[3]["Id"]);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_NotNull_PersistsAcrossWriterReopen()
    {
        // IsNullable=false is encoded in the JET TDEF column-flag bit FLAG_NULL_ALLOWED (0x02).
        // A second writer must restore the constraint without any in-memory state from the
        // writer that originally declared it.
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Required";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50) { IsNullable = false },
                ],
                TestContext.Current.CancellationToken);
        }

        // Reopen and verify the constraint still throws on a NULL insert.
        await using (var writer = await OpenWriterAsync(stream))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.InsertRowAsync(table, [1, DBNull.Value], TestContext.Current.CancellationToken));

            await writer.InsertRowAsync(table, [2, "Alice"], TestContext.Current.CancellationToken);
        }

        // The reader also surfaces the persisted nullability via ColumnMetadata.
        await using var reader = await OpenReaderAsync(stream);
        var meta = await reader.GetColumnMetadataAsync(table, TestContext.Current.CancellationToken);
        Assert.True(meta[0].IsNullable);
        Assert.False(meta[1].IsNullable);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_AutoIncrement_PersistsAcrossWriterReopen()
    {
        // IsAutoIncrement=true is encoded in the JET TDEF column-flag bit FLAG_AUTO_LONG (0x04).
        // A second writer must continue assigning monotonic values seeded from max(existing) + 1.
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "AutoIncPersist";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                table,
                [
                    new("Id", typeof(int)) { IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(table, [DBNull.Value, "A"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(table, [DBNull.Value, "B"], TestContext.Current.CancellationToken);
        }

        // Reopen and continue inserting — the auto-increment must resume from max+1 = 3.
        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.InsertRowAsync(table, [DBNull.Value, "C"], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(table, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(3, dt.Rows.Count);
        Assert.Equal(1, dt.Rows[0]["Id"]);
        Assert.Equal(2, dt.Rows[1]["Id"]);
        Assert.Equal(3, dt.Rows[2]["Id"]);
    }

    [Fact]
    public async Task SchemaEvolution_ColumnDefinition_ValidationRule_RejectsBadValues()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();
        const string table = "Validated";

        await using var writer = await OpenWriterAsync(stream);
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

    [Fact]
    public async Task SchemaEvolution_FreshlyCreatedTable_HasNoUserDefinedIndexEntries()
    {
        // README: "MSysRelationships and MSysIndexes entries are not written."
        // Open the system catalog after creating a heap table and assert no index
        // rows reference our new table id.
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"NoIdx_{Guid.NewGuid():N}".Substring(0, 18);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Name", typeof(string), maxLength: 50),
                ],
                TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);

        // The freshly created table must not appear in MSysIndexes / MSysRelationships at all.
        // (We deliberately do NOT predicate this on first finding the table in MSysObjects;
        // that lookup is incidental scaffolding. The pin is purely about index/relationship rows.)
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

    // ── Specialized column kinds ──────────────────────────────────────

    [Fact]
    public void SpecializedColumns_NoPublicAttachmentApi()
    {
        // README: "No attachment columns. … CreateTableAsync cannot declare an Attachment column,
        //          and there is no API to add files to one."
        AssertNoMethodMatching(typeof(IAccessWriter), "Attachment");
        AssertNoMethodMatching(typeof(AccessWriter), "Attachment");
        AssertNoMemberMatching(typeof(ColumnDefinition), "Attachment");
    }

    [Fact]
    public void SpecializedColumns_NoPublicMultiValueApi()
    {
        // README: "No multi-value (complex) columns."
        AssertNoMethodMatching(typeof(IAccessWriter), "MultiValue");
        AssertNoMethodMatching(typeof(IAccessWriter), "Complex");
        AssertNoMethodMatching(typeof(AccessWriter), "MultiValue");
        AssertNoMethodMatching(typeof(AccessWriter), "Complex");
        AssertNoMemberMatching(typeof(ColumnDefinition), "MultiValue");
        AssertNoMemberMatching(typeof(ColumnDefinition), "Complex");
    }

    [Fact]
    public void SpecializedColumns_NoCalculatedColumnApi()
    {
        // README: "No calculated columns (Access 2010+ expression columns)."
        // The Jet-expression-string members (DefaultValueExpression / ValidationRuleExpression)
        // are persisted-property strings, not Access-2010 calculated-column formulas.
        AssertNoMemberMatching(typeof(ColumnDefinition), "Calculated");
        AssertNoMemberMatching(typeof(ColumnDefinition), "Formula");
    }

    [Fact]
    public async Task SpecializedColumns_HyperlinkText_RoundTripsAsPlainMemo_NoSemanticParsing()
    {
        // README: "No hyperlink semantics. Hyperlink fields round-trip as plain MEMO text;
        //          the #display#address#subaddress# structure is not parsed or emitted."
        await using var stream = await CreateFreshAccdbStreamAsync();
        string tableName = $"Hyper_{Guid.NewGuid():N}".Substring(0, 18);

        // The string contains the literal Access hyperlink delimiters '#'.
        // A semantics-aware writer would split it into display/address/subaddress
        // and emit a structured value — we expect the writer to do nothing of the kind.
        const string hyperlinkText = "Click me#https://example.com/page#anchor#";

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                tableName,
                [
                    new("Id", typeof(int)),
                    new("Link", typeof(string)), // no maxLength → MEMO
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(tableName, [1, hyperlinkText], TestContext.Current.CancellationToken);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = (await reader.ReadDataTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken))!;
        Assert.Equal(1, dt.Rows.Count);

        // Round-trip is byte-for-byte identical: '#' delimiters preserved verbatim.
        Assert.Equal(hyperlinkText, dt.Rows[0]["Link"]);

        // And the column type is the generic string/MEMO type — there is no
        // dedicated Hyperlink CLR type surfaced by the reader.
        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.Equal(typeof(string), meta[1].ClrType);
    }

    // ── Compression on write ──────────────────────────────────────────

    [Fact]
    public async Task Compression_AsciiInlineText_IsWrittenAsUncompressedUcs2()
    {
        // README: "Strings are written uncompressed. The reader handles JET4
        //          'compressed unicode' (the 0xFF 0xFE marker + 1-byte/2-byte
        //          mode toggle), but the writer always emits full UCS-2."
        //
        // Strategy: insert a unique ASCII sentinel into a fresh ACCDB and verify
        // the bytes appear as UCS-2 (one 0x00 between each ASCII byte). If the
        // writer ever starts emitting the compressed form we'll see the sentinel
        // preceded by the 0xFF 0xFE marker as 1-byte chars instead.
        const string sentinel = "ZZUNIQUEWRITERSENTINELZZ";

        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [
                    new("Id", typeof(int)),
                    new("Txt", typeof(string), maxLength: 100),
                ],
                TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, sentinel], TestContext.Current.CancellationToken);
        }

        byte[] bytes = ms.ToArray();
        byte[] ucs2 = System.Text.Encoding.Unicode.GetBytes(sentinel);
        byte[] ascii = System.Text.Encoding.ASCII.GetBytes(sentinel);

        Assert.True(
            IndexOf(bytes, ucs2) >= 0,
            "Expected the sentinel string to be stored as UCS-2 in the file bytes.");

        // Compressed JET4 strings are prefixed with 0xFF 0xFE followed by the 1-byte form.
        byte[] compressed = new byte[ascii.Length + 2];
        compressed[0] = 0xFF;
        compressed[1] = 0xFE;
        Buffer.BlockCopy(ascii, 0, compressed, 2, ascii.Length);
        Assert.True(
            IndexOf(bytes, compressed) < 0,
            "Sentinel must NOT appear in the JET4 compressed-unicode form (FF FE + 1-byte).");
    }

    // ── Linked tables ─────────────────────────────────────────────────

    [Fact]
    public void LinkedTables_AccessWriter_DoesNotExposePublicCreateLinkedTableApi()
    {
        // README: "No public API to create linked tables. An InsertLinkedTableEntryAsync
        //          helper exists internally for tests but is not part of the public surface."
        var publicCreateLinked = typeof(AccessWriter)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Where(m => m.Name.Contains("LinkedTable", StringComparison.OrdinalIgnoreCase))
            .Where(m => m.Name.StartsWith("Create", StringComparison.OrdinalIgnoreCase) ||
                         m.Name.StartsWith("Insert", StringComparison.OrdinalIgnoreCase) ||
                         m.Name.StartsWith("Add", StringComparison.OrdinalIgnoreCase))
            .ToArray();

        Assert.Empty(publicCreateLinked);
        AssertNoMethodMatching(typeof(IAccessWriter), "LinkedTable");
    }

    // ── Encryption ────────────────────────────────────────────────────

    [Fact]
    public void Encryption_AccessWriter_DoesNotExposePasswordChangeOrReencryptApis()
    {
        // README: "Creating a new encrypted database, changing a password, or
        //          re-encrypting an existing file is not supported."
        AssertNoMethodMatching(typeof(IAccessWriter), "ChangePassword");
        AssertNoMethodMatching(typeof(IAccessWriter), "SetPassword");
        AssertNoMethodMatching(typeof(IAccessWriter), "RemovePassword");
        AssertNoMethodMatching(typeof(IAccessWriter), "Encrypt");
        AssertNoMethodMatching(typeof(IAccessWriter), "Decrypt");
        AssertNoMethodMatching(typeof(AccessWriter), "ChangePassword");
        AssertNoMethodMatching(typeof(AccessWriter), "SetPassword");
        AssertNoMethodMatching(typeof(AccessWriter), "RemovePassword");
        AssertNoMethodMatching(typeof(AccessWriter), "Encrypt");
        AssertNoMethodMatching(typeof(AccessWriter), "Decrypt");
    }

    [Fact]
    public async Task Encryption_CreateDatabaseAsync_ProducesUnencryptedFile_EvenWhenOptionsCarryAPassword()
    {
        // README: "Creating a new encrypted database … is not supported."
        // Demonstrate: pass a password to CreateDatabaseAsync and prove the resulting
        // file is openable WITHOUT any password — i.e. it was never encrypted.
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions("ignoredpassword") { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "T",
                [new("Id", typeof(int))],
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { UseLockFile = false }, // NO password
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Contains("T", tables);
    }

    // ── Forms, reports, macros, queries, VBA ──────────────────────────

    [Fact]
    public void FormsReportsMacrosQueriesVba_NoPublicWriteApi()
    {
        // README: "Out of scope. The library targets the JET storage layer only.
        //          MSysObjects entries of type Form, Report, Macro, Module, or
        //          Query are preserved on disk but are neither parsed nor editable."
        //
        // Pin write/edit-style methods only — we use verb+noun pairs so legitimate
        // members like 'DatabaseFormat' (contains 'Form') don't false-positive.
        string[] verbs = ["Create", "Insert", "Add", "Update", "Drop", "Delete", "Remove", "Edit", "Save", "Compile", "Execute", "Run"];
        string[] nouns = ["Form", "Report", "Macro", "Module", "Query", "Vba"];
        foreach (string verb in verbs)
        {
            foreach (string noun in nouns)
            {
                AssertNoMethodMatching(typeof(IAccessWriter), verb + noun);
                AssertNoMethodMatching(typeof(AccessWriter), verb + noun);
            }
        }
    }

    // ── Concurrency ───────────────────────────────────────────────────

    [Fact]
    public void Concurrency_AccessWriterOptions_RespectsExistingLockFileByDefault()
    {
        // README: "Open with RespectExistingLockFile = true (default) to fail fast
        //          when another process holds the file."
        var defaultOptions = new AccessWriterOptions();
        Assert.True(defaultOptions.RespectExistingLockFile);
        Assert.True(defaultOptions.UseLockFile);
    }

    [Fact]
    public async Task Concurrency_SecondWriter_OnSameFile_FailsFastWhenLockfileExists()
    {
        // README: "Concurrent writers against the same file will corrupt it.
        //          Open with RespectExistingLockFile = true (default) to fail fast."
        string temp = CopyToTemp(TestDatabases.NorthwindTraders);

        await using var first = await AccessWriter.OpenAsync(
            temp,
            new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true },
            TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<IOException>(async () =>
        {
            await using var second = await AccessWriter.OpenAsync(
                temp,
                new AccessWriterOptions { UseLockFile = true, RespectExistingLockFile = true },
                TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public void Concurrency_AccessWriter_DoesNotExposePageOrRowLockingApi()
    {
        // README: "the writer does NOT implement page-level locking."
        AssertNoMethodMatching(typeof(IAccessWriter), "Lock");
        AssertNoMethodMatching(typeof(IAccessWriter), "Transaction");
        AssertNoMethodMatching(typeof(AccessWriter), "BeginTransaction");
        AssertNoMethodMatching(typeof(AccessWriter), "AcquirePageLock");
        AssertNoMethodMatching(typeof(AccessWriter), "AcquireRowLock");
    }

    // ── Helpers ───────────────────────────────────────────────────────

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (string f in _tempFiles)
        {
            try
            {
                if (File.Exists(f))
                {
                    File.Delete(f);
                }

                string lockPath = Path.ChangeExtension(f, Path.GetExtension(f) == ".accdb" ? ".laccdb" : ".ldb");
                if (File.Exists(lockPath))
                {
                    File.Delete(lockPath);
                }
            }
            catch (IOException)
            {
                // best-effort cleanup
            }
        }
    }

    private static void AssertNoMethodMatching(Type type, string substring)
    {
        var matches = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Where(m => m.Name.Contains(substring, StringComparison.OrdinalIgnoreCase))
            .Select(m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        string message = $"Type '{type.FullName}' must not expose any public method whose name contains '{substring}'. Found: {string.Join(", ", matches)}";
        Assert.True(matches.Length == 0, message);
    }

    private static void AssertNoMemberMatching(Type type, string substring)
    {
        var matches = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static)
            .Where(m => m.Name.Contains(substring, StringComparison.OrdinalIgnoreCase))
            .Select(m => m.Name)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        string message = $"Type '{type.FullName}' must not expose any public member whose name contains '{substring}'. Found: {string.Join(", ", matches)}";
        Assert.True(matches.Length == 0, message);
    }

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

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        if (needle.Length == 0 || haystack.Length < needle.Length)
        {
            return -1;
        }

        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            bool found = true;
            for (int j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] != needle[j])
                {
                    found = false;
                    break;
                }
            }

            if (found)
            {
                return i;
            }
        }

        return -1;
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            // Just create the empty database; tests will reopen and add tables.
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

    private string CopyToTemp(string sourcePath)
    {
        string ext = Path.GetExtension(sourcePath);
        string temp = Path.Combine(Path.GetTempPath(), $"JetLimitsTest_{Guid.NewGuid():N}{ext}");
        File.Copy(sourcePath, temp, overwrite: true);
        _tempFiles.Add(temp);
        return temp;
    }
}
