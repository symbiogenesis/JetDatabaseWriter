namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>
/// A table's persisted properties are read from the <c>MSysObjects</c> row
/// whose <c>Id</c> is exactly the table's TDEF page, as the stored bytes of its
/// <c>LvProp</c> column. The reader's metadata, the writer's constraint loading
/// and the schema rewrite share that read.
/// </summary>
public sealed class LvPropReadTests
{
    private const long LowIdBits = 0xFFFFFF;

    private static readonly string[] SignatureDescriptions = ["BMI of patient", "%PDF scan", "GIF89a logo", "{\\rtf1 note"];

    /// <summary>The schema rewrite a test runs.</summary>
    [SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
    public enum Rewrite
    {
        /// <summary><c>AddColumnAsync</c> of a new column.</summary>
        AddColumn = 0,

        /// <summary><c>DropColumnAsync</c> of another column.</summary>
        DropOtherColumn = 1,

        /// <summary><c>RenameColumnAsync</c> of another column.</summary>
        RenameOtherColumn = 2,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every writer-created format with every description that holds a file signature.</summary>
    /// <returns>The format and description pairs.</returns>
    public static TheoryData<DatabaseFormat, string> FormatsAndDescriptions()
    {
        var data = new TheoryData<DatabaseFormat, string>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (string description in SignatureDescriptions)
            {
                data.Add(format, description);
            }
        }

        return data;
    }

    /// <summary>Gets every writer-created format in every write mode.</summary>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes()
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    /// <summary>Gets every writer-created format, rewrite and write mode.</summary>
    /// <returns>The format, rewrite and write-mode triples.</returns>
    public static TheoryData<DatabaseFormat, Rewrite, WriteMode> FormatsRewritesAndModes()
    {
        var data = new TheoryData<DatabaseFormat, Rewrite, WriteMode>();
        foreach (DatabaseFormat format in Formats)
        {
            foreach (Rewrite rewrite in Enum.GetValues<Rewrite>())
            {
                foreach (WriteMode mode in new[] { WriteMode.Direct, WriteMode.AutoCommit, WriteMode.ExplicitCommit })
                {
                    data.Add(format, rewrite, mode);
                }
            }
        }

        return data;
    }

    /// <summary>
    /// Gets the Access-authored tables whose <c>MSysObjects</c> Id shares its low
    /// 24 bits with an earlier catalog row: a form, report, module or query whose
    /// Id has the high bit set.
    /// </summary>
    /// <returns>The fixture and table pairs.</returns>
    public static TheoryData<string, string> CollidingTables()
    {
        var data = new TheoryData<string, string>();
        foreach (string table in new[] { "Companies", "CompanyTypes", "EmployeePrivileges", "Catalog_TableOfContents", "Contacts", "Employees" })
        {
            data.Add(TestDatabases.NorthwindTraders, table);
        }

        data.Add(TestDatabases.OdbcLinkerTestV2007, "Lokal");
        data.Add(TestDatabases.MdbtoolsNwind, "Categories");
        return data;
    }

    /// <summary>Gets every Access-authored fixture the reader can open.</summary>
    /// <returns>The fixture paths.</returns>
    public static TheoryData<string> AccessAuthoredFixtures()
    {
        var data = new TheoryData<string>();
        IEnumerable<string> paths = new[] { TestDatabases.NorthwindTraders, TestDatabases.AdventureWorks, TestDatabases.Jet3Test, TestDatabases.ComplexFields }
            .Concat(TestDatabases.AllJackcessDatabases)
            .Concat([TestDatabases.MdbtoolsNwind, TestDatabases.MdbtoolsASampleDatabase, TestDatabases.MdbtoolsDateTestDatabase]);
        foreach (string path in paths.Where(TestDatabases.IsReadable))
        {
            data.Add(path);
        }

        return data;
    }

    private static DatabaseFormat[] Formats => [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    /// <summary>
    /// A Jet3 LvProp blob is code-page text, so a description that starts with
    /// a file signature ("BM", "%PDF", "GIF", "{\rt") used to be taken for an
    /// embedded file, and the whole blob, defaults and rules included, was lost.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="description">The column description.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndDescriptions))]
    public async Task ColumnMetadata_DescriptionContainsFileSignature_ReturnsStoredProperties(DatabaseFormat format, string description)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), ScoreColumn(description)], Ct);
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        ColumnMetadata score = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), c => c.Name == "Score");
        Assert.Equal(description, score.Description);
        Assert.Equal("0", score.DefaultValueExpression);
        Assert.Equal(">=0", score.ValidationRuleExpression);
        Assert.Equal("Score must not be negative", score.ValidationText);
    }

    /// <summary>
    /// A later writer loads the column's rule from the stored properties, so it
    /// rejects a value the rule forbids whatever the description holds.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode">How the later writer runs the inserts.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRow_DescriptionContainsFileSignature_EnforcesStoredValidationRule(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            for (int i = 0; i < SignatureDescriptions.Length; i++)
            {
                await writer.CreateTableAsync($"T{i}", [new("Id", typeof(int)), ScoreColumn(SignatureDescriptions[i])], Ct);
            }
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                for (int i = 0; i < SignatureDescriptions.Length; i++)
                {
                    JetValidationRuleException rejected = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync($"T{i}", [1, -5d], Ct));
                    Assert.Contains("Score must not be negative", rejected.Message, StringComparison.Ordinal);
                    await writer.InsertRowAsync($"T{i}", new RowValues { ["Id"] = 2 }, Ct);
                }
            });
        }

        await using AccessReader reader = await OpenReaderAsync(ms);
        for (int i = 0; i < SignatureDescriptions.Length; i++)
        {
            object[] row = Assert.Single(await ReadRowsAsync(reader, $"T{i}"));
            Assert.Equal(2, row[0]);
            Assert.Equal(0d, Convert.ToDouble(row[1], CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// AddColumn, DropColumn and RenameColumn rebuild the table's properties from
    /// what they read, so a lost read used to drop the column's description,
    /// default and rule.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="rewrite">The schema rewrite.</param>
    /// <param name="mode">How the writer runs the rewrite.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsRewritesAndModes))]
    public async Task SchemaRewrite_DescriptionContainsFileSignature_KeepsColumnProperties(DatabaseFormat format, Rewrite rewrite, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), ScoreColumn("%PDF scan"), new("Note", typeof(string), maxLength: 20)], Ct);
            await writer.InsertRowAsync("T", [1, 3d, "n"], Ct);
        }

        await using (AccessWriter writer = await OpenWriterAsync(ms, mode))
        {
            await RunAsync(writer, mode, async () =>
            {
                switch (rewrite)
                {
                    case Rewrite.AddColumn:
                        await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
                        break;
                    case Rewrite.DropOtherColumn:
                        await writer.DropColumnAsync("T", "Note", Ct);
                        break;
                    case Rewrite.RenameOtherColumn:
                        await writer.RenameColumnAsync("T", "Note", "Remark", Ct);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(rewrite));
                }
            });
        }

        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            ColumnMetadata score = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), c => c.Name == "Score");
            Assert.Equal("%PDF scan", score.Description);
            Assert.Equal("0", score.DefaultValueExpression);
            Assert.Equal(">=0", score.ValidationRuleExpression);
        }

        await using AccessWriter later = await OpenWriterAsync(ms, WriteMode.Direct);
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await later.InsertRowAsync("T", new RowValues { ["Id"] = 2, ["Score"] = -1d }, Ct));
    }

    /// <summary>
    /// A table's Id can share its low 24 bits with an earlier catalog row (a form,
    /// report, module or query, whose Id has the high bit set). The table's
    /// properties come from its own row, the one whose Id is the TDEF page.
    /// </summary>
    /// <param name="fixture">The Access-authored fixture.</param>
    /// <param name="table">The table whose Id collides.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(CollidingTables))]
    public async Task ReadLvProp_TableIdSharesLow24BitsWithAnotherObject_ReturnsOwnBlob(string fixture, string table)
    {
        Assert.SkipUnless(await TestDatabases.IsReadableAsync(fixture, Ct), $"Fixture not readable: {fixture}");

        await using MemoryStream ms = await CopyFixtureAsync(fixture);
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync(table, Ct));
        IReadOnlyList<RawCatalogRow> rows = await ReadRawCatalogRowsAsync(harness.Database);

        Assert.Contains(rows, r => r.Id != entry.TDefPage && (r.Id & LowIdBits) == entry.TDefPage);
        RawCatalogRow own = Assert.Single(rows, r => r.Id == entry.TDefPage);
        ColumnPropertyBlock oracle = Assert.IsType<ColumnPropertyBlock>(ColumnPropertyBlock.Parse(await own.ReadLvPropAsync(harness.Database), harness.Database.Format));

        ColumnPropertyBlock? read = await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct);
        Assert.Equal(Describe(oracle), Describe(read));

        await using AccessReader reader = await OpenReaderAsync(ms);
        foreach (ColumnMetadata column in await reader.GetColumnMetadataAsync(table, Ct))
        {
            Assert.Equal(
                oracle.FindTarget(column.Name)?.GetTextValue(Constants.ColumnPropertyNames.Description, harness.Database.Format),
                column.Description);
        }
    }

    /// <summary>
    /// Every table of every Access-authored fixture reads the same properties as
    /// the earlier string decode of <c>MSysObjects</c>, kept in this test, except
    /// where that decode matched a row by its Id's low 24 bits only, or took the
    /// blob for a file because it holds a file signature. Those differences are
    /// intended, and the read then returns the table's own stored blob.
    /// </summary>
    /// <param name="fixture">The fixture path.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(AccessAuthoredFixtures))]
    public async Task ReadLvProp_EveryFixtureTable_MatchesLegacyStringPath(string fixture)
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(fixture, cancellationToken: Ct);
        (List<string> collisions, _) = await CompareWithLegacyStringPathAsync(harness);

        if (fixture == TestDatabases.NorthwindTraders)
        {
            Assert.Superset(
                new HashSet<string>(["Companies", "CompanyTypes", "EmployeePrivileges", "Catalog_TableOfContents", "Contacts", "Employees"]),
                new HashSet<string>(collisions));
        }
    }

    /// <summary>
    /// The same comparison on a writer-created table whose description holds a
    /// file signature. On Jet3 the earlier decode found the signature in the
    /// code-page text of the blob and dropped it, while the read returns it; on
    /// Jet4 and ACCDB the blob is UTF-16 and both reads agree.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="description">The column description.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndDescriptions))]
    public async Task ReadLvProp_DescriptionContainsFileSignature_DiffersFromLegacyStringPathOnJet3Only(DatabaseFormat format, string description)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), ScoreColumn(description)], Ct);
        }

        ms.Position = 0;
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: Ct);
        (List<string> collisions, List<string> signatures) = await CompareWithLegacyStringPathAsync(harness);

        string[] expectedSignatures = format == DatabaseFormat.Jet3Mdb ? ["T"] : [];
        Assert.Empty(collisions);
        Assert.Equal(expectedSignatures, signatures);
    }

    /// <summary>
    /// A blob too long for one LVAL row is stored as an LVAL chain, and is read
    /// whole: testV2003's Table2 holds 16,175 bytes for 90 targets.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ReadLvProp_ChainedLvalBlob_ReadsExactBytes()
    {
        Assert.SkipUnless(await TestDatabases.IsReadableAsync(TestDatabases.TestV2003, Ct), "testV2003 is not readable.");

        await using MemoryStream ms = await CopyFixtureAsync(TestDatabases.TestV2003);
        await using WriterHarness harness = await WriterHarness.OpenAsync(ms, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await harness.Services.Catalog.GetCatalogEntryAsync("Table2", Ct));
        RawCatalogRow own = Assert.Single(await ReadRawCatalogRowsAsync(harness.Database), r => r.Id == entry.TDefPage);
        byte[] blob = Assert.IsType<byte[]>(await own.ReadLvPropAsync(harness.Database));

        Assert.Equal(0x00, own.StorageMode);
        Assert.Equal(16_175, blob.Length);
        ColumnPropertyBlock? read = await harness.Services.Snapshots.ReadLvPropBlockAsync(entry.TDefPage, Ct);
        Assert.Equal(90, read?.Targets.Count);
        Assert.Equal(Describe(ColumnPropertyBlock.Parse(blob, harness.Database.Format)), Describe(read));
    }

    /// <summary>
    /// Inside an explicit transaction a second rewrite reads the properties the
    /// first one wrote, which are still pending, so the added column keeps its
    /// description and rule through the second rewrite.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Fact]
    public async Task ReadLvProp_InsideTransaction_SeesJournaledRow()
    {
        await using MemoryStream ms = await CreateDatabaseAsync(DatabaseFormat.AceAccdb);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("Note", typeof(string), maxLength: 20)], Ct);
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.AddColumnAsync("T", ScoreColumn("GIF89a logo"), Ct);
            await writer.RenameColumnAsync("T", "Note", "Remark", Ct);
            await transaction.CommitAsync(Ct);
        }

        await using (AccessReader reader = await OpenReaderAsync(ms))
        {
            ColumnMetadata score = Assert.Single(await reader.GetColumnMetadataAsync("T", Ct), c => c.Name == "Score");
            Assert.Equal("GIF89a logo", score.Description);
            Assert.Equal(">=0", score.ValidationRuleExpression);
        }

        await using AccessWriter later = await OpenWriterAsync(ms, WriteMode.Direct);
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await later.InsertRowAsync("T", [1, "r", -2d], Ct));
    }

    /// <summary>A lookup must not read an unrelated corrupt property chain.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>A task representing the asynchronous check.</returns>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReadLvProp_DoesNotReadEarlierUnrelatedChains(DatabaseFormat format)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter writer = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await writer.CreateTableAsync("Earlier", [new ColumnDefinition("Id", typeof(int)) { Description = new string('x', 10_000) }], Ct);
            await writer.CreateTableAsync("Wanted", [new ColumnDefinition("Id", typeof(int)) { Description = "Selected property" }], Ct);
        }

        await PatchLvPropAsync(ms, "Earlier", breakChain: true);
        await using AccessReader reader = await OpenReaderAsync(ms);
        ColumnMetadata column = Assert.Single(await reader.GetColumnMetadataAsync("Wanted", Ct));
        Assert.Equal("Selected property", column.Description);
    }

    /// <summary>A present unreadable property block prevents every schema rewrite before mutation.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="rewrite">The requested rewrite.</param>
    /// <param name="mode">The write mode.</param>
    /// <returns>A task representing the asynchronous check.</returns>
    [Theory]
    [MemberData(nameof(FormatsRewritesAndModes))]
    public async Task MalformedPresentLvProp_RefusesRewriteWithoutChangingBytes(DatabaseFormat format, Rewrite rewrite, WriteMode mode)
    {
        await using MemoryStream ms = await CreateDatabaseAsync(format);
        await using (AccessWriter initial = await OpenWriterAsync(ms, WriteMode.Direct))
        {
            await initial.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { Description = "Stored rule" }, new("Note", typeof(string), maxLength: 20)], Ct);
        }

        await PatchLvPropAsync(ms, "T", breakChain: false);
        byte[] before = ms.ToArray();
        await using AccessWriter writer = await OpenWriterAsync(ms, mode);
        await Assert.ThrowsAsync<JetCorruptDataException>(() => RunAsync(writer, mode, async () =>
        {
            switch (rewrite)
            {
                case Rewrite.AddColumn:
                    await writer.AddColumnAsync("T", new("New", typeof(int)), Ct);
                    break;
                case Rewrite.DropOtherColumn:
                    await writer.DropColumnAsync("T", "Note", Ct);
                    break;
                case Rewrite.RenameOtherColumn:
                    await writer.RenameColumnAsync("T", "Note", "Renamed", Ct);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(rewrite));
            }
        }));
        Assert.Equal(before, ms.ToArray());
    }

    private static async Task PatchLvPropAsync(MemoryStream stream, string tableName, bool breakChain)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        TableDef catalog = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, Ct));
        ColumnInfo nameColumn = Assert.IsType<ColumnInfo>(catalog.FindColumn("Name"));
        ColumnInfo propertyColumn = Assert.IsType<ColumnInfo>(catalog.FindColumn("LvProp"));
        bool found = false;
        await harness.Database.OwnedPages.ForEachLiveTableRowAsync(
            2,
            async (row, token) =>
            {
                string name = ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, row.Page, row.Location.RowStart, row.Location.RowSize, nameColumn);
                if (!string.Equals(name, tableName, StringComparison.Ordinal))
                {
                    return true;
                }

                Assert.True(RowDecodePlan.TryParseRowLayout(harness.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, catalog.HasVarColumns, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(harness.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, propertyColumn);
                Assert.Equal(ColumnSliceKind.Var, slice.Kind);
                byte[] patched = (byte[])row.Page.Clone();
                int start = row.Location.RowStart + slice.DataStart;
                if (breakChain)
                {
                    Assert.NotEqual(0x80, patched[start + 3] & 0xC0);
                    patched.AsSpan(start + 4, 4).Fill(0xFF);
                }
                else
                {
                    Assert.True(LongValueDescriptor.TryRead(patched.AsSpan(start, slice.DataLen), out LongValueDescriptor descriptor));
                    if (descriptor.IsInline)
                    {
                        patched[start + Constants.LongValue.HeaderSize] ^= 0x01;
                    }
                    else
                    {
                        using var pages = new ReaderPageCache(harness.Database.Format, harness.Database.Pages, capacity: 0);
                        var decoder = new LongValueDecoder(harness.Database.Format, pages);
                        LvalRowLocation location = await decoder.LocateLvalRowAsync(descriptor.FirstDp, token);
                        Assert.False(location.Failed, location.Error);
                        int prefixLength = descriptor.IsSinglePage ? 0 : sizeof(uint);
                        Assert.True(location.Size >= prefixLength + sizeof(uint));
                        byte[] payloadPage = (byte[])location.Page.Clone();
                        payloadPage[location.Start + prefixLength] ^= 0x01;
                        await harness.Pager.WritePageAsync(LongValueStore.PageNumber(descriptor.FirstDp), payloadPage, token);
                    }

                }

                await harness.Pager.WritePageAsync(row.Location.DataPageNumber, patched, token);
                found = true;
                return false;
            },
            Ct);
        Assert.True(found);
        await harness.Pager.FlushAsync(toDisk: false, Ct);
    }

    private static ColumnDefinition ScoreColumn(string description) => new("Score", typeof(double))
    {
        Description = description,
        DefaultValueExpression = "0",
        ValidationRuleExpression = ">=0",
        ValidationText = "Score must not be negative",
    };

    /// <summary>
    /// Renders a property block as one line per entry and unknown chunk, so two
    /// blocks compare by their targets, entries and values.
    /// </summary>
    /// <param name="block">The block, or <see langword="null"/>.</param>
    private static List<string> Describe(ColumnPropertyBlock? block)
    {
        var lines = new List<string>();
        if (block is null)
        {
            return lines;
        }

        foreach (ColumnPropertyTarget target in block.Targets)
        {
            lines.Add($"target '{target.Name}' {(int)target.ChunkType}");
            foreach (ColumnPropertyEntry entry in target.Entries)
            {
                lines.Add($"  {entry.Name} {(int)entry.DataType} {entry.DdlFlag} {Convert.ToHexString(entry.Value)}");
            }
        }

        foreach (ColumnPropertyUnknownChunk chunk in block.UnknownChunks)
        {
            lines.Add($"chunk {chunk.ChunkType} {Convert.ToHexString(chunk.Payload)}");
        }

        return lines;
    }

    /// <summary>
    /// Reads every table's properties and compares them with the earlier string
    /// decode of <c>MSysObjects</c>, kept in this test. Asserts that the read is
    /// the table's own stored blob, and that it differs from the earlier decode
    /// only where that decode took another object's row (a collision) or dropped
    /// the blob for a file signature (a signature case).
    /// </summary>
    /// <param name="harness">The open database.</param>
    /// <returns>The names of the tables in each kind of intended difference.</returns>
    private static async Task<(List<string> Collisions, List<string> Signatures)> CompareWithLegacyStringPathAsync(ReaderHarness harness)
    {
        DatabaseFile db = harness.Database;
        IReadOnlyList<RawCatalogRow> rows = await ReadRawCatalogRowsAsync(db);
        var collisions = new List<string>();
        var signatures = new List<string>();
        foreach (RawCatalogRow table in rows.Where(r => r.Type == Constants.SystemObjects.UserTableType && r.Id > 0 && r.Id == (r.Id & LowIdBits)))
        {
            ColumnPropertyBlock? read = await harness.Services.Catalog.ReadLvPropForTableAsync(table.Id, Ct);
            byte[]? ownBlob = await table.ReadLvPropAsync(db);
            Assert.Equal(Describe(ColumnPropertyBlock.Parse(ownBlob, db.Format)), Describe(read));

            // The earlier read scanned the rows in page and slot order, as
            // ReadRawCatalogRowsAsync lists them, and took the first whose Id's
            // low 24 bits equal the TDEF page.
            RawCatalogRow legacyRow = rows.First(r => (r.Id & LowIdBits) == table.Id);
            var legacyBlock = ColumnPropertyBlock.Parse(await ReadLegacyLvPropAsync(db, legacyRow), db.Format);
            if (Describe(legacyBlock).SequenceEqual(Describe(read)))
            {
                continue;
            }

            if (legacyRow.Id != table.Id)
            {
                collisions.Add(table.Name);
            }
            else
            {
                Assert.True(legacyBlock is null && ownBlob is not null && SignatureOffset(ownBlob) >= 0, $"{table.Name}: the read differs from the string decode for no intended reason.");
                signatures.Add(table.Name);
            }
        }

        return (collisions, signatures);
    }

    /// <summary>
    /// The <c>LvProp</c> bytes the earlier string decode took from a catalog row.
    /// It rendered the OLE column as a data URI the way the OLE decode did before
    /// reads returned stored bytes, and kept the bytes only for an
    /// <c>application/octet-stream</c> URI. That decode first unwrapped an OLE
    /// Package (<c>15 1C</c>); a property blob starts with <c>KKD\0</c> or
    /// <c>MR2\0</c>, so that step never applied and is asserted instead of copied.
    /// It then looked for a file signature in the first 512 bytes, and on a match
    /// rendered the bytes from there with the signature's media type, which the
    /// read rejected. Otherwise every stored byte was kept. An unreadable value
    /// rendered as placeholder text, which the read also rejected.
    /// </summary>
    /// <param name="db">The open database.</param>
    /// <param name="row">The catalog row.</param>
    /// <returns>The bytes the earlier read parsed, or <see langword="null"/> when it found none.</returns>
    private static async Task<byte[]?> ReadLegacyLvPropAsync(DatabaseFile db, RawCatalogRow row)
    {
        byte[]? stored;
        try
        {
            stored = await row.ReadLvPropAsync(db);
        }
        catch (InvalidDataException)
        {
            return null;
        }

        if (stored is null || stored.Length == 0)
        {
            return null;
        }

        Assert.False(stored is [0x15, 0x1C, ..], $"{row.Name}: an LvProp blob starts with the OLE Package header, which the copied decode does not unwrap.");
        return SignatureOffset(stored) >= 0 ? null : stored;
    }

    /// <summary>
    /// Returns the offset of the first file signature in the first 512 bytes, as
    /// the earlier OLE decode looked for one, or -1. Like that decode, it tries
    /// every offset up to four bytes before the end of the window, and a
    /// signature must end inside the window.
    /// </summary>
    /// <param name="bytes">The stored bytes.</param>
    private static int SignatureOffset(byte[] bytes)
    {
        byte[][] signatures =
        [
            [0xFF, 0xD8, 0xFF], [0x89, 0x50, 0x4E, 0x47], [0x47, 0x49, 0x46], [0x42, 0x4D], [0x49, 0x49, 0x2A, 0x00],
            [0x4D, 0x4D, 0x00, 0x2A], [0x25, 0x50, 0x44, 0x46], [0x50, 0x4B, 0x03, 0x04], [0xD0, 0xCF, 0x11, 0xE0], [0x7B, 0x5C, 0x72, 0x74],
        ];
        int end = Math.Min(bytes.Length, 512);
        for (int offset = 0; offset + 4 <= end; offset++)
        {
            if (signatures.Any(s => offset + s.Length <= end && bytes.AsSpan(offset, s.Length).SequenceEqual(s)))
            {
                return offset;
            }
        }

        return -1;
    }

    /// <summary>
    /// Lists every live <c>MSysObjects</c> row with its Id, name, type and a copy
    /// of its bytes, walking the catalog's data pages directly rather than
    /// through the row decoders under test.
    /// </summary>
    /// <param name="db">The open database.</param>
    private static async Task<IReadOnlyList<RawCatalogRow>> ReadRawCatalogRowsAsync(DatabaseFile db)
    {
        TableDef msys = Assert.IsType<TableDef>(await db.TableDefs.ReadTableDefAsync(2, Ct));
        ColumnInfo id = Assert.IsType<ColumnInfo>(msys.FindColumn("Id"));
        ColumnInfo name = Assert.IsType<ColumnInfo>(msys.FindColumn("Name"));
        ColumnInfo type = Assert.IsType<ColumnInfo>(msys.FindColumn("Type"));
        ColumnInfo? lvProp = msys.FindColumn("LvProp");
        var rows = new List<RawCatalogRow>();
        await db.OwnedPages.ForEachLiveTableRowAsync(
            2,
            (row, _) =>
            {
                byte[] bytes = row.Page.AsSpan(row.Location.RowStart, row.Location.RowSize).ToArray();
                ColumnSlice slice = default;
                if (lvProp is not null && RowDecodePlan.TryParseRowLayout(db.Format.RowFields, bytes, 0, bytes.Length, hasVarColumns: true, out RowLayout layout))
                {
                    slice = RowDecodePlan.ResolveColumnSlice(db.Format.RowFields, bytes, 0, bytes.Length, layout, lvProp);
                }

                rows.Add(new RawCatalogRow(
                    long.TryParse(ScalarColumnReader.DecodeSimpleColumnValue(db.Format, bytes, 0, bytes.Length, id), NumberStyles.Integer, CultureInfo.InvariantCulture, out long idValue) ? idValue : 0,
                    ScalarColumnReader.DecodeSimpleColumnValue(db.Format, bytes, 0, bytes.Length, name),
                    int.TryParse(ScalarColumnReader.DecodeSimpleColumnValue(db.Format, bytes, 0, bytes.Length, type), NumberStyles.Integer, CultureInfo.InvariantCulture, out int typeValue) ? typeValue : 0,
                    bytes,
                    slice));
                return new ValueTask<bool>(true);
            },
            Ct);
        return rows;
    }

    private static async Task<List<object[]>> ReadRowsAsync(AccessReader reader, string table)
    {
        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
        {
            rows.Add(row);
        }

        return rows;
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(ms, format, WriterOptions(WriteMode.Direct), leaveOpen: true, cancellationToken: Ct))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static async Task<MemoryStream> CopyFixtureAsync(string path)
    {
        var ms = new MemoryStream();
        byte[] bytes = await File.ReadAllBytesAsync(path, Ct);
        await ms.WriteAsync(bytes, Ct);
        ms.Position = 0;
        return ms;
    }

    private static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    private static async Task<AccessWriter> OpenWriterAsync(MemoryStream ms, WriteMode mode)
    {
        ms.Position = 0;
        return await AccessWriter.OpenAsync(ms, WriterOptions(mode), leaveOpen: true, cancellationToken: Ct);
    }

    private static async Task<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

    private static Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work)
        => JetDatabaseWriter.Tests.Infrastructure.WriteModes.RunAsync(writer, mode, work, Ct);

    /// <summary>One <c>MSysObjects</c> row, read straight from its data page.</summary>
    /// <param name="Id">The row's <c>Id</c>.</param>
    /// <param name="Name">The row's <c>Name</c>.</param>
    /// <param name="Type">The row's object <c>Type</c>.</param>
    /// <param name="Bytes">A copy of the row's bytes.</param>
    /// <param name="LvPropSlice">The <c>LvProp</c> column's slice of <paramref name="Bytes"/>.</param>
    private sealed record RawCatalogRow(long Id, string Name, int Type, byte[] Bytes, ColumnSlice LvPropSlice)
    {
        /// <summary>Gets the storage-mode bits of the <c>LvProp</c> descriptor, or -1 when the row has none.</summary>
        public int StorageMode => this.LvPropSlice.Kind == ColumnSliceKind.Var && this.LvPropSlice.DataLen >= 12
            ? this.Bytes[this.LvPropSlice.DataStart + 3] & 0xC0
            : -1;

        /// <summary>Reads the stored <c>LvProp</c> bytes, or <see langword="null"/> when the row has none.</summary>
        /// <param name="db">The open database.</param>
        public async Task<byte[]?> ReadLvPropAsync(DatabaseFile db)
        {
            if (this.LvPropSlice.Kind != ColumnSliceKind.Var || this.LvPropSlice.DataLen <= 0)
            {
                return null;
            }

            using var pages = new ReaderPageCache(db.Format, db.Pages, capacity: 0);
            return await new LongValueDecoder(db.Format, pages).ReadLongValueBytesExactAsync(this.Bytes, this.LvPropSlice.DataStart, this.LvPropSlice.DataLen, Ct);
        }
    }
}
