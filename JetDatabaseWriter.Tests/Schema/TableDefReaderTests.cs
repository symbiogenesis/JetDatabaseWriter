namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Enums.ColumnType;

/// <summary>
/// Pins <see cref="TableDefReader"/>, the TDEF parser built over a page source
/// and a format profile alone. Its parse of every table of the Access-authored
/// fixtures matches a hash of every descriptor field, pinned from the parser
/// before it moved out of <see cref="DatabaseFile"/>, so a change to the parse
/// fails here even though the facade's own reads, which go through the same
/// parser, would change with it. It also agrees with the public column
/// metadata, and it reads the whole chain of a TDEF that spans several pages,
/// inside a transaction too.
/// </summary>
public sealed class TableDefReaderTests
{
    private const string WideTable = "Wide";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<string, string?> Fixtures => new()
    {
        { "Jet3Test", null },
        { "AdventureLT2008", null },
        { "NorthwindTraders", null },
        { "ComplexFields", null },
        { "CompositeTextIndex", null },
        { "AesEncrypted", TestDatabases.AesEncryptedPassword },
    };

    /// <summary>
    /// Gets the SHA-256 of each fixture's descriptor text (see
    /// <see cref="AppendDescriptors"/>), captured from
    /// <c>DatabaseFile.ReadTableDefAsync</c> before the TDEF parser moved into
    /// <see cref="TableDefReader"/>.
    /// </summary>
    public static TheoryData<string, string?, string> PinnedFixtures => new()
    {
        { "Jet3Test", null, "99CF57A4F7EA77CC891E9F6652E6C6D446461A419F3A6336D75070E72C0ADB24" },
        { "AdventureLT2008", null, "5032DBA5C4147D35BBF2355A1C792E0E332A37FBEABD7C2D59251866BD72E002" },
        { "NorthwindTraders", null, "92B04B8AD8173174F3B955E91736D4E8275D6A817F71CB5C749323767A5AD8BF" },
        { "ComplexFields", null, "0B7E845C886558F291F58F1DF9A2939400A7A4E385D28CCAC97DAFDB79D5E824" },
        { "CompositeTextIndex", null, "D2EE3BE939AA941E7C32AD460498FBCAD05EF3B74EEC02116EB4741C347C5E5F" },
        { "AesEncrypted", TestDatabases.AesEncryptedPassword, "92B04B8AD8173174F3B955E91736D4E8275D6A817F71CB5C749323767A5AD8BF" },
    };

    /// <summary>
    /// Every table of each fixture, in ordinal name order, parses to the
    /// pinned descriptors: each table's row count and deleted-column flag, and
    /// each column's name, type, number, variable index, fixed offset, size,
    /// flags, extra flags, misc slot, precision and scale.
    /// </summary>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="password">The fixture's password, or <see langword="null"/>.</param>
    /// <param name="expectedSha256">The pinned hash, upper-case hex.</param>
    [Theory]
    [MemberData(nameof(PinnedFixtures))]
    public async Task ReadTableDef_EveryFixtureTable_MatchesPinnedDescriptors(string fixture, string? password, string expectedSha256)
    {
        var options = new AccessReaderOptions(password) { UseLockFile = false };
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(FixturePath(fixture), options, Ct);
        var tableDefs = new TableDefReader(harness.Database.Pages, harness.Database.Profile);

        var text = new StringBuilder();
        IReadOnlyList<string> tables = await harness.Services.Schema.ListTablesAsync(Ct);
        Assert.NotEmpty(tables);
        foreach (string table in tables.Order(StringComparer.Ordinal))
        {
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
            Assert.NotNull(entry);
            TableDef? tableDef = await tableDefs.ReadTableDefAsync(entry.TDefPage, Ct);
            Assert.NotNull(tableDef);
            AppendDescriptors(text, table, tableDef);
        }

        string actual = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ToString())));
        Assert.True(
            string.Equals(expectedSha256, actual, StringComparison.Ordinal),
            $"The {fixture} descriptors changed: SHA-256 {actual}, pinned {expectedSha256}.{Environment.NewLine}{text}");
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task ReadTableDef_EveryFixtureTable_MatchesColumnMetadata(string fixture, string? password)
    {
        var options = new AccessReaderOptions(password) { UseLockFile = false };
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(FixturePath(fixture), options, Ct);
        var tableDefs = new TableDefReader(harness.Database.Pages, harness.Database.Profile);

        IReadOnlyList<string> tables = await harness.Services.Schema.ListTablesAsync(Ct);
        Assert.NotEmpty(tables);
        foreach (string table in tables)
        {
            CatalogEntry? entry = await harness.GetCatalogEntryAsync(table, Ct);
            Assert.NotNull(entry);
            TableDef? tableDef = await tableDefs.ReadTableDefAsync(entry.TDefPage, Ct);
            Assert.NotNull(tableDef);
            IReadOnlyList<ColumnMetadata> metadata = await harness.Services.Schema.GetColumnMetadataAsync(table, Ct);

            Assert.Equal(metadata.Select(m => m.Name), tableDef.Columns.Select(c => c.Name));
            for (int i = 0; i < metadata.Count; i++)
            {
                ColumnInfo column = tableDef.Columns[i];
                ColumnMetadata expected = metadata[i];
                Assert.Equal(i, expected.Ordinal);
                Assert.Equal(expected.IsFixedLength, column.IsFixed);
                if (!column.IsCalculated && !expected.IsHyperlink && column.Type is not (ComplexType or AttachmentType))
                {
                    Assert.True(
                        expected.TypeName == JetTypeInfo.GetTypeDisplayName(column.Type),
                        $"{fixture} {table}.{column.Name}: metadata says {expected.TypeName}, the TDEF says {JetTypeInfo.GetTypeDisplayName(column.Type)}.");
                }
            }

            TableDef required = await tableDefs.ReadRequiredTableDefAsync(entry.TDefPage, table, Ct);
            Assert.Equal(tableDef.Columns.Select(c => c.Name), required.Columns.Select(c => c.Name));
        }
    }

    [Fact]
    public async Task ReadTableDef_NotATdefPage_ReturnsNullAndRequiredThrows()
    {
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(TestDatabases.NorthwindTraders, cancellationToken: Ct);
        var tableDefs = new TableDefReader(harness.Database.Pages, harness.Database.Profile);

        // Page 1 is the global usage map, a data page.
        Assert.Null(await tableDefs.ReadTableDefAsync(1, Ct));
        Assert.Null(await tableDefs.ReadTDefBytesAsync(1, Ct));
        InvalidDataException ex = await Assert.ThrowsAsync<InvalidDataException>(async () => await tableDefs.ReadRequiredTableDefAsync(1, "Missing", Ct));
        Assert.Contains("'Missing'", ex.Message, StringComparison.Ordinal);
        _ = await Assert.ThrowsAsync<InvalidDataException>(async () => await tableDefs.ReadTDefChainAsync(1, Ct));
    }

    /// <summary>
    /// A 32-bit field on the second page of a multi-page TDEF chain, written
    /// in place through the writer's file and read back through
    /// <see cref="TableDefReader"/>: with no transaction it reaches the file
    /// at once; inside an explicit transaction the reader sees it from the
    /// journal while the file keeps the old bytes, and the commit writes it.
    /// The original value is written back, and the table then reads as before.
    /// (The in-place TDEF writes stay on <see cref="DatabaseFile"/> until
    /// core-split-b gives the writer its TDEF writer.)
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="inTransaction">Whether the write runs inside an explicit transaction.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task WriteInt32_OnContinuationPage_RoundTrips(DatabaseFormat format, bool inTransaction)
    {
        const int patched = 0x5A17C0DE;
        await using MemoryStream stream = await CreateWideTableAsync(format);
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        DatabaseFile db = harness.Database;
        var tableDefs = new TableDefReader(db.Pages, db.Profile);
        int pageSize = db.PageSizeBytes;

        CatalogEntry? entry = await harness.Services.Catalog.GetCatalogEntryAsync(WideTable, Ct);
        Assert.NotNull(entry);
        TableDef before = await tableDefs.ReadRequiredTableDefAsync(entry.TDefPage, WideTable, Ct);
        LogicalTDefChain chain = await tableDefs.ReadTDefChainAsync(entry.TDefPage, Ct);
        Assert.True(chain.PageNumbers.Count >= 2, $"The {format} TDEF spans {chain.PageNumbers.Count} page(s).");

        // Bytes 8-11 of the second page's body: that body starts after the
        // 8-byte page header, so logical offset pageSize + 8 is physical
        // offset 16 of the continuation page.
        int logicalOffset = pageSize + 8;
        long continuationPage = chain.PageNumbers[1];
        int original = BitConverter.ToInt32(chain.Bytes, logicalOffset);
        byte[] fileBefore = stream.ToArray();

        JetTransaction? tx = inTransaction ? await harness.Services.Transactions.BeginTransactionAsync(Ct) : null;
        await db.WriteTDefInt32Async(entry.TDefPage, logicalOffset, patched, Ct);

        byte[]? logical = await tableDefs.ReadTDefBytesAsync(entry.TDefPage, Ct);
        Assert.NotNull(logical);
        Assert.Equal(patched, BitConverter.ToInt32(logical, logicalOffset));
        Assert.Equal(patched, BitConverter.ToInt32(await db.ReadPageCopyAsync(continuationPage, Ct), 16));
        int onDiskOffset = checked((int)(continuationPage * pageSize)) + 16;
        if (tx is not null)
        {
            Assert.Equal(fileBefore, stream.ToArray());
            await tx.CommitAsync(Ct);
        }

        Assert.Equal(patched, BitConverter.ToInt32(stream.ToArray(), onDiskOffset));

        await db.WriteTDefInt32Async(entry.TDefPage, logicalOffset, original, Ct);
        TableDef after = await tableDefs.ReadRequiredTableDefAsync(entry.TDefPage, WideTable, Ct);
        Assert.Equal(before.Columns.Select(c => (c.Name, c.Type, c.ColNum)), after.Columns.Select(c => (c.Name, c.Type, c.ColNum)));
        Assert.Equal(fileBefore, stream.ToArray());
    }

    private static string FixturePath(string fixture) => fixture switch
    {
        "Jet3Test" => TestDatabases.Jet3Test,
        "AdventureLT2008" => TestDatabases.AdventureWorks,
        "NorthwindTraders" => TestDatabases.NorthwindTraders,
        "ComplexFields" => TestDatabases.ComplexFields,
        "CompositeTextIndex" => TestDatabases.CompositeTextIndex,
        "AesEncrypted" => TestDatabases.AesEncrypted,
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, "Unknown fixture."),
    };

    /// <summary>
    /// Appends one line for <paramref name="table"/> (name, row count,
    /// deleted-column flag) and one line per column with every field the TDEF
    /// parser reads from the column descriptor.
    /// </summary>
    /// <param name="text">The descriptor text.</param>
    /// <param name="table">The table name.</param>
    /// <param name="tableDef">The parsed table definition.</param>
    private static void AppendDescriptors(StringBuilder text, string table, TableDef tableDef)
    {
        _ = text.Append(CultureInfo.InvariantCulture, $"{table}|{tableDef.RowCount}|{tableDef.HasDeletedColumns}\n");
        foreach (ColumnInfo c in tableDef.Columns)
        {
            _ = text.Append(
                CultureInfo.InvariantCulture,
                $"  {c.Name}|{(int)c.Type}|{c.ColNum}|{c.VarIdx}|{c.FixedOff}|{c.Size}|{c.Flags}|{c.ExtraFlags}|{c.Misc}|{c.NumericPrecision}|{c.NumericScale}\n");
        }
    }

    private static async Task<MemoryStream> CreateWideTableAsync(DatabaseFormat format)
    {
        int columnCount = format == DatabaseFormat.Jet3Mdb ? 50 : 200;
        int indexCount = format == DatabaseFormat.Jet3Mdb ? 20 : 30;
        var columns = new List<ColumnDefinition>(columnCount);
        for (int i = 0; i < columnCount; i++)
        {
            columns.Add(new ColumnDefinition(string.Create(CultureInfo.InvariantCulture, $"C{i:D3}"), typeof(int)));
        }

        var indexes = new List<IndexDefinition>(indexCount);
        for (int i = 0; i < indexCount; i++)
        {
            indexes.Add(new IndexDefinition(
                string.Create(CultureInfo.InvariantCulture, $"IX_{i:D2}"),
                string.Create(CultureInfo.InvariantCulture, $"C{i:D3}")));
        }

        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false, UseByteRangeLocks = false },
            leaveOpen: true,
            Ct))
        {
            await writer.CreateTableAsync(WideTable, columns, indexes, Ct);
        }

        stream.Position = 0;
        return stream;
    }
}
