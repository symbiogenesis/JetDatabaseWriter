namespace JetDatabaseWriter.Tests.Catalog;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins the <c>MSysObjects</c> scan summary that <see cref="AccessReader.LastDiagnostics"/>
/// prints after <see cref="AccessReader.ListTablesAsync"/>. "Total rows scanned" counts the
/// live catalog rows the scan could decode; rows too short or malformed to decode are skipped,
/// as the table reader skips them.
/// </summary>
public sealed partial class CatalogDiagnosticsTests
{
    /// <summary>Gets Access-authored Jet3, Jet4 and ACE fixtures.</summary>
    public static TheoryData<string> RowsScannedFixtures =>
    [
        TestDatabases.TestV1997,
        TestDatabases.IndexTestV2003,
        TestDatabases.TestV2003,
        TestDatabases.UnsupportedFieldsTestV2007,
        TestDatabases.ComplexDataTestV2007,
        TestDatabases.TestV2010,
        TestDatabases.NorthwindTraders,
        TestDatabases.MdbtoolsNwind,
    ];

    /// <summary>Gets the fixtures whose catalogs hold overflow rows, with their catalog row counts.</summary>
    public static TheoryData<string, int> OverflowCatalogFixtures => new()
    {
        { TestDatabases.NorthwindTraders, 239 },
        { TestDatabases.MdbtoolsNwind, 106 },
    };

    /// <summary>
    /// Every live catalog row of these fixtures decodes. Three rows of each share
    /// their offset with deleted slots, and the scan used to give them zero bytes
    /// and skip them as undecodable (25 and 35).
    /// </summary>
    /// <param name="fixture">The fixture file name under the Jackcess tree.</param>
    /// <param name="expectedRows">The expected "Total rows scanned" count.</param>
    [Theory]
    [InlineData("V2003/indexTestV2003.mdb", 28)]
    [InlineData("V2007/unsupportedFieldsTestV2007.accdb", 38)]
    public async Task ListTables_Diagnostics_CountsEveryLiveCatalogRow(string fixture, int expectedRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = System.IO.Path.Combine(TestDatabases.JackcessRoot, fixture);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            ct);

        _ = await reader.ListTablesAsync(ct);

        Assert.Equal(expectedRows, ParseRowsScanned(reader.LastDiagnostics));
    }

    /// <summary>
    /// A live catalog row whose column count is zero cannot be decoded. The scan
    /// leaves it out of "Total rows scanned", as the table reader leaves it out of
    /// <c>MSysObjects</c>.
    /// </summary>
    [Fact]
    public async Task ListTables_Diagnostics_SkipsUndecodableCatalogRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] bytes = await File.ReadAllBytesAsync(TestDatabases.TestV2003, ct);
        long declaredRows = await ZeroColumnCountOfLastUserTableRowAsync(bytes, ct);

        await using var ms = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            leaveOpen: true,
            ct);

        _ = await reader.ListTablesAsync(ct);
        int rowsScanned = ParseRowsScanned(reader.LastDiagnostics);
        using DataTable msys = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);

        Assert.Equal(declaredRows - 1, rowsScanned);
        Assert.Equal(msys.Rows.Count, rowsScanned);
    }

    /// <summary>
    /// The catalogs of NorthwindTraders.accdb and the Jet3 nwind.mdb hold overflow
    /// rows (40 of NorthwindTraders' 239); each counts once, at its header, as
    /// Access counts it in the catalog's TDEF row count.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    /// <param name="expectedRows">The expected "Total rows scanned" count.</param>
    [Theory]
    [MemberData(nameof(OverflowCatalogFixtures))]
    public async Task ListTables_Diagnostics_CountsOverflowCatalogRowsOnce(string path, int expectedRows)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            ct);

        _ = await reader.ListTablesAsync(ct);

        Assert.Equal(expectedRows, ParseRowsScanned(reader.LastDiagnostics));
    }

    /// <summary>
    /// "Total rows scanned" matches the number of rows the table reader returns for
    /// <c>MSysObjects</c>, across Jet3, Jet4 and ACE fixtures.
    /// </summary>
    /// <param name="path">The fixture path.</param>
    [Theory]
    [MemberData(nameof(RowsScannedFixtures))]
    public async Task ListTables_Diagnostics_RowsScannedMatchesMSysObjectsRowCount(string path)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            ct);

        _ = await reader.ListTablesAsync(ct);
        int rowsScanned = ParseRowsScanned(reader.LastDiagnostics);

        using DataTable msys = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);

        Assert.Equal(msys.Rows.Count, rowsScanned);
    }

    /// <summary>
    /// "Total rows scanned" matches the <c>MSysObjects</c> row count on databases the
    /// writer creates, where no catalog row names <c>MSysObjects</c> except in the
    /// full-catalog ACCDB schema; the table read used to find no <c>MSysObjects</c>
    /// there and return no rows.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="fullCatalog">Whether the database has the full catalog schema.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    public async Task ListTables_Diagnostics_RowsScannedMatchesMSysObjectsRowCount_OnCreatedDatabase(DatabaseFormat format, bool fullCatalog)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            format,
            new AccessWriterOptions { UseLockFile = false, WriteFullCatalogSchema = fullCatalog },
            leaveOpen: true,
            ct))
        {
            await writer.CreateTableAsync("T1", [new ColumnDefinition("Id", typeof(int))], ct);
        }

        ms.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            ms,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            leaveOpen: true,
            ct);

        _ = await reader.ListTablesAsync(ct);
        int rowsScanned = ParseRowsScanned(reader.LastDiagnostics);

        using DataTable msys = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);

        Assert.True(rowsScanned > 0, "The catalog scan found no rows.");
        Assert.Equal(msys.Rows.Count, rowsScanned);
    }

    private static int ParseRowsScanned(string diagnostics)
    {
        Match match = RowsScannedRegex().Match(diagnostics);
        Assert.True(match.Success, $"No 'Total rows scanned' line in diagnostics:\n{diagnostics}");
        return int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Zeroes the column-count field of the last user table's <c>MSysObjects</c>
    /// row in <paramref name="bytes"/> and returns the row count the catalog's
    /// TDEF declares.
    /// </summary>
    /// <param name="bytes">The database image, changed in place.</param>
    /// <param name="cancellationToken">A token used to cancel the reads.</param>
    private static async ValueTask<long> ZeroColumnCountOfLastUserTableRowAsync(byte[] bytes, CancellationToken cancellationToken)
    {
        await using var ms = new MemoryStream(bytes, writable: false);
        await using ReaderHarness harness = await ReaderHarness.OpenAsync(ms, cancellationToken: cancellationToken);
        DatabaseFile db = harness.Database;
        TableDef? msys = await harness.ReadTableDefAsync(2, cancellationToken);
        Assert.NotNull(msys);

        List<CatalogRow> rows = await new CatalogRowReader(db.Profile, db.TableDefs, db.OwnedPages).GetCatalogRowsAsync(msys, cancellationToken);
        CatalogRow target = rows.Last(row => row.IsDecoded && row.ObjectType == 1 && !row.Name.StartsWith("MSys", StringComparison.OrdinalIgnoreCase));

        byte[] page = await harness.ReadPageCopyAsync(target.PageNumber, cancellationToken);
        RowBound bound = db.EnumerateLiveRowBounds(page).Single(b => b.RowIndex == target.RowIndex);
        int rowOffset = checked((int)(target.PageNumber * db.PageSizeBytes)) + bound.RowStart;
        bytes.AsSpan(rowOffset, db.RowColumnCountFieldSize).Clear();

        byte[] tdef = await harness.ReadPageCopyAsync(2, cancellationToken);
        return BinaryPrimitives.ReadUInt32LittleEndian(tdef.AsSpan(db.TDef.NumRows));
    }

    [GeneratedRegex(@"Total rows scanned: (\d+)")]
    private static partial Regex RowsScannedRegex();
}
