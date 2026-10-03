namespace JetDatabaseWriter.Tests.Catalog;

using System.Data;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
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
    /// <summary>
    /// The scan summary reports the decodable catalog rows only. Both fixtures carry
    /// three live <c>MSysObjects</c> rows that cannot be decoded, which the count used to include
    /// (28 and 38).
    /// </summary>
    /// <param name="fixture">The fixture file name under the Jackcess tree.</param>
    /// <param name="expectedRows">The expected "Total rows scanned" count.</param>
    [Theory]
    [InlineData("V2003/indexTestV2003.mdb", 25)]
    [InlineData("V2007/unsupportedFieldsTestV2007.accdb", 35)]
    public async Task ListTables_Diagnostics_CountsOnlyDecodableCatalogRows(string fixture, int expectedRows)
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
    /// "Total rows scanned" matches the number of rows the table reader returns for
    /// <c>MSysObjects</c>, across Jet3, Jet4 and ACE fixtures.
    /// </summary>
    /// <param name="fixture">The fixture file name under the Jackcess tree.</param>
    [Theory]
    [InlineData("V1997/testV1997.mdb")]
    [InlineData("V2003/indexTestV2003.mdb")]
    [InlineData("V2003/testV2003.mdb")]
    [InlineData("V2007/unsupportedFieldsTestV2007.accdb")]
    [InlineData("V2007/complexDataTestV2007.accdb")]
    [InlineData("V2010/testV2010.accdb")]
    public async Task ListTables_Diagnostics_RowsScannedMatchesMSysObjectsRowCount(string fixture)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = System.IO.Path.Combine(TestDatabases.JackcessRoot, fixture);

        await using AccessReader reader = await AccessReader.OpenAsync(
            path,
            new AccessReaderOptions { DiagnosticsEnabled = true, UseLockFile = false },
            ct);

        _ = await reader.ListTablesAsync(ct);
        int rowsScanned = ParseRowsScanned(reader.LastDiagnostics);

        using DataTable msys = await reader.ReadDataTableAsync("MSysObjects", cancellationToken: ct);

        Assert.Equal(msys.Rows.Count, rowsScanned);
    }

    private static int ParseRowsScanned(string diagnostics)
    {
        Match match = RowsScannedRegex().Match(diagnostics);
        Assert.True(match.Success, $"No 'Total rows scanned' line in diagnostics:\n{diagnostics}");
        return int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    [GeneratedRegex(@"Total rows scanned: (\d+)")]
    private static partial Regex RowsScannedRegex();
}
