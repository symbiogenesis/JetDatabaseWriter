namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Byte-level coverage for Access-authored calculated-column cached values.
/// </summary>
/// <param name="db">The database input.</param>
public sealed class CalculatedColumnPayloadTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string JackcessTableName = "Table1";
    private const string DaoTableName = "CalcBuiltins";
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    [Fact]
    public async Task JackcessFixture_CalculatedColumns_HaveExpectedCachedPayloadBytes()
    {
        AccessReader reader = await db.GetReaderAsync(
            TestDatabases.CalcFieldTestV2010,
            TestContext.Current.CancellationToken);

        DataTable table = await reader.ReadDataTableAsync(
            JackcessTableName,
            cancellationToken: TestContext.Current.CancellationToken);

        List<Dictionary<string, byte[]>> rawRows = await ReadCalculatedPayloadRowsAsync(
            reader,
            JackcessTableName,
            ["LastFirst", "LastFirstLen"],
            TestContext.Current.CancellationToken);

        Assert.Equal(table.Rows.Count, rawRows.Count);

        for (int rowIndex = 0; rowIndex < table.Rows.Count; rowIndex++)
        {
            DataRow decodedRow = table.Rows[rowIndex];
            Dictionary<string, byte[]> rawRow = rawRows[rowIndex];

            Assert.Equal(TextPayload(Convert.ToString(decodedRow["LastFirst"], CultureInfo.InvariantCulture)!), rawRow["LastFirst"]);
            Assert.Equal(Int32Payload(decodedRow["LastFirstLen"]), rawRow["LastFirstLen"]);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoAuthoredIIfColumn_HasExpectedCachedPayloadBytes_AndUnsupportedExpressionsAreRejected()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedColumns");
        string dbPath = session.CreateDatabasePath("calc_builtin_payloads");

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(BuildDaoCalculatedBuiltinAuthoringScript(dbPath), DaoTimeout);
        Assert.True(
            result.ExitCode == 0 && File.Exists(dbPath),
            $"DAO calculated-column authoring failed (exit={result.ExitCode}).\nstdout: {result.StdOut}\nstderr: {result.StdErr}");

        await using AccessReader reader = await AccessReader.OpenAsync(
            dbPath,
            new AccessReaderOptions { UseLockFile = false },
            TestContext.Current.CancellationToken);

        IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync(
            DaoTableName,
            TestContext.Current.CancellationToken);

        ColumnMetadata iifBand = Assert.Single(metadata, column => column.Name == "IIfBand");
        ColumnMetadata isHigh = Assert.Single(metadata, column => column.Name == "IsHigh");
        Assert.DoesNotContain(metadata, column => column.Name == "SwitchBand");

        Assert.True(iifBand.IsCalculated);
        Assert.True(isHigh.IsCalculated);
        Assert.Contains("IIf", iifBand.CalculationExpression, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("IIf", isHigh.CalculationExpression, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SWITCH_REJECTED=", result.StdOut, StringComparison.Ordinal);
        foreach (string functionName in new[] { "DLookUp", "DCount", "DSum", "DAvg", "DMin", "DMax" })
        {
            Assert.Contains($"DOMAIN_AGGREGATE_REJECTED {functionName}=", result.StdOut, StringComparison.Ordinal);
        }

        DataTable table = await reader.ReadDataTableAsync(
            DaoTableName,
            cancellationToken: TestContext.Current.CancellationToken);

        string[] expectedIIfBands = ["Low", "High", "High"];
        bool[] expectedIsHigh = [false, true, true];

        Assert.Equal(expectedIIfBands.Length, table.Rows.Count);

        List<Dictionary<string, byte[]>> rawRows = await ReadCalculatedPayloadRowsAsync(
            reader,
            DaoTableName,
            ["IIfBand", "IsHigh"],
            TestContext.Current.CancellationToken);

        Assert.Equal(table.Rows.Count, rawRows.Count);

        for (int rowIndex = 0; rowIndex < table.Rows.Count; rowIndex++)
        {
            DataRow decodedRow = table.Rows[rowIndex];
            Dictionary<string, byte[]> rawRow = rawRows[rowIndex];

            Assert.Equal(rowIndex + 1, Convert.ToInt32(decodedRow["Id"], CultureInfo.InvariantCulture));
            Assert.Equal(expectedIIfBands[rowIndex], decodedRow["IIfBand"]);
            Assert.Equal(TextPayload(expectedIIfBands[rowIndex]), rawRow["IIfBand"]);
            Assert.Equal(BooleanPayload(expectedIsHigh[rowIndex]), rawRow["IsHigh"]);
            Assert.Equal(expectedIsHigh[rowIndex], Convert.ToBoolean(decodedRow["IsHigh"], CultureInfo.InvariantCulture));
        }
    }

    private static async ValueTask<List<Dictionary<string, byte[]>>> ReadCalculatedPayloadRowsAsync(
        AccessReader reader,
        string tableName,
        IReadOnlyList<string> columnNames,
        CancellationToken cancellationToken)
    {
        CatalogEntry? entry = await reader.GetCatalogEntryAsync(tableName, cancellationToken).ConfigureAwait(false);
        Assert.NotNull(entry);

        TableDef? tableDef = await reader.ReadTableDefAsync(entry.TDefPage, cancellationToken).ConfigureAwait(false);
        Assert.NotNull(tableDef);

        var columns = new ColumnInfo[columnNames.Count];
        for (int i = 0; i < columnNames.Count; i++)
        {
            ColumnInfo? column = tableDef.FindColumn(columnNames[i]);
            Assert.NotNull(column);
            Assert.True(column.IsCalculated, $"Column '{column.Name}' should be calculated.");
            columns[i] = column;
        }

        var rows = new List<Dictionary<string, byte[]>>();
        var dataPage = DataPageLayout.For(reader.DatabaseFormat);
        var rowSizes = RowFieldSizes.For(reader.DatabaseFormat);
        long pageCount = new FileInfo(reader.HostDatabasePath).Length / reader.PageSize;

        for (long pageNumber = 1; pageNumber < pageCount; pageNumber++)
        {
            byte[] page = await reader.GetRawPageBytesAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            if (page[0] != 0x01 || Ri32(page, dataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound rowBound in reader.EnumerateLiveRowBounds(page))
            {
                Assert.True(
                    TryParseRawRowLayout(page, rowBound.RowStart, rowBound.RowSize, tableDef.HasVarColumns, rowSizes, out RawRowLayout layout),
                    $"Could not parse row layout for page {pageNumber}, row {rowBound.RowIndex}.");

                var payloads = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
                foreach (ColumnInfo column in columns)
                {
                    Assert.True(
                        TryReadColumnBytes(page, rowBound.RowStart, rowBound.RowSize, layout, rowSizes, column, out byte[]? wrapped),
                        $"Could not read calculated column '{column.Name}' on page {pageNumber}, row {rowBound.RowIndex}.");
                    payloads[column.Name] = AssertAndUnwrapCalculatedPayload(wrapped!, column.Name);
                }

                rows.Add(payloads);
            }
        }

        return rows;
    }

    private static bool TryParseRawRowLayout(
        byte[] page,
        int rowStart,
        int rowSize,
        bool hasVarColumns,
        RowFieldSizes rowSizes,
        out RawRowLayout layout)
    {
        layout = default;
        if (rowSize < rowSizes.NumCols)
        {
            return false;
        }

        int numCols = rowSizes.ReadNumCols(page, rowStart);
        if (numCols == 0)
        {
            return false;
        }

        int nullMaskSize = (numCols + 7) / 8;
        int nullMaskPos = rowSize - nullMaskSize;
        if (nullMaskPos < rowSizes.NumCols)
        {
            return false;
        }

        if (!hasVarColumns)
        {
            layout = new RawRowLayout(numCols, nullMaskPos, 0, nullMaskPos, nullMaskPos);
            return true;
        }

        int varLenPos = nullMaskPos - rowSizes.VarLen;
        if (varLenPos < rowSizes.NumCols)
        {
            return false;
        }

        int varLen = rowSizes.ReadVarLen(page, rowStart + varLenPos);
        int varTableStart = varLenPos - (varLen * rowSizes.VarEntry);
        int eodPos = varTableStart - rowSizes.Eod;
        if (eodPos < rowSizes.NumCols)
        {
            return false;
        }

        int eod = rowSizes.ReadEod(page, rowStart + eodPos);
        layout = new RawRowLayout(numCols, nullMaskPos, varLen, varTableStart, eod);
        return true;
    }

    private static bool TryReadColumnBytes(
        byte[] page,
        int rowStart,
        int rowSize,
        RawRowLayout layout,
        RowFieldSizes rowSizes,
        ColumnInfo column,
        out byte[]? value)
    {
        value = null;
        bool nullBit = false;
        if (column.ColNum < layout.NumCols)
        {
            int maskByte = layout.NullMaskPos + (column.ColNum / 8);
            int maskBit = column.ColNum % 8;
            if (maskByte < rowSize)
            {
                nullBit = (page[rowStart + maskByte] & (1 << maskBit)) != 0;
            }
        }

        if (column.ColNum >= layout.NumCols || !nullBit)
        {
            return false;
        }

        int dataStart;
        int dataLength;
        if (column.IsFixed)
        {
            dataStart = rowSizes.NumCols + column.FixedOff;
            dataLength = column.IsCalculated ? column.Size : GetFixedSize(column.Type);
        }
        else
        {
            if (column.VarIdx >= layout.VarLen)
            {
                return false;
            }

            int entryPos = layout.VarTableStart + ((layout.VarLen - 1 - column.VarIdx) * rowSizes.VarEntry);
            if (entryPos < 0 || entryPos + rowSizes.VarEntry > rowSize)
            {
                return false;
            }

            int varOff = rowSizes.ReadVarEntry(page, rowStart + entryPos);
            int varEnd;
            if (column.VarIdx + 1 < layout.VarLen)
            {
                int nextEntry = layout.VarTableStart + ((layout.VarLen - 2 - column.VarIdx) * rowSizes.VarEntry);
                varEnd = rowSizes.ReadVarEntry(page, rowStart + nextEntry);
            }
            else
            {
                varEnd = layout.Eod;
            }

            dataStart = varOff;
            dataLength = varEnd - varOff;
        }

        if (dataLength < 0 || dataStart < 0 || dataStart + dataLength > rowSize)
        {
            return false;
        }

        value = new byte[dataLength];
        Buffer.BlockCopy(page, rowStart + dataStart, value, 0, dataLength);
        return true;
    }

    private static byte[] AssertAndUnwrapCalculatedPayload(byte[] wrapped, string columnName)
    {
        Assert.True(
            wrapped.Length >= Constants.CalculatedColumn.DataOffset,
            $"Calculated column '{columnName}' wrapper should contain the 23-byte header.");

        int declaredLength = BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(Constants.CalculatedColumn.DataLenOffset, 4));
        Assert.True(declaredLength >= 0, $"Calculated column '{columnName}' payload length should be non-negative.");
        Assert.True(
            wrapped.Length >= Constants.CalculatedColumn.DataOffset + declaredLength,
            $"Calculated column '{columnName}' wrapper is shorter than its declared payload length.");

        byte[] payload = CalculatedColumnUtil.Unwrap(wrapped);
        Assert.Equal(declaredLength, payload.Length);
        return payload;
    }

    private static byte[] TextPayload(string value) => JetTypeInfo.EncodeJet4Text(value, int.MaxValue, compress: true);

    private static byte[] BooleanPayload(bool value) => [value ? (byte)0xFF : (byte)0x00];

    private static byte[] Int32Payload(object value)
    {
        byte[] bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, Convert.ToInt32(value, CultureInfo.InvariantCulture));
        return bytes;
    }

    private static string BuildDaoCalculatedBuiltinAuthoringScript(string dbPath)
    {
        string dbLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(dbPath);
        return $$"""
                $db = $engine.CreateDatabase({{dbLiteral}}, ';LANGID=0x0409;CP=1252;COUNTRY=0')
                try {
                    $db.Execute('CREATE TABLE [DomainPeople] ([Id] LONG, [Name] TEXT(40), [Score] LONG)')
                    $db.Execute('INSERT INTO [DomainPeople] ([Id], [Name], [Score]) VALUES (1, ''Alpha'', 10)')
                    $db.Execute('INSERT INTO [DomainPeople] ([Id], [Name], [Score]) VALUES (2, ''Beta'', 20)')

                    $db.Execute('CREATE TABLE [CalcBuiltins] ([Id] LONG, [Score] LONG)')

                    $tdf = $db.TableDefs('CalcBuiltins')

                    $field = $tdf.CreateField('IIfBand', 10, 16)
                    $field.Expression = 'IIf([Score]>=10,"High","Low")'
                    $tdf.Fields.Append($field)

                    $field = $tdf.CreateField('IsHigh', 1)
                    $field.Expression = 'IIf([Score]>=10,True,False)'
                    $tdf.Fields.Append($field)

                    $switchRejected = $false
                    try {
                        $field = $tdf.CreateField('SwitchBand', 10, 16)
                        $field.Expression = 'Switch([Score]>=90,"A",[Score]>=75,"B",True,"C")'
                        $tdf.Fields.Append($field)
                    } catch {
                        $switchRejected = $true
                        Write-Output "SWITCH_REJECTED=$($_.Exception.Message)"
                    }

                    if (-not $switchRejected) { throw 'DAO unexpectedly accepted Switch in a calculated column.' }

                    $domainAggregateCases = @(
                        @('DLookUp', 10, 40, 'DLookUp("Name", "DomainPeople", "Id=1")'),
                        @('DCount', 4, 0, 'DCount("*", "DomainPeople")'),
                        @('DSum', 5, 0, 'DSum("Score", "DomainPeople")'),
                        @('DAvg', 7, 0, 'DAvg("Score", "DomainPeople")'),
                        @('DMin', 4, 0, 'DMin("Score", "DomainPeople")'),
                        @('DMax', 4, 0, 'DMax("Score", "DomainPeople")')
                    )

                    $domainAggregateRejected = 0
                    foreach ($case in $domainAggregateCases) {
                        try {
                            if ($case[2] -gt 0) { $field = $tdf.CreateField("Domain$($case[0])", $case[1], $case[2]) } else { $field = $tdf.CreateField("Domain$($case[0])", $case[1]) }
                            $field.Expression = $case[3]
                            $tdf.Fields.Append($field)
                            Write-Output "DOMAIN_AGGREGATE_ACCEPTED $($case[0])"
                        } catch {
                            $domainAggregateRejected++
                            Write-Output "DOMAIN_AGGREGATE_REJECTED $($case[0])=$($_.Exception.Message)"
                        }
                    }

                    if ($domainAggregateRejected -ne $domainAggregateCases.Count) { throw 'DAO unexpectedly accepted a domain aggregate in a calculated column.' }

                    $field = $null
                    $tdf = $null

                    $db.Execute('INSERT INTO [CalcBuiltins] ([Id], [Score]) VALUES (1, 5)')
                    $db.Execute('INSERT INTO [CalcBuiltins] ([Id], [Score]) VALUES (2, 80)')
                    $db.Execute('INSERT INTO [CalcBuiltins] ([Id], [Score]) VALUES (3, 95)')
                } finally {
                    if ($db -ne $null) { try { $db.Close() } catch {} }
                }

                """;
    }

    private readonly record struct RawRowLayout(int NumCols, int NullMaskPos, int VarLen, int VarTableStart, int Eod);
}
