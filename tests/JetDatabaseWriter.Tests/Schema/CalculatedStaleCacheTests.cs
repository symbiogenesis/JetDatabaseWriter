namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Stale bracketed qualifiers preserve caches and expose absent caches distinctly.</summary>
public sealed class CalculatedStaleCacheTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task NativeMissingCache_IsDistinctFromNullAndRetainsDependentStatus()
    {
        string path = Fixture("native-after.accdb");
        Assert.Equal("7F24BD7F93F53102D89D1E4F5050F9229851480395D7C07A64F6C8354A18F03C", Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(path, Ct))));
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, Ct);
        object[][] rows = (await reader.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        Assert.Equal(1, rows[0][2]);
        Assert.Equal(DBNull.Value, rows[1][2]);
        CalculatedError missing = Assert.IsType<CalculatedError>(rows[3][2]);
        Assert.Null(missing.NativeCode);
        Assert.Equal("#Error(Uncached)", missing.ToString());
        Assert.Equal(-5617, Assert.IsType<CalculatedError>(rows[3][7]).NativeCode);
        Assert.Equal(-5617, Assert.IsType<CalculatedError>(rows[3][9]).NativeCode);
        string[][] text = await reader.RowsAsStrings("Renamed", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal("#Error(Uncached)", Assert.Single(text, row => row[0] == "4")[2]);
        using DataTable table = await reader.ReadTableAsync("Renamed", cancellationToken: Ct);
        Assert.Equal(missing, Assert.Single(table.Rows.Cast<DataRow>(), row => (int)row[0] == 4)[2]);
        CachedRow[] mapped = await reader.Rows<CachedRow>("Renamed", cancellationToken: Ct).ToArrayAsync(Ct);
        Assert.Equal(missing, Assert.Single(mapped, row => row.Id == 4).V);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task StaleRequiredOrFieldRule_AcceptsPreservedValueAndRefusesMissingValue(bool required)
    {
        byte[] image = await File.ReadAllBytesAsync(Fixture("native-constraints.accdb"), Ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        string name = required ? "RequiredRenamed" : "FieldRuleRenamed";
        await writer.UpdateRowsAsync(name, RowCriteria.Where("N", 2), new RowValues { ["N"] = 4 }, Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync(name, new RowValues { ["N"] = 3 }, Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task StaleTableRule_RefusesLiveUnresolvableReferenceEvenWithPreservedValue()
    {
        byte[] image = await File.ReadAllBytesAsync(Fixture("native-constraints.accdb"), Ct);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        byte[] before = stream.ToArray();
        await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.UpdateRowsAsync("TableRuleRenamed", RowCriteria.Where("N", 1), new RowValues { ["N"] = 2 }, Ct));
        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task UpdateAndInsert_MatchNativeStoredCachesAfterTableRename()
    {
        byte[] before = await File.ReadAllBytesAsync(Fixture("native-before.accdb"), Ct);
        Assert.Equal("A474FEA80652FBE16DCD01D393DC5E6B2AE6708889ED308A826722AD68903C6B", Convert.ToHexString(SHA256.HashData(before)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(before, Ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.UpdateRowsAsync("Renamed", RowCriteria.Where("Id", 1), new RowValues { ["N"] = 5 }, Ct);
            await writer.UpdateRowsAsync("Renamed", RowCriteria.Where("Id", 3), new RowValues { ["N"] = 6 }, Ct);
            await writer.InsertRowAsync("Renamed", new RowValues { ["Id"] = 4, ["N"] = 8 }, Ct);
        }

        await using AccessReader expected = await AccessReader.OpenAsync(Fixture("native-after.accdb"), new AccessReaderOptions { UseLockFile = false }, Ct);
        await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        object[][] expectedRows = (await expected.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        object[][] actualRows = (await actual.Rows("Renamed", cancellationToken: Ct).ToArrayAsync(Ct)).OrderBy(row => row[0]).ToArray();
        Assert.Equal(expectedRows.Length, actualRows.Length);
        for (int index = 0; index < expectedRows.Length; index++)
        {
            Assert.Equal(expectedRows[index], actualRows[index]);
        }
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task EvaluatedNull_CarriesNativeNullEnvelopeInsteadOfAnAbsentCache()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedNullEnvelope");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("T", [new("N", typeof(int)), new("V", typeof(int)) { IsCalculated = true, CalculationExpression = "[N]" }], Ct);
            await writer.InsertRowAsync("T", new RowValues { ["N"] = DBNull.Value }, Ct);
        }

        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, Ct))
        {
            CatalogEntry? entry = await reader.Services.TableCatalog.GetCatalogEntryAsync("T", Ct);
            TableDef? table = await reader.Services.TableCatalog.ReadTableDefAsync(entry!.TDefPage, Ct);
            JetFormat format = reader.Database.Format;
            bool found = false;
            for (long number = 1; number < reader.Database.Pages.PageCount; number++)
            {
                byte[] page = await reader.ReadPageCopyAsync(number, Ct);
                if (page[0] != 1 || JetTypeInfo.Ri32(page, format.DataPage.TDefOff) != entry.TDefPage)
                {
                    continue;
                }

                foreach (RowBound bound in DataPageRows.EnumerateLiveRowBounds(format, page))
                {
                    Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, page, bound.RowStart, bound.RowSize, table!.HasVarColumns, out RowLayout layout));
                    ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, bound.RowStart, bound.RowSize, layout, table.Columns[1]);
                    Assert.Equal(23, slice.DataLen);
                    found = true;
                }
            }

            Assert.True(found);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(
            session.SourcePath,
            """
            $rs=$db.OpenRecordset('T')
            try {if($rs.Fields('V').Value -isnot [DBNull]){throw 'Calculated Null was written as a missing cache'}}finally{$rs.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
    }

    private static string Fixture(string name)
        => Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedStale", name);

#pragma warning disable CA1812 // Rows<T> creates this object projection through reflection.
    private sealed class CachedRow
    {
        public int Id { get; set; }

        public object? V { get; set; }
    }
#pragma warning restore CA1812
}
