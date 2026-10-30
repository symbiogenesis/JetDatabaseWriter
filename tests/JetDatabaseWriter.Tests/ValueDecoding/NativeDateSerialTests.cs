namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Writer snapshots retain date serials that DateTime cannot distinguish.</summary>
public sealed class NativeDateSerialTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native-date-serials.accdb", "51A349923E719E5320F32577994BFDD493E7A191F533546EE6B7C7FBD47340B4")]
    [InlineData("native-date-serials-compacted.accdb", "79A1A5878E325695FD4B8AF42C9275DA7AD7FDD28EFCEC84F7A60724E81FA07C")]
    public async Task UnrelatedUpdate_RetainsNegativeDateSerial(string fixture, string hash)
    {
        await using var stream = new MemoryStream();
        byte[] source = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeDateSerials", fixture), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(source)));
        await stream.WriteAsync(source, Ct);
        Dictionary<int, double> before = await ReadSerialsAsync(stream, "At");
        Assert.True(before[1] < 0d);
        Assert.True(before[2] > 0d);
        Assert.True(before[4] > before[2]);
        Assert.Equal(DateTime.FromOADate(before[2]), DateTime.FromOADate(before[4]));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "changed" }, Ct));
        }

        Assert.Equal(before.OrderBy(pair => pair.Key), (await ReadSerialsAsync(stream, "At")).OrderBy(pair => pair.Key));
        await AssertIndexAsync(stream);
    }

    [Fact]
    public async Task SchemaRewrite_RetainsStoredAndCalculatedDateSerials()
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeDateSerials", "native-date-serials.accdb"), Ct), Ct);
        Dictionary<int, double> ordinary = await ReadSerialsAsync(stream, "At");
        Dictionary<int, double> calculated = await ReadSerialsAsync(stream, "CalculatedAt");
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)), Ct);
        }

        Assert.Equal(ordinary.OrderBy(pair => pair.Key), (await ReadSerialsAsync(stream, "At")).OrderBy(pair => pair.Key));
        Assert.Equal(calculated.OrderBy(pair => pair.Key), (await ReadSerialsAsync(stream, "CalculatedAt")).OrderBy(pair => pair.Key));
        await AssertIndexAsync(stream);
    }

    [Fact]
    public void NativeSerial_ValueProjectionAndComparisonRetainDifferentIdentities()
    {
        var negative = NativeDate.FromSerial(-1d / 24d);
        var positive = NativeDate.FromSerial(1d / 24d);
        Assert.Equal(negative.Value, positive.Value);
        Assert.NotEqual(negative, positive);
        Assert.True(negative.CompareTo(positive) < 0);
        Assert.Equal(negative.Serial, Convert.ToDouble(negative, System.Globalization.CultureInfo.InvariantCulture));
        Assert.NotEqual(IndexKeyEncoder.EncodeEntry(Enums.ColumnType.DateTimeType, negative), IndexKeyEncoder.EncodeEntry(Enums.ColumnType.DateTimeType, positive));
        Assert.Equal(positive.Serial, NativeDate.FromDateTime(negative.Value).Serial);
        var table = new TableDef { Columns = [new ColumnInfo { Name = "At", Type = Enums.ColumnType.DateTimeType, Size = 8 }] };
        var equal = RowCriteriaEvaluator.Compile(RowCriteria.Where("At", positive.Value), table, "T", "criteria");
        Assert.True(equal.Matches([positive]));
        Assert.False(equal.Matches([negative]));
        var less = RowCriteriaEvaluator.Compile(RowCriteria.Where(ColumnPredicate.LessThan("At", DateTime.FromOADate(0))), table, "T", "criteria");
        Assert.True(less.Matches([negative]));
        Assert.False(less.Matches([positive]));
    }

    [Fact(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task WriterUpdate_NativeIndexedReadAndCompactRetainNegativeSerial()
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeDateSerials");
        File.Copy(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeDateSerials", "native-date-serials.accdb"), session.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "changed" }, Ct);
        }

        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs=$db.OpenRecordset('SELECT [Id],CDbl([At]) AS [Serial] FROM [T] WHERE [At]=TimeSerial(-1,0,0)')
            try {if($rs.EOF -or $rs.Fields('Id').Value -ne 1 -or $rs.Fields('Serial').Value -ge 0){throw 'Negative native date or index was changed'};$rs.MoveNext();if(!$rs.EOF){throw 'Positive date was incorrectly matched'}} finally {$rs.Close()}
            $db.Execute('UPDATE [T] SET [Label]="native" WHERE [Id]=1',128)
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        await using var compacted = new MemoryStream(await File.ReadAllBytesAsync(session.CompactedPath, Ct));
        Assert.Equal(-1d / 24d, (await ReadSerialsAsync(compacted, "At"))[1]);
    }

    private static async Task AssertIndexAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        object[][] rows = await reader.FromIndex("T", "AtIndex").ToRowsAsync(Ct).ToArrayAsync(Ct);
        Assert.Equal([1, 3, 2, 4], rows.Select(row => (int)row[0]));
        Assert.All(rows, row => Assert.IsType<DateTime>(row[1]));
    }

    private static async Task<Dictionary<int, double>> ReadSerialsAsync(MemoryStream stream, string columnName)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        CatalogEntry? entry = await reader.GetCatalogEntryAsync("T", Ct);
        TableDef? table = await reader.ReadTableDefAsync(entry!.TDefPage, Ct);
        JetFormat format = reader.Database.Format;
        var result = new Dictionary<int, double>();
        for (long number = 1; number < reader.Database.Pages.PageCount; number++)
        {
            byte[] page = await reader.ReadPageCopyAsync(number, Ct);
            if (page[0] != 1 || JetTypeInfo.Ri32(page, format.DataPage.TDefOff) != entry.TDefPage)
            {
                continue;
            }

            foreach (RowBound row in DataPageRows.EnumerateLiveRowBounds(format, page))
            {
                Assert.True(RowDecodePlan.TryParseRowLayout(format.RowFields, page, row.RowStart, row.RowSize, table!.HasVarColumns, out RowLayout layout));
                ColumnSlice id = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, row.RowStart, row.RowSize, layout, table.Columns[0]);
                ColumnInfo column = Assert.IsType<ColumnInfo>(table.FindColumn(columnName));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(format.RowFields, page, row.RowStart, row.RowSize, layout, column);
                int payload = row.RowStart + slice.DataStart + (column.IsCalculated ? Constants.CalculatedColumn.DataOffset : 0);
                result.Add(JetTypeInfo.Ri32(page, row.RowStart + id.DataStart), JetTypeInfo.ReadDoubleLittleEndian(page.AsSpan(payload, 8)));
            }
        }

        return result;
    }
}
