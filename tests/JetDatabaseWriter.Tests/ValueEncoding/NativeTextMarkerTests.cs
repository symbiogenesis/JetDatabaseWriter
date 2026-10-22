namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native variable Text controls distinguish literal leading markers from compressed storage.</summary>
/// <param name="output">Records complete native stored field bytes.</param>
public sealed class NativeTextMarkerTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void NativeTerminalLowByte_IsAcceptedOnlyForCalculatedText(int value)
    {
        byte[] bytes = [0xFF, 0xFE, 0, checked((byte)value)];
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        Assert.Equal(((char)value).ToString(), format.DecodeCalculatedText(bytes, 0, bytes.Length));
        Assert.Equal(string.Empty, JetFormat.DecodeJet4Text(bytes, 0, bytes.Length));
        Assert.Equal(string.Empty, format.DecodeMemoText(bytes, 0, bytes.Length, compressedStorage: true));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UncompressedText_LeadingMarkerSurvivesEveryReadSurfaceAndWriteBack(DatabaseFormat kind)
    {
        string[] values = ["\uFEFF", "\uFEFFtail", "\uFEFF中", "\uFEFF\u0100", "\uFEFF\0tail"];
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string), 255) { IsCompressedUnicode = false }], Ct);
            await writer.InsertRowsAsync("Items", values.Select(static (value, id) => new object?[] { id, value }), Ct);
        }

        await CheckReadSurfacesAsync(stream, values);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 0, new Dictionary<string, object?> { ["Id"] = 20 }, Ct));
        }

        await CheckReadSurfacesAsync(stream, values);
    }

    [Theory]
    [InlineData("native-calculated.accdb", "0A7109CE897614EA0AF897E5882532DBFCD35B08F7A2AD5340551412424369CA")]
    [InlineData("native-compacted.accdb", "8098D75FA0BDD45FDCF1D29E0103B2AAC83FF7564D971EB0D944B0603FC93D47")]
    public async Task FrozenNativeTextMarkers_MatchNativeStoredResultsAcrossReadSurfaces(string name, string hash)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeTextMarkers", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        string[] values = ["\uFEFF", "\uFEFFtail", "\uFEFF中", "\uFEFFΩ", "\uFEFF\u0100", "\uFEFF\u0200", "\uFEFF\0tail"];
        string[] cached = [string.Empty, "\uFEFFtail", "-N", "\u00A9\u0003", "\u0001", "\u0002", "tail"];
        await using var stream = new MemoryStream(bytes, writable: false);
        await CheckReadSurfacesAsync(stream, values, cached);
        if (name == "native-calculated.accdb")
        {
            await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
            long owner = Assert.IsType<JetDatabaseWriter.Catalog.Models.CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage;
            TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
            var session = new CompactDiscoverySession(reader.Database.Pages, reader.Database.Format, new());
            var payloads = new List<byte[]>();
            await session.ScanRowsAsync(
                owner,
                (location, row, token) => CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Cached")
                        {
                            payloads.Add(CalculatedColumnUtil.Unwrap(field.RawBytes.Span));
                        }

                        return default;
                    },
                    token),
                Ct);
            Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 1 }));
            Assert.Contains(payloads, static payload => payload.AsSpan().SequenceEqual(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0, 0x74, 0x61, 0x69, 0x6C }));
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CompressedTextLeadingMarker_RefusesInsertAndUpdateWithoutChangingDatabase(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Memo", typeof(string), 20_000), new("Content", typeof(string), 255)], Ct);
            await writer.InsertRowAsync("Items", [1, "retained", "original"], Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetLimitationException inserted = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.InsertRowAsync("Items", [2, new string('m', 5000), "\uFEFFtail"], Ct));
            Assert.Equal(JetErrorCode.FeatureNotSupported, inserted.ErrorCode);
            JetLimitationException updated = await Assert.ThrowsAsync<JetLimitationException>(async () =>
                await writer.UpdateRowsAsync("Items", "Id", 1, new Dictionary<string, object?> { ["Memo"] = new string('m', 5000), ["Content"] = "\uFEFFtail" }, Ct));
            Assert.Equal(JetErrorCode.FeatureNotSupported, updated.ErrorCode);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Fact]
    public async Task CalculatedTextLeadingMarker_RefusesInsertAndRecalculationWithoutChangingDatabase()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string), 40) { IsCompressedUnicode = false }, new("Cached", typeof(string), 40) { IsCalculated = true, CalculationExpression = "[Content]" }], Ct);
            await writer.InsertRowAsync("Items", [1, "original", null], Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync("Items", [2, "\uFEFFtail", null], Ct));
            await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.UpdateRowsAsync("Items", "Id", 1, new Dictionary<string, object?> { ["Content"] = "\uFEFFtail" }, Ct));
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, true)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeUncompressedLeadingMarker_ExactTextSurvivesManagedReadsAndNativeCompact(DatabaseFormat kind, bool nativeWrites, bool calculated)
    {
        string[] values = ["\uFEFF", "\uFEFFtail", "\uFEFF中", "\uFEFFΩ", "\uFEFF\u0100", "\uFEFF\u0200", "\uFEFF\0tail"];
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.NativeTextMarker", databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(native.SourcePath, kind, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Content", typeof(string), 255) { IsCompressedUnicode = false }], Ct);
            if (calculated)
            {
                await writer.AddColumnAsync("Items", new("Cached", typeof(string), 40) { IsCalculated = true, CalculationExpression = "[Content]" }, Ct);
            }

            if (!nativeWrites)
            {
                await writer.InsertRowsAsync("Items", values.Select((value, id) => calculated ? new object?[] { id, value, null } : [id, value]), Ct);
            }
        }

        string[] cached = [string.Empty, "\uFEFFtail", "-N", "\u00A9\u0003", "\u0001", "\u0002", "tail"];
        string expected = string.Join(',', values.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        string expectedCached = string.Join(',', cached.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string script = $$"""
            $values=@({{expected}})
            $cached=@({{expectedCached}})
            $db=$engine.OpenDatabase({{source}})
            try {
                for($id=0;{{(nativeWrites ? "$true" : "$false")}} -and $id -lt $values.Count;$id++) {
                    $value=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                    $expression=($value.ToCharArray() | ForEach-Object { 'ChrW('+([int]$_)+')' }) -join ' & '
                    $db.Execute("INSERT INTO Items (Id,Content) VALUES ($id,$expression)",128)
                }
            } finally { $db.Close() }
            function Check-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT Id,Content{{(calculated ? ",Cached" : string.Empty)}} FROM Items ORDER BY Id',2)
                    try {
                        $id=0
                        while(!$rs.EOF) {
                            $expected=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                            if($rs.Fields.Item('Id').Value -ne $id -or ![string]::Equals([string]$rs.Fields.Item('Content').Value,$expected,[StringComparison]::Ordinal)) { throw "Native leading marker changed at $id" }
                            if({{(calculated ? "$true" : "$false")}}) { $cachedExpected=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($cached[$id])); if(![string]::Equals([string]$rs.Fields.Item('Cached').Value,$cachedExpected,[StringComparison]::Ordinal)) { throw "Native cached Text changed at $id" } }
                            $id++
                            $rs.MoveNext()
                        }
                        if($id -ne $values.Count) { throw 'Native leading marker row count changed' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Values {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Check-Values {{compacted}}
            """;
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        output.WriteLine(result.StdOut);
        Assert.True(result.ExitCode == 0, $"Native Text marker control failed:\n{result.StdOut}\n{result.StdErr}");
        foreach (string path in new[] { native.SourcePath, native.CompactedPath })
        {
            await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
            List<object[]> actual = await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct);
            Assert.Equal(values, actual.OrderBy(static row => (int)row[0]).Select(static row => Assert.IsType<string>(row[1])));
            if (calculated)
            {
                Assert.Equal(cached, actual.OrderBy(static row => (int)row[0]).Select(static row => Assert.IsType<string>(row[2])));
            }
        }
    }

    private static async Task CheckReadSurfacesAsync(MemoryStream stream, string[] values, string[]? cached = null)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        string[] expected = values.Order(StringComparer.Ordinal).ToArray();
        List<object[]> typed = await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct);
        List<string[]> strings = await reader.RowsAsStrings("Items", cancellationToken: Ct).ToListAsync(Ct);
        List<TextRow> pocos = await reader.Rows<TextRow>("Items", cancellationToken: Ct).ToListAsync(Ct);
        using System.Data.DataTable table = await reader.ReadTableAsync("Items", cancellationToken: Ct);
        Assert.Equal(expected, typed.Select(static row => Assert.IsType<string>(row[1])).Order(StringComparer.Ordinal));
        Assert.Equal(expected, strings.Select(static row => row[1]).Order(StringComparer.Ordinal));
        Assert.Equal(expected, pocos.Select(static row => row.Content).Order(StringComparer.Ordinal));
        Assert.Equal(expected, table.Rows.Cast<System.Data.DataRow>().Select(static row => Assert.IsType<string>(row["Content"])).Order(StringComparer.Ordinal));
        if (cached is not null)
        {
            string[] expectedCached = cached.Order(StringComparer.Ordinal).ToArray();
            Assert.Equal(expectedCached, typed.Select(static row => Assert.IsType<string>(row[2])).Order(StringComparer.Ordinal));
            Assert.Equal(expectedCached, strings.Select(static row => row[2]).Order(StringComparer.Ordinal));
            Assert.Equal(expectedCached, pocos.Select(static row => row.Cached).Order(StringComparer.Ordinal));
            Assert.Equal(expectedCached, table.Rows.Cast<System.Data.DataRow>().Select(static row => Assert.IsType<string>(row["Cached"])).Order(StringComparer.Ordinal));
        }
    }

#pragma warning disable CA1812 // Public POCO materialization constructs this projection target.
    private sealed class TextRow
    {
        public string Content { get; set; } = string.Empty;

        public string Cached { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
