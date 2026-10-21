namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using JetDatabaseWriter.ValueEncoding;
using JetDatabaseWriter.ValueEncoding.Models;
using Xunit;

/// <summary>Native Memo compression boundaries must agree on encoding and every materialized read path.</summary>
public sealed class NativeMemoCompressionTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("native.mdb", "F1D9A911859401DB5CAD80D40F637CE0819DA273318F37A388890FD16A8A4FC2")]
    [InlineData("native.accdb", "6CAC20346F956E10297D0D0EA994DA84BB28B4BDBAC2FC206689EBF4B6446C95")]
    public async Task NativeMemoFixture_ReadsStoredModesAndLiteralFeffExactly(string fileName, string sha256)
    {
        byte[] database = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "NativeMemo", fileName), Ct);
        Assert.Equal(sha256, Convert.ToHexString(SHA256.HashData(database)));
        int[] lengths = [10, 1023, 1024, 1025, 1494, 1500, 2000, 2023, 2024, 2025, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2047, 2048, 2049, 4094, 4095, 10_001, 10, 1024, 2048, 4096];
        string[] values = lengths.Select((length, index) => (index >= 28 ? "\uFEFF" : string.Empty) + new string('b', length)).ToArray();
        await using var stream = new MemoryStream(database);
        await AssertReadApisAsync(stream, values);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        var descriptors = new List<byte[]>();
        List<byte[]> payloads = await PayloadsAsync(reader, descriptors);
        Assert.True(LongValueDescriptor.TryRead(descriptors[5], out LongValueDescriptor compressed));
        Assert.True(compressed.IsSinglePage);
        Assert.Equal(new byte[] { 0xFF, 0xFE }, payloads[5][..2]);
        Assert.True(LongValueDescriptor.TryRead(descriptors[6], out LongValueDescriptor plain));
        Assert.False(plain.IsInline || plain.IsSinglePage);
        Assert.Equal(Encoding.Unicode.GetBytes(values[6]), payloads[6]);
        Assert.Equal(new byte[] { 0xFF, 0xFE, 0, 0xFF, 0xFE, 0 }, payloads[28][..6]);
        Assert.Equal(Encoding.Unicode.GetBytes(values[30]), payloads[30]);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task WriterMemoBoundary_DaoReadsExactValuesBeforeAndAfterCompact(DatabaseFormat kind)
    {
        int[] lengths = [62, 1022, 1023, 1024, 1025, 2046, 2047, 2048, 2049, 4093, 4094, 4095, 4096, 4097, 10_001];
        await using var native = AccessRoundTripSession.CreateEmpty(Path.Combine("compact-unblock-20261019-c3e2f074", "validation", "memo-compression"), databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using MemoryStream stream = await CreateAsync(kind, lengths.Select(static length => new string('b', length)));
        await File.WriteAllBytesAsync(native.SourcePath, stream.ToArray(), Ct);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string script = $$"""
            $lengths=@({{string.Join(',', lengths)}})
            function Assert-Memos([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT Id, Content FROM Items ORDER BY Id',2)
                    try {
                        $failures=0
                        $count=0
                        while(!$rs.EOF) {
                            $length=$lengths[$count]
                            $expected='b'*$length
                            $actual=[string]$rs.Fields.Item('Content').Value
                            if(![string]::Equals($actual,$expected,[StringComparison]::Ordinal)) {
                                $head=($actual.ToCharArray() | Select-Object -First 4 | ForEach-Object { '{0:X4}' -f [int]$_ }) -join ','
                                Write-Output "LENGTH=$length ACTUAL=$($actual.Length) HEAD=$head"
                                $failures++
                            }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne $lengths.Count -or $failures -ne 0) { throw "Native Memo mismatch: $failures/$count" }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Memos {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Memos {{compacted}}
            """;
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO Memo compression boundary failed:\n{result.StdOut}\n{result.StdErr}");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, 2048)]
    [InlineData(DatabaseFormat.Jet4Mdb, 2049)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4093)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4094)]
    [InlineData(DatabaseFormat.Jet4Mdb, 4095)]
    [InlineData(DatabaseFormat.AceAccdb, 4094)]
    [InlineData(DatabaseFormat.AceAccdb, 4095)]
    [InlineData(DatabaseFormat.AceAccdb, 10_001)]
    public async Task WriterMemoBoundary_UsesNativeStoredBytes(DatabaseFormat kind, int length)
    {
        string value = new('b', length);
        await using MemoryStream stream = await CreateAsync(kind, [value]);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        byte[] actual = Assert.Single(await PayloadsAsync(reader));
        byte[] expected = length <= 2048 ? [0xFF, 0xFE, .. Enumerable.Repeat((byte)'b', length)] : Encoding.Unicode.GetBytes(value);
        Assert.Equal(expected, actual);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoAuthoredMemos_ExposeNativeCompressionBoundary(DatabaseFormat kind)
    {
        int[] lengths = [10, 1023, 1024, 1025, 1494, 1500, 2000, 2023, 2024, 2025, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2047, 2048, 2049, 4094, 4095, 10_001, 10, 1024, 2048, 4096];
        string[] texts = [.. lengths.Select((length, index) => (index >= lengths.Length - 4 ? "\uFEFF" : string.Empty) + new string('b', length))];
        await using var native = AccessRoundTripSession.CreateEmpty(Path.Combine("compact-unblock-20261019-c3e2f074", "validation", "memo-oracle"), databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using MemoryStream empty = await CreateAsync(kind, []);
        await File.WriteAllBytesAsync(native.SourcePath, empty.ToArray(), Ct);
        string encodedValues = string.Join(',', texts.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        string script = $$"""
            $values=@({{encodedValues}})
            Write-Output "ENGINE=$($engine.Version)"
            Get-Process -Id $PID | ForEach-Object { $_.Modules } | Where-Object { $_.ModuleName -match 'acedao' } | ForEach-Object {
                Write-Output "DAO_DLL=$($_.FileName) VERSION=$([Diagnostics.FileVersionInfo]::GetVersionInfo($_.FileName).FileVersion)"
            }
            for($id=0;$id -lt $values.Count;$id++) {
                $text=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                $expression=if($text.Contains([char]0)){"ChrW(65279) & ChrW(0) & 'tail'"}else{"'"+$text.Replace("'","''")+"'"}
                $db.Execute("INSERT INTO Items (Id, Content) VALUES ($id, $expression)",128)
            }
            $rs=$db.OpenRecordset('SELECT Id, Content FROM Items ORDER BY Id',2)
            try {
                $id=0
                while(!$rs.EOF) {
                    $expected=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                    if(![string]::Equals([string]$rs.Fields.Item('Content').Value,$expected,[StringComparison]::Ordinal)) { throw "Native-authored Memo reread mismatch at $id" }
                    $id++
                    $rs.MoveNext()
                }
                if($id -ne $values.Count) { throw 'Native-authored Memo row count mismatch' }
            } finally { $rs.Close() }
            """;
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoDatabaseScript(native.SourcePath, script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO Memo boundary authoring failed:\n{result.StdOut}\n{result.StdErr}");
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(native.SourcePath, cancellationToken: Ct);
        var descriptors = new List<byte[]>();
        List<byte[]> payloads = await PayloadsAsync(reader, descriptors);
        string evidence = Path.Combine(Path.GetTempPath(), "compact-unblock-20261019-c3e2f074", "validation", "memo-native-evidence-" + kind + "-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(evidence);
        byte[] database = await File.ReadAllBytesAsync(native.SourcePath, Ct);
        await File.WriteAllBytesAsync(Path.Combine(evidence, "native" + Path.GetExtension(native.SourcePath)), database, Ct);
        for (int index = 0; index < payloads.Count; index++)
        {
            await File.WriteAllBytesAsync(Path.Combine(evidence, $"payload-{index}.bin"), payloads[index], Ct);
        }

        string manifest = JsonSerializer.Serialize(new
        {
            Format = kind.ToString(),
            NativeInfo = result.StdOut,
            Sha256 = Convert.ToHexString(SHA256.HashData(database)),
            Values = payloads.Select((payload, index) => new { Id = index, Characters = texts[index].Length, HasLiteralFeff = texts[index].StartsWith('\uFEFF'), StoredBytes = payload.Length, Descriptor = Convert.ToHexString(descriptors[index]), Prefix = Convert.ToHexString(payload.AsSpan(0, Math.Min(20, payload.Length))) }),
        });
        await File.WriteAllTextAsync(Path.Combine(evidence, "manifest.json"), manifest, Ct);
        List<object[]> rows = await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct);
        Assert.Equal(texts, rows.Select(static row => Assert.IsType<string>(row[1])));
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task WriterLiteralFeff_DaoReadsEveryValueAndCompactedCopy(DatabaseFormat kind, bool compress)
    {
        string[] values = ["\uFEFF" + new string('b', 2048), "\uFEFF" + new string('b', 4096), "\uFEFF\u0100\u0200\0" + new string('b', 4096)];
        await using var native = AccessRoundTripSession.CreateEmpty(Path.Combine("compact-unblock-20261019-c3e2f074", "validation", "memo-feff"), databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using MemoryStream stream = await CreateAsync(kind, values, compress);
        await File.WriteAllBytesAsync(native.SourcePath, stream.ToArray(), Ct);
        string expected = string.Join(',', values.Select(static value => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Convert.ToBase64String(Encoding.Unicode.GetBytes(value)))));
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string script = $$"""
            $values=@({{expected}})
            function Assert-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT Id, Content FROM Items ORDER BY Id',2)
                    try {
                        $id=0
                        while(!$rs.EOF) {
                            $expected=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id]))
                            if(![string]::Equals([string]$rs.Fields.Item('Content').Value,$expected,[StringComparison]::Ordinal)) { $actual=[string]$rs.Fields.Item('Content').Value; $head=($actual.ToCharArray() | Select-Object -First 12 | ForEach-Object { '{0:X4}' -f [int]$_ }) -join ','; throw "Native literal FEFF Memo mismatch at $id length=$($actual.Length) head=$head" }
                            $id++
                            $rs.MoveNext()
                        }
                        if($id -ne $values.Count) { throw 'Native literal FEFF Memo row count mismatch' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Values {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Values {{compacted}}
            """;
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO literal FEFF Memo failed:\n{result.StdOut}\n{result.StdErr}");
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WriterLiteralFeff_EveryReadApiAndWriteBackPreserveExactValues(DatabaseFormat kind)
    {
        string[] values = ["\uFEFF" + new string('b', 2048), "\uFEFF" + new string('b', 4096), "\uFEFF\u0100\u0200\0" + new string('b', 4096)];
        await using MemoryStream stream = await CreateAsync(kind, values);
        await AssertReadApisAsync(stream, values);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 2, new Dictionary<string, object?> { ["Id"] = 10 }, Ct));
        }

        await AssertReadApisAsync(stream, values);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await writer.RenameColumnAsync("Items", "Id", "Identifier", Ct);
        }

        await AssertReadApisAsync(stream, values);
    }

    [Theory]
    [InlineData("\uFEFF")]
    [InlineData("\uFEFFbbbb")]
    [InlineData("\uFEFF中")]
    [InlineData("\uFEFFΩ")]
    [InlineData("\uFEFF\u0100")]
    [InlineData("\uFEFF\u0200")]
    [InlineData("\uFEFF\0tail")]
    public async Task WriterAmbiguousLiteralFeff_RefusesBeforeAllocating(string value)
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb, []);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync("Items", [0, value], Ct));
            Assert.Equal(JetErrorCode.FeatureNotSupported, error.ErrorCode);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(20)]
    [InlineData(21)]
    [InlineData(2000)]
    public async Task CalculatedMemo_PlainEnvelopePreservesLiteralFeffAcrossReadApis(int length)
    {
        string value = "\uFEFF" + new string('b', length - 1);
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string)) { IsCalculated = true, CalculationExpression = "\"" + value + "\"" }], Ct);
            await writer.InsertRowAsync("Items", [0, value], Ct);
        }

        await AssertReadApisAsync(stream, [value]);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        byte[] wrapped = Assert.Single(await PayloadsAsync(reader));
        Assert.Equal(Encoding.Unicode.GetBytes(value), CalculatedColumnUtil.Unwrap(wrapped));
        Assert.Equal(23 + (length * 2), wrapped.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ForcedMemoAndCalculatedPreparation_UseTheSameNativeRules(bool compress)
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb, []);
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        var encoder = new LongValueEncoder(writer.Database.Format, writer.Pager, writer.Services.PageAllocator, new AccessWriterOptions());
        string value = new('b', 10_001);
        PreEncodedLongValue encoded = Assert.IsType<PreEncodedLongValue>(await encoder.ForceEncodeMemoAsLvalAsync(value, compress, 0, 0, Ct));
        using var cache = new ReaderPageCache(writer.Database.Format, writer.Database.Pages, 0);
        var decoder = new LongValueDecoder(writer.Database.Format, cache);
        Assert.Equal(Encoding.Unicode.GetBytes(value), await decoder.ReadLongValueBytesExactAsync(encoded.HeaderBytes, 0, encoded.HeaderBytes.Length, Ct));
        var column = new ColumnInfo { Name = "Content", Type = ColumnType.MemoType, ExtraFlags = Constants.CalculatedColumn.ExtFlagMask | Constants.CompressedUnicodeExtFlagMask, CalculatedResultType = ColumnType.MemoType };
        object[] prepared = encoder.PrepareLongValues(new TableDef { Columns = [column] }, ["\uFEFF" + value]);
        byte[] wrapped = Assert.IsType<PreEncodedLongValue>(prepared[0]).PendingPayload!;
        Assert.Equal(Encoding.Unicode.GetBytes("\uFEFF" + value), CalculatedColumnUtil.Unwrap(wrapped));
    }

    private static async Task AssertReadApisAsync(MemoryStream stream, string[] values)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        string[] expected = values.Order(StringComparer.Ordinal).ToArray();
        Assert.Equal(expected, (await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct)).Select(static row => Assert.IsType<string>(row[1])).Order(StringComparer.Ordinal));
        Assert.Equal(expected, (await reader.RowsAsStrings("Items", cancellationToken: Ct).ToListAsync(Ct)).Select(static row => row[1]).Order(StringComparer.Ordinal));
        Assert.Equal(expected, (await reader.Rows<MemoRow>("Items", cancellationToken: Ct).ToListAsync(Ct)).Select(static row => row.Content).Order(StringComparer.Ordinal));
        Assert.Equal(expected, (await reader.ReadTableAsync("Items", cancellationToken: Ct)).Rows.Cast<System.Data.DataRow>().Select(static row => Assert.IsType<string>(row["Content"])).Order(StringComparer.Ordinal));
    }

    private static async Task<MemoryStream> CreateAsync(DatabaseFormat kind, IEnumerable<string> values, bool compress = true)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Content", typeof(string), 20_000) { IsCompressedUnicode = compress }], Ct);
            await writer.InsertRowsAsync("Items", values.Select(static (value, id) => new object?[] { id, value }), Ct);
        }

        return stream;
    }

    private static async Task<List<byte[]>> PayloadsAsync(AccessReader reader, List<byte[]>? descriptors = null)
    {
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct));
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
        ColumnInfo column = Assert.Single(image.Columns, static column => column.Name == "Content");
        using var cache = new ReaderPageCache(reader.Database.Format, reader.Database.Pages, 16);
        var decoder = new LongValueDecoder(reader.Database.Format, cache);
        var payloads = new List<byte[]>();
        await reader.Database.OwnedPages.ForEachLiveTableRowAsync(
            entry.TDefPage,
            async (row, token) =>
            {
                Assert.True(RowDecodePlan.TryParseRowLayout(reader.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, hasVarColumns: true, out RowLayout layout));
                ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(reader.Database.Format.RowFields, row.Page, row.Location.RowStart, row.Location.RowSize, layout, column);
                descriptors?.Add(row.Page.AsSpan(row.Location.RowStart + slice.DataStart, Constants.LongValue.HeaderSize).ToArray());
                payloads.Add(await decoder.ReadLongValueBytesExactAsync(row.Page, row.Location.RowStart + slice.DataStart, slice.DataLen, token));
                return true;
            },
            Ct,
            requireCompleteRows: true);
        return payloads;
    }

#pragma warning disable CA1812 // Public POCO row materialization constructs this projection.
    private sealed class MemoRow
    {
        public string Content { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
