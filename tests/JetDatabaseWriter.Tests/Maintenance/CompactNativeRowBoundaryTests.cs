namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native record admission, fixed gaps and variable-offset edges establish bounded raw-copy evidence.</summary>
/// <param name="output">Records native geometry and complete private comparison evidence.</param>
public sealed class CompactNativeRowBoundaryTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Jet3", 2000, "DE9DD7E0A6B13A276589D51CC5D650EDAB45050718C012C9B932F6538E6C1639")]
    [InlineData("Jet4", 4051, "786F3C2A2ED5AACDCF46CBB16D906CA959649FA297D5A71C24801E4D0DE33857")]
    [InlineData("Ace", 4051, "554CE2012AAFE36FEC80A3A528F57A8511374F64D9152E1124EB44EBEFA14581")]
    public async Task FrozenNativeRows_CompleteAssemblyPreservesBoundaryBytesAndValues(string generation, int payload, string hash)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactNativeRows", databaseExtension: generation == "Ace" ? ".accdb" : ".mdb");
        byte[] original = await File.ReadAllBytesAsync(Fixture(generation), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(original)));
        await File.WriteAllBytesAsync(session.SourcePath, original, Ct);
        string staged = await this.BuildAsync(session);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        await using AccessReader destination = await TestDatabase.OpenReaderAsync(staged, cancellationToken: Ct);
        await AssertValuesAsync(source, payload);
        await AssertValuesAsync(destination, payload);
        foreach (string table in await source.ListTablesAsync(Ct))
        {
            Dictionary<int, byte[]> expected = await RawRowsAsync(source, table);

            // Original DATA stays pinned; compare the independently known appended raw-copy region.
            Dictionary<int, byte[]> actual = await RawRowsAsync(destination, table, source.Database.Pages.PageCount);
            Assert.Equal(expected.Keys.Order(), actual.Keys.Order());
            foreach (int id in expected.Keys)
            {
                Assert.Equal(expected[id], actual[id]);
            }
        }

        Dictionary<int, byte[]> fixedRows = await RawRowsAsync(source, "FixedOnly");
        Assert.Equal(Convert.FromHexString(generation == "Jet3" ? "0304030201060507" : "030004030201060507"), Assert.Single(fixedRows).Value);
        Dictionary<int, byte[]> limitRows = await RawRowsAsync(source, "Fixed" + payload);
        Assert.Equal(generation == "Jet3" ? 2003 : 4056, Assert.Single(limitRows).Value.Length);
        CatalogEntry dropped = Assert.IsType<CatalogEntry>(await source.GetCatalogEntryAsync("DroppedFixed", Ct));
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(dropped.TDefPage, Ct));
        Assert.Equal(3, Assert.Single(image.Columns, column => column.Name == "B").ColNum);
        Assert.Equal(12, Assert.Single(image.Columns, column => column.Name == "B").FixedOff);
        Dictionary<int, byte[]> droppedRows = await RawRowsAsync(source, "DroppedFixed");
        Assert.Equal(Convert.FromHexString("08070605"), droppedRows[1].AsSpan((generation == "Jet3" ? 1 : 2) + 8, 4).ToArray());
        if (generation == "Jet3")
        {
            Dictionary<int, byte[]> edges = await RawRowsAsync(source, "VariableEdges");
            Assert.Equal(256, edges[243].Length);
            Assert.Equal(258, edges[244].Length);
            Assert.Equal(Convert.FromHexString("FBFAF905FF030F"), edges[244].AsSpan(^7).ToArray());
            Assert.Equal(Convert.FromHexString("0100FF0502030F"), edges[250].AsSpan(^7).ToArray());
            Assert.Equal(512, edges[1244].Length);
            Assert.Equal(514, edges[1245].Length);
            Assert.Equal(Convert.FromHexString("FAF90405FF01030F"), edges[1245].AsSpan(^8).ToArray());
            Assert.Equal(Convert.FromHexString("010004050201030F"), edges[1252].AsSpan(^8).ToArray());
        }

        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
    }

    [Theory]
    [InlineData(240, 241)]
    [InlineData(1240, 1241)]
    public async Task VariableRowAssertions_RejectMissingRowReplacedByDuplicate(int missingId, int duplicateId)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(Fixture("Jet3"), cancellationToken: Ct);
        using DataTable edges = await reader.ReadTableAsync("VariableEdges", cancellationToken: Ct);
        AssertVariableRows(edges);
        DataRow missing = Assert.Single(edges.AsEnumerable(), row => (int)row["Id"] == missingId);
        DataRow duplicate = Assert.Single(edges.AsEnumerable(), row => (int)row["Id"] == duplicateId);
        edges.Rows.Remove(missing);
        edges.ImportRow(duplicate);
        Assert.Equal(32, edges.Rows.Count);
        Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => AssertVariableRows(edges));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Jet3NativeLimitsAndRows_Dao36AuthoringAndCompactAgree()
        => await this.NativeAsync("Jet3", 2000);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("Jet4")]
    [InlineData("Ace")]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeLimitsAndRows_Dao120AuthoringAndCompactAgree(string generation)
        => await this.NativeAsync(generation, 4051);

    private static string Fixture(string generation)
        => Path.Combine(AppContext.BaseDirectory, "Databases", "RowBoundaries", "NativeRowBoundaries" + generation + (generation == "Ace" ? ".accdb" : ".mdb"));

    private static async Task AssertValuesAsync(AccessReader reader, int payload)
    {
        using DataTable fixedOnly = await reader.ReadTableAsync("FixedOnly", cancellationToken: Ct);
        DataRow scalar = Assert.Single(fixedOnly.AsEnumerable());
        Assert.Equal(16909060, scalar["Id"]);
        Assert.Equal((short)1286, scalar["Small"]);
        Assert.Equal(true, scalar["Flag"]);
        using DataTable maximum = await reader.ReadTableAsync("Fixed" + payload, cancellationToken: Ct);
        DataRow limit = Assert.Single(maximum.AsEnumerable());
        Assert.Equal(1, limit["Id"]);
        int remaining = payload - 4;
        for (int index = 0; remaining > 0; index++)
        {
            int width = Math.Min(255, remaining);
            Assert.Equal(Enumerable.Repeat(checked((byte)(160 + index)), width), Assert.IsType<byte[]>(limit["B" + index]));
            remaining -= width;
        }

        using DataTable edges = await reader.ReadTableAsync("VariableEdges", cancellationToken: Ct);
        AssertVariableRows(edges);

        using DataTable dropped = await reader.ReadTableAsync("DroppedFixed", cancellationToken: Ct);
        Assert.Equal(2, dropped.Rows.Count);
        Assert.DoesNotContain(dropped.Columns.Cast<DataColumn>(), column => column.ColumnName == "Removed");
        foreach (DataRow row in dropped.Rows)
        {
            Assert.Equal(16909060, row["A"]);
            Assert.Equal(151653132, row["B"]);
            Assert.Equal((int)row["Id"] == 1 ? "before" : "after", row["Note"]);
        }
    }

    private static void AssertVariableRows(DataTable edges)
    {
        Assert.Equal(
            Enumerable.Range(240, 16).Concat(Enumerable.Range(1240, 16)),
            edges.AsEnumerable().Select(row => (int)row["Id"]).Order());
        foreach (DataRow row in edges.Rows)
        {
            int id = (int)row["Id"];
            Assert.InRange(id < 1000 ? id : id - 1000, 240, 255);
            Assert.Equal(new string('a', id < 1000 ? id : 255), row["A"]);
            Assert.Equal(new string('b', id < 1000 ? 1 : id - 1000), row["B"]);
            Assert.Equal("c", row["C"]);
        }
    }

    private static async Task<Dictionary<int, byte[]>> RawRowsAsync(AccessReader reader, string table, long minimumPage = 0)
    {
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync(table, Ct));
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
        var rows = new Dictionary<int, byte[]>();
        int prefix = reader.Database.Format.RowFields.NumCols;
        Encoding textEncoding = reader.Database.Format.IsJet3 ? Encoding.ASCII : Encoding.Unicode;
        await CompactRawRowReader.ScanAsync(
            reader.Database.Pages,
            reader.Database.Format,
            entry.TDefPage,
            new(),
            async (location, row, token) =>
            {
                int id = BinaryPrimitives.ReadInt32LittleEndian(row.Span[prefix..]);
                rows.Add(id, row.ToArray());
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        Assert.Equal<byte>(row.Span.Slice(field.RowOffset, field.RawBytes.Length), field.RawBytes.Span);
                        if (table == "VariableEdges" && field.Column.Name is "A" or "B" or "C")
                        {
                            string text = field.Column.Name switch
                            {
                                "A" => new('a', id < 1000 ? id : 255),
                                "B" => new('b', id < 1000 ? 1 : id - 1000),
                                _ => "c",
                            };
                            Assert.Equal(textEncoding.GetBytes(text), field.RawBytes.ToArray());
                            int expectedOffset = prefix + 4;
                            if (field.Column.Name != "A")
                            {
                                expectedOffset += textEncoding.GetByteCount(new string('a', id < 1000 ? id : 255));
                            }

                            if (field.Column.Name == "C")
                            {
                                expectedOffset += textEncoding.GetByteCount(new string('b', id < 1000 ? 1 : id - 1000));
                            }

                            Assert.Equal(expectedOffset, field.RowOffset);
                        }

                        return ValueTask.CompletedTask;
                    },
                    token);
            },
            Ct,
            location => location.PageNumber < minimumPage);
        return rows;
    }

    private static async IAsyncEnumerable<IndexEntry> EntriesAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private static string PrivatePath(string directory, CompactPinnedDatabaseFile role, long owner)
        => Path.Combine(directory, $"{role}-{owner}.bin");

#pragma warning disable CA2000 // Coordinator owns and closes every stream returned by these factories.
    private static ValueTask<Stream> CreateAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(new FileStream(PrivatePath(directory, role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));

    private static ValueTask<Stream> OpenAsync(string directory, CompactPinnedDatabaseFile role, long owner)
        => ValueTask.FromResult<Stream>(File.OpenRead(PrivatePath(directory, role, owner)));
#pragma warning restore CA2000

    private async Task<string> BuildAsync(AccessRoundTripSession session)
    {
        string directory = Path.Combine(session.WorkDir, "private");
        Directory.CreateDirectory(directory);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(session.SourcePath, cancellationToken: Ct);
        TDefImage catalog = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
        var decoder = new CatalogRowReader(source.Database.Format, source.Database.TableDefs, source.Database.OwnedPages);
        List<CatalogRow> entries = await decoder.GetCatalogRowsAsync(catalog.Definition, Ct);
        var tables = new List<CompactPinnedDatabaseBuildTable>();
        foreach (CatalogRow entry in entries.Where(static entry => entry.ObjectType == Constants.SystemObjects.UserTableType).OrderBy(static entry => entry.TDefPage))
        {
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
            var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
            for (int slot = 0; slot < indexes.Length; slot++)
            {
                TDefIndexDescriptor descriptor = image.RealIndexes[slot];
                Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
                List<IndexEntry> stored = await IndexLeafChain.ReadEntriesAsync(source.Database, entry.TDefPage, descriptor.RootPage, Ct);
                indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(stored, Ct));
            }

            tables.Add(new(entry.TDefPage, image, indexes));
        }

        var workspace = new CompactPinnedDatabaseWorkspace(
            (role, owner, _) => CreateAsync(directory, role, owner),
            (role, owner, _) => OpenAsync(directory, role, owner));
        CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(source.Database.Pages, source.Database.Format, tables, workspace, new(), Ct);
        Assert.Equal(tables.Count, result.Validation.Tables);
        Assert.Equal(source.Database.Pages.PageCount, result.Validation.Prefix.PinnedPages);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.ReclamationAuthorized);
        output.WriteLine($"{Path.GetExtension(session.SourcePath)}: {result.Validation.Tables} independently compared tables, {result.Validation.Prefix.PinnedPages} retained source pages.");
        string staged = session.CreateDatabasePath("staged");
        File.Copy(PrivatePath(directory, CompactPinnedDatabaseFile.Destination, 0), staged);
        return staged;
    }

    private async Task NativeAsync(string generation, int payload)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.CompactNativeRows.Native", databaseExtension: generation == "Ace" ? ".accdb" : ".mdb");
        byte[] original = await File.ReadAllBytesAsync(Fixture(generation), Ct);
        await File.WriteAllBytesAsync(session.SourcePath, original, Ct);
        string staged = await this.BuildAsync(session);
        string nativeLimit = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CreateDatabasePath("native-limit"));
        string stagedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(staged);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int createOption = generation switch { "Jet3" => 32, "Jet4" => 64, _ => 128 };
        string script = $$"""
            function Add-Fixed($db,[int]$payload) {
                $tdf=$db.CreateTableDef('N'+$payload)
                $tdf.Fields.Append($tdf.CreateField('Id',4))
                $left=$payload-4
                $i=0
                while($left -gt 0) {
                    $width=[Math]::Min(255,$left)
                    $field=$tdf.CreateField('B'+$i,9,$width)
                    $field.Attributes=1
                    $tdf.Fields.Append($field)
                    $left-=$width
                    $i++
                }
                $db.TableDefs.Append($tdf)
            }
            $db=$engine.CreateDatabase({{nativeLimit}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{createOption}})
            try {
                Add-Fixed $db {{payload}}
                $refused=$false
                try { Add-Fixed $db {{payload + 1}} } catch {
                    $codes=@($engine.Errors | ForEach-Object { $_.Number })
                    if($codes.Count -ne 1 -or $codes[0] -ne 3047) { throw }
                    $refused=$true
                }
                if(!$refused) { throw 'Native fixed schema first refusal changed' }
            } finally { $db.Close() }
            function Check-Rows([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('Fixed{{payload}}',2)
                    try {
                        if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 1) { throw 'Native maximum fixed row lost' }
                        $left={{payload - 4}}
                        $i=0
                        while($left -gt 0) {
                            $width=[Math]::Min(255,$left)
                            $value=[byte[]]$rs.Fields.Item('B'+$i).Value
                            if($value.Length -ne $width) { throw 'Native fixed field width changed' }
                            foreach($item in $value) { if($item -ne 160+$i) { throw 'Native fixed bytes changed' } }
                            $left-=$width
                            $i++
                        }
                    } finally { $rs.Close() }
                    $rs=$db.OpenRecordset('SELECT Id,A,B,C FROM VariableEdges ORDER BY Id',4)
                    try {
                        [int[]]$expectedIds=@(240..255)+@(1240..1255)
                        $count=0
                        while(!$rs.EOF) {
                            $id=[int]$rs.Fields.Item('Id').Value
                            if($count -ge $expectedIds.Length -or $id -ne $expectedIds[$count]) { throw "Native variable row identity changed at index $count" }
                            $alen=if($id -lt 1000){$id}else{255}
                            $blen=if($id -lt 1000){1}else{$id-1000}
                            if($rs.Fields.Item('A').Value -cne ('a'*$alen) -or $rs.Fields.Item('B').Value -cne ('b'*$blen) -or $rs.Fields.Item('C').Value -cne 'c') { throw 'Native variable value changed' }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne $expectedIds.Length) { throw 'Native variable row count changed' }
                    } finally { $rs.Close() }
                    $rs=$db.OpenRecordset('SELECT [Id],[A],[B],[Note] FROM DroppedFixed ORDER BY [Id]',4)
                    try {
                        foreach($id in 1,2) {
                            $note=if($id -eq 1){'before'}else{'after'}
                            if($rs.EOF -or $rs.Fields.Item('Id').Value -ne $id -or $rs.Fields.Item('A').Value -ne 16909060 -or $rs.Fields.Item('B').Value -ne 151653132 -or $rs.Fields.Item('Note').Value -cne $note) { throw 'Native dropped fixed value changed' }
                            $rs.MoveNext()
                        }
                        if(!$rs.EOF) { throw 'Extra native dropped fixed row' }
                    } finally { $rs.Close() }
                    $rs=$db.OpenRecordset('FixedOnly',4)
                    try { if($rs.EOF -or $rs.Fields.Item('Id').Value -ne 16909060 -or $rs.Fields.Item('Small').Value -ne 1286 -or !$rs.Fields.Item('Flag').Value) { throw 'Native fixed-only value changed' } } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Rows {{stagedPath}}
            $engine.CompactDatabase({{stagedPath}},{{compacted}})
            Check-Rows {{compacted}}
            Write-Output 'NATIVE_FIXED_LIMIT_RAW_ROW_COPY_AND_COMPACT=OK'
            """;
        if (generation == "Jet3")
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 native rows failed: {result.StandardOutput}\n{result.StandardError}");
            output.WriteLine(result.StandardOutput);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 native rows failed: {result.StdOut}\n{result.StdErr}");
            output.WriteLine(result.StdOut);
        }

        await using AccessReader reopened = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        await AssertValuesAsync(reopened, payload);
        Assert.Equal(original, await File.ReadAllBytesAsync(session.SourcePath, Ct));
    }
}
