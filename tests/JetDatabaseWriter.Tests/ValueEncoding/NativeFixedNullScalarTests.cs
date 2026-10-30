namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native scalar Null slots retain fixed-area geometry through writer edits and native compact.</summary>
/// <param name="output">Records independently authored and writer-emitted row geometry.</param>
public sealed class NativeFixedNullScalarTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeScalarNullSlots_Dao36MatchesWriterAndCompacts(DatabaseFormat databaseFormat)
        => await this.AssertNativeAsync(databaseFormat, dao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeScalarNullSlots_Dao120MatchesWriterAndCompacts(DatabaseFormat databaseFormat)
        => await this.AssertNativeAsync(databaseFormat, dao36: false);

    private async Task AssertNativeAsync(DatabaseFormat databaseFormat, bool dao36)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.FixedNullScalars", databaseExtension: databaseFormat == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        int version = databaseFormat switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(databaseFormat)),
        };
        string author = $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                foreach($table in @('FixedOnly','WithVariable')) {
                    $td=$db.CreateTableDef($table)
                    foreach($field in @(@('Id',4),@('ByteValue',2),@('IntegerValue',3),@('LongValue',4),@('CurrencyValue',5),@('SingleValue',6),@('DoubleValue',7),@('DateValue',8),@('GuidValue',15),@('Flag',1))) {
                        $td.Fields.Append($td.CreateField([string]$field[0],[int]$field[1]))
                    }
                    if($table -eq 'WithVariable') { $td.Fields.Append($td.CreateField('Tail',10,30)) }
                    $db.TableDefs.Append($td)
                    $rs=$db.OpenRecordset($table,2)
                    try {
                        $rs.AddNew()
                        $rs.Fields.Item('Id').Value=1
                        $rs.Fields.Item('Flag').Value=$true
                        if($table -eq 'WithVariable') { $rs.Fields.Item('Tail').Value='native' }
                        $rs.Update()
                    } finally { $rs.Close() }
                }
            } finally { $db.Close() }
            Write-Output "ENGINE=$($engine.Version)"
            """;
        this.RunNative(native, author, dao36);
        var nativeLengths = new Dictionary<string, int>();
        foreach (string table in new[] { "FixedOnly", "WithVariable" })
        {
            nativeLengths.Add(table, await this.AssertRawGeometryAsync(native.SourcePath, table, [1]));
        }

        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(native.SourcePath, cancellationToken: Ct))
        {
            object?[] fixedValues = [2, null, null, null, null, null, null, null, null, true];
            await writer.InsertRowAsync("FixedOnly", fixedValues, Ct);
            await writer.InsertRowAsync("WithVariable", [.. fixedValues, "writer"], Ct);
            foreach (string table in new[] { "FixedOnly", "WithVariable" })
            {
                Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["GuidValue"] = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff") }, Ct));
                Assert.Equal(1, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["GuidValue"] = null }, Ct));
            }
        }

        foreach (string table in new[] { "FixedOnly", "WithVariable" })
        {
            Assert.Equal(nativeLengths[table], await this.AssertRawGeometryAsync(native.SourcePath, table, [1, 2]));
        }

        string validate = $$"""
            function Assert-Scalars([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    foreach($table in @('FixedOnly','WithVariable')) {
                        $rs=$db.OpenRecordset("SELECT * FROM $table ORDER BY Id",2)
                        try {
                            $id=1
                            while(!$rs.EOF) {
                                if($rs.Fields.Item('Id').Value -ne $id -or !$rs.Fields.Item('Flag').Value) { throw 'Scalar row identity or Boolean changed' }
                                foreach($name in @('ByteValue','IntegerValue','LongValue','CurrencyValue','SingleValue','DoubleValue','DateValue','GuidValue')) {
                                    if($rs.Fields.Item($name).Value -isnot [DBNull]) { throw "Expected Null scalar $name" }
                                }
                                if($table -eq 'WithVariable') {
                                    $expected=if($id -eq 1){'native'}else{'writer'}
                                    if($rs.Fields.Item('Tail').Value -cne $expected) { throw 'Variable data moved into a Null fixed slot' }
                                }
                                $id++
                                $rs.MoveNext()
                            }
                            if($id -ne 3) { throw 'Scalar row count changed' }
                        } finally { $rs.Close() }
                    }
                } finally { $db.Close() }
            }
            Assert-Scalars {{source}}
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('UPDATE FixedOnly SET GuidValue=Null, LongValue=Null WHERE Id=2',128)
                $db.Execute("UPDATE WithVariable SET GuidValue=Null, LongValue=Null, Tail='writer' WHERE Id=2",128)
            } finally { $db.Close() }
            Assert-Scalars {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Scalars {{compacted}}
            """;
        this.RunNative(native, validate, dao36);
        foreach (string path in new[] { native.SourcePath, native.CompactedPath })
        {
            foreach (string table in new[] { "FixedOnly", "WithVariable" })
            {
                Assert.Equal(nativeLengths[table], await this.AssertRawGeometryAsync(path, table, [1, 2]));
            }
        }
    }

    private void RunNative(AccessRoundTripSession native, string script, bool dao36)
    {
        if (dao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StandardOutput);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StdOut);
            Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
        }
    }

    private async Task<int> AssertRawGeometryAsync(string path, string table, int[] expectedIds)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync(table, Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        JetFormat format = reader.Database.Format;
        ColumnInfo guid = Assert.Single(image.Columns, column => column.Name == "GuidValue");
        Assert.Equal(39, guid.FixedOff);
        Assert.Equal(16, guid.Size);
        int fixedEnd = format.RowFields.NumCols + 55;
        var ids = new List<int>();
        var lengths = new List<int>();
        var session = new CompactDiscoverySession(reader.Database.Pages, format, new());
        await session.ScanRowsAsync(
            owner,
            async (location, row, token) =>
            {
                int id = BinaryPrimitives.ReadInt32LittleEndian(row.Span[format.RowFields.NumCols..]);
                ids.Add(id);
                lengths.Add(table == "FixedOnly" ? fixedEnd : row.Length);
                output.WriteLine($"{Path.GetFileName(path)} {table} Id={id} Bytes={row.Length} Raw={Convert.ToHexString(row.Span)}");
                if (table == "WithVariable")
                {
                    await CompactRawFieldReader.ScanAsync(
                        format,
                        image,
                        location,
                        row,
                        (field, _) =>
                        {
                            if (field.Column.Name == "Tail")
                            {
                                Assert.Equal(fixedEnd, field.RowOffset);
                            }

                            return ValueTask.CompletedTask;
                        },
                        token);
                }
                else
                {
                    // Native fixed-only rows omit the writer's unused variable trailer.
                    Assert.True(row.Length >= fixedEnd + 2);
                }
            },
            Ct);
        Assert.Equal(expectedIds, ids.Order());
        Assert.Single(lengths.Distinct());
        return lengths[0];
    }
}
