namespace JetDatabaseWriter.Tests.ValueEncoding;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native dropped scalar slots remain readable and writable beside trailing Nulls.</summary>
/// <param name="output">Records native producer and row-layout evidence.</param>
public sealed class NativeDroppedNullSlotTests(ITestOutputHelper output)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("jet3-source.mdb", "B9CB0072A843BC54B5DEF082E595A9E9C00A812A0D3DA4C4CDEDB7C4C77C6944", true)]
    [InlineData("jet3-compacted.mdb", "D6D7AFE586AFB97B679F2CA6A98D6D25BB324B9883051BF56CF2290A48B9A31A", false)]
    [InlineData("jet4-source.mdb", "ED7541E050837F6247D7D8CD11E6BFA9DD6FE7432268BF746ED83AC92F4A6ACB", true)]
    [InlineData("jet4-compacted.mdb", "D753BD46353F0797F330BB512D429A155BFCCD8F9341AC6C8D55256C862507E7", false)]
    public async Task FrozenNativeDroppedSlots_ReadAndMutateTrailingNulls(string name, string hash, bool hasGaps)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "DroppedNullSlots", name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var copy = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.FrozenDroppedNullSlots", databaseExtension: ".mdb");
        await File.WriteAllBytesAsync(copy.SourcePath, bytes, Ct);
        await AssertRowsAsync(copy.SourcePath, mutated: false);
        await this.AssertGeometryAsync(copy.SourcePath, hasGaps);
        await MutateAsync(copy.SourcePath);
        await AssertRowsAsync(copy.SourcePath, mutated: true);
        await this.AssertGeometryAsync(copy.SourcePath, hasGaps, mutated: true);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DroppedFixedAndVariableSlots_Dao36AcceptsWriterNullRowsAndCompact(DatabaseFormat format)
        => await this.AssertNativeAsync(format, dao36: true);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DroppedFixedAndVariableSlots_Dao120AcceptsWriterNullRowsAndCompact(DatabaseFormat format)
        => await this.AssertNativeAsync(format, dao36: false);

    private static async Task MutateAsync(string path)
    {
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        await writer.InsertRowAsync("Items", [3, null, null, "writer"], Ct);
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 1, new Dictionary<string, object?> { ["Note"] = "updated" }, Ct));
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 2, new Dictionary<string, object?> { ["TailValue"] = Guid.Parse("00112233-4455-6677-8899-aabbccddeeff") }, Ct));
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", "Id", 2, new Dictionary<string, object?> { ["TailValue"] = null }, Ct));
    }

    private static async Task AssertRowsAsync(string path, bool mutated)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        var rows = (await reader.Rows("Items", cancellationToken: Ct).ToListAsync(Ct)).OrderBy(static row => (int)row[0]).ToList();
        Assert.Equal(mutated ? 3 : 2, rows.Count);
        string[] notes = mutated ? ["updated", "after", "writer"] : ["before", "after"];
        for (int index = 0; index < rows.Count; index++)
        {
            Assert.Equal(index + 1, rows[index][0]);
            Assert.IsType<DBNull>(rows[index][1]);
            Assert.IsType<DBNull>(rows[index][2]);
            Assert.Equal(notes[index], rows[index][3]);
        }

        var text = (await reader.RowsAsStrings("Items", cancellationToken: Ct).ToListAsync(Ct)).OrderBy(static row => row[0], StringComparer.Ordinal).ToList();
        Assert.Equal(notes, text.Select(static row => row[3]));
        Assert.All(text, static row =>
        {
            Assert.Empty(row[1]);
            Assert.Empty(row[2]);
        });
        var pocos = (await reader.Rows<DroppedRow>("Items", cancellationToken: Ct).ToListAsync(Ct)).OrderBy(static row => row.Id).ToList();
        Assert.Equal(notes, pocos.Select(static row => row.Note));
        Assert.All(pocos, static row =>
        {
            Assert.Null(row.Value);
            Assert.Null(row.TailValue);
        });
    }

    private async Task AssertNativeAsync(DatabaseFormat format, bool dao36)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.DroppedNullSlots", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string baseline = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(Path.Combine(native.WorkDir, "native-compacted" + Path.GetExtension(native.SourcePath)));
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        string author = $$$"""
            Write-Output "ENGINE=$($engine.Version)"
            Get-Process -Id $PID | ForEach-Object { $_.Modules } | Where-Object { $_.ModuleName -match '^(dao360|acedao)\.dll$' } | ForEach-Object {
                Write-Output "DAO_DLL=$($_.FileName) VERSION=$([Diagnostics.FileVersionInfo]::GetVersionInfo($_.FileName).FileVersion)"
            }
            $db=$engine.CreateDatabase({{{source}}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{{version}}})
            try {
                $td=$db.CreateTableDef('Items')
                foreach($field in @(@('Id',4),@('RemovedHead',4),@('Value',7),@('RemovedMiddle',4),@('TailValue',15),@('RemovedTail',7))) {
                    $td.Fields.Append($td.CreateField([string]$field[0],[int]$field[1]))
                }
                $td.Fields.Append($td.CreateField('Note',10,40))
                $td.Fields.Append($td.CreateField('RemovedVariable',10,40))
                $db.TableDefs.Append($td)
                $db.Execute("INSERT INTO [Items] ([Id],[RemovedHead],[Value],[RemovedMiddle],[TailValue],[RemovedTail],[Note],[RemovedVariable]) VALUES (1,16909060,0,84281096,{guid {00112233-4455-6677-8899-aabbccddeeff}},123.5,'before','discarded')",128)
                $db.Execute('UPDATE Items SET [Value]=Null,TailValue=Null WHERE Id=1',128)
                foreach($name in @('RemovedHead','RemovedMiddle','RemovedTail','RemovedVariable')) { $db.Execute("ALTER TABLE Items DROP COLUMN [$name]",128) }
                $db.Execute("INSERT INTO [Items] ([Id],[Value],[TailValue],[Note]) VALUES (2,0,{guid {00112233-4455-6677-8899-aabbccddeeff}},'after')",128)
                $db.Execute('UPDATE Items SET [Value]=Null,TailValue=Null WHERE Id=2',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{{source}}},{{{baseline}}})
            """;
        this.RunNative(native, author, dao36);
        File.Copy(native.SourcePath, Path.Combine(native.WorkDir, "native-source" + Path.GetExtension(native.SourcePath)));
        await AssertRowsAsync(native.SourcePath, mutated: false);
        await this.AssertGeometryAsync(native.SourcePath, hasGaps: true);
        await AssertRowsAsync(Path.Combine(native.WorkDir, "native-compacted" + Path.GetExtension(native.SourcePath)), mutated: false);
        await this.AssertGeometryAsync(Path.Combine(native.WorkDir, "native-compacted" + Path.GetExtension(native.SourcePath)), hasGaps: false);
        await MutateAsync(native.SourcePath);
        await AssertRowsAsync(native.SourcePath, mutated: true);
        string validate = $$"""
            function Assert-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT [Id],[Value],[TailValue],[Note] FROM [Items] ORDER BY [Id]',2)
                    try {
                        $notes=@('updated','after','writer')
                        $id=1
                        while(!$rs.EOF) {
                            if($rs.Fields.Item('Id').Value -ne $id -or $rs.Fields.Item('Value').Value -isnot [DBNull] -or $rs.Fields.Item('TailValue').Value -isnot [DBNull] -or $rs.Fields.Item('Note').Value -cne $notes[$id-1]) { throw "Dropped Null slot mismatch at $id" }
                            $id++
                            $rs.MoveNext()
                        }
                        if($id -ne 4) { throw 'Dropped Null slot row count changed' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Values {{source}}
            $db=$engine.OpenDatabase({{source}})
            try { $db.Execute("UPDATE Items SET [Value]=Null,TailValue=Null WHERE Id=3",128) } finally { $db.Close() }
            Assert-Values {{source}}
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Values {{compacted}}
            """;
        this.RunNative(native, validate, dao36);
        await AssertRowsAsync(native.SourcePath, mutated: true);
        await AssertRowsAsync(native.CompactedPath, mutated: true);
    }

    private async Task AssertGeometryAsync(string path, bool hasGaps, bool mutated = false)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        long owner = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Items", Ct)).TDefPage;
        TDefImage image = Assert.IsType<TDefImage>(await reader.Database.TableDefs.ReadImageAsync(owner, Ct));
        ColumnInfo value = Assert.Single(image.Columns, static column => column.Name == "Value");
        ColumnInfo tail = Assert.Single(image.Columns, static column => column.Name == "TailValue");
        Assert.Equal(hasGaps ? 8 : 4, value.FixedOff);
        Assert.Equal(hasGaps ? 20 : 12, tail.FixedOff);
        Assert.Equal(hasGaps ? 2 : 1, value.ColNum);
        Assert.Equal(hasGaps ? 4 : 2, tail.ColNum);
        var session = new CompactDiscoverySession(reader.Database.Pages, reader.Database.Format, new());
        int rowCount = 0;
        await session.ScanRowsAsync(
            owner,
            async (location, row, token) =>
            {
                rowCount++;
                int noteFields = 0;
                int prefix = reader.Database.Format.RowFields.NumCols;
                int rowId = BinaryPrimitives.ReadInt32LittleEndian(row.Span[prefix..]);
                if (hasGaps && !mutated && rowId == 1)
                {
                    Assert.Equal(16909060, BinaryPrimitives.ReadInt32LittleEndian(row.Span[(prefix + 4)..]));
                    Assert.Equal(84281096, BinaryPrimitives.ReadInt32LittleEndian(row.Span[(prefix + 16)..]));
                    Assert.Equal(BitConverter.DoubleToInt64Bits(123.5), BinaryPrimitives.ReadInt64LittleEndian(row.Span[(prefix + 36)..]));
                }

                output.WriteLine($"{Path.GetFileName(path)} Raw={Convert.ToHexString(row.Span)}");
                await CompactRawFieldReader.ScanAsync(
                    reader.Database.Format,
                    image,
                    location,
                    row,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Note")
                        {
                            noteFields++;
                            int id = BinaryPrimitives.ReadInt32LittleEndian(row.Span[reader.Database.Format.RowFields.NumCols..]);
                            int fixedBytes = 28;
                            if (hasGaps)
                            {
                                fixedBytes = id == 1 && !mutated ? 44 : 36;
                            }

                            Assert.Equal(reader.Database.Format.RowFields.NumCols + fixedBytes, field.RowOffset);
                        }

                        return ValueTask.CompletedTask;
                    },
                    token);
                Assert.Equal(1, noteFields);
            },
            Ct);
        Assert.Equal(mutated ? 3 : 2, rowCount);
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

#pragma warning disable CA1812 // Public POCO row materialization constructs this projection.
    private sealed class DroppedRow
    {
        public int Id { get; set; }

        public double? Value { get; set; }

        public Guid? TailValue { get; set; }

        public string Note { get; set; } = string.Empty;
    }
#pragma warning restore CA1812
}
