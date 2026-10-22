namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Controlled native engines mutate and compact a private copy with its replacement global map installed.</summary>
public sealed class CompactGlobalMapNativeTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task InstalledMap_Dao120ReadsMutatesEnforcesAndCompacts(DatabaseFormat kind) => VerifyAsync(kind);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task InstalledMap_Dao36ReadsMutatesEnforcesAndCompacts() => VerifyAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyAsync(DatabaseFormat kind)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactGlobalMap", databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        await using var original = new MemoryStream();
        string initial = native.CreateDatabasePath("initial");
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            File.Copy(Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb"), initial);
        }

        await using (AccessWriter writer = kind == DatabaseFormat.Jet3Mdb
            ? await AccessWriter.OpenAsync(initial, new AccessWriterOptions { UseLockFile = false }, Ct)
            : await AccessWriter.CreateDatabaseAsync(initial, kind, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)], Ct);
            for (int id = 0; id < 3; id++)
            {
                await writer.InsertRowAsync("Items", [id, new string((char)('a' + id), 3000)], Ct);
            }
        }

        await original.WriteAsync(await File.ReadAllBytesAsync(initial, Ct), Ct);
        string control = native.CreateDatabasePath("control");
        await File.WriteAllBytesAsync(control, original.ToArray(), Ct);
        await using (AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct))
        await using (var destination = new MemoryStream())
        {
            await destination.WriteAsync(original.ToArray(), Ct);
            byte[] global = await source.Database.Pages.ReadPageCopyAsync(1, Ct);
            Assert.True(UsageMap.TryGetFirstRowBound(global, source.Database.Format.DataPage, source.Database.Format.PageSize, out RowBound row));
            long appendPoint = Math.Max((((long)row.RowSize - Constants.UsageMap.InlineMapHeaderSize) * 8) + 1, source.Database.Pages.PageCount);
            destination.SetLength(appendPoint * source.Database.Format.PageSize);
            await CompactGlobalMapInstaller.InstallAsync(source.Database.Pages, source.Database.Format, appendPoint, destination, new(), Ct);
            await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
            await CompactGlobalMapValidator.ValidateAsync(source.Database.Pages, source.Database.Format, appendPoint, reopened, new(), Ct);
            await File.WriteAllBytesAsync(native.SourcePath, destination.ToArray(), Ct);
        }

        string script = NativeScript(native, control);
        await File.WriteAllTextAsync(Path.Combine(native.WorkDir, "verification.ps1"), script, Ct);
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 global map mutation failed:\n{result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO global map mutation failed:\n{result.StdOut}\n{result.StdErr}");
        }

        await using AccessReader compacted = await TestDatabase.OpenReaderAsync(native.CompactedPath, cancellationToken: Ct);
        int count = 0;
        var seen = new HashSet<int>();
        await foreach (object[] row in compacted.Rows("Items", cancellationToken: Ct))
        {
            int id = Assert.IsType<int>(row[0]);
            Assert.True(seen.Add(id));
            Assert.True(id is 0 or 1 or (>= 101 and <= 219));
            string expected = id switch
            {
                0 => "updated",
                1 => new string('b', 3000),
                _ => new string((char)('a' + (id % 26)), 5000),
            };
            Assert.Equal(expected, Assert.IsType<string>(row[1]));
            count++;
        }

        Assert.Equal(121, count);
    }

    private static string NativeScript(AccessRoundTripSession native, string control)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string original = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control);
        return $$"""
            function Assert-Contents([string]$path, [bool]$mutated) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('SELECT Id,Memo FROM Items ORDER BY Id',2)
                    try {
                        $count=0
                        while(!$rs.EOF) {
                            [int]$id=$rs.Fields.Item('Id').Value
                            $expectedId=if(!$mutated -or $count -lt 2){$count}else{$count+99}
                            if($id -ne $expectedId) { throw "Native global map row identity mismatch: $path row=$count id=$id" }
                            $expected=if($mutated -and $id -eq 0){'updated'}elseif($id -lt 3){(''+[char](97+$id))*3000}else{(''+[char](97+($id%26)))*5000}
                            if(-not [string]::Equals([string]$rs.Fields.Item('Memo').Value,$expected,[StringComparison]::Ordinal)) { throw 'Native global map Memo mismatch' }
                            $count++
                            $rs.MoveNext()
                        }
                        $expectedCount=if($mutated){121}else{3}
                        if($count -ne $expectedCount) { throw "Native global map row count mismatch: $count" }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Contents {{original}} $false
            Assert-Contents {{source}} $false
            function Mutate([string]$path) {
            $db=$engine.OpenDatabase($path)
            try {
                $db.Execute("UPDATE Items SET Memo='updated' WHERE Id=0",128)
                $db.Execute('DELETE FROM Items WHERE Id=2',128)
                for($id=100;$id -lt 220;$id++) {
                    [string]$payload=(''+[char](97+($id%26)))*5000
                    $db.Execute("INSERT INTO Items VALUES ($id,'$payload')",128)
                }
                $db.Execute('DELETE FROM Items WHERE Id=100',128)
                $rejected=$false
                try { $db.Execute("INSERT INTO Items VALUES (0,'duplicate')",128) } catch {
                    if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                    $rejected=$true
                }
                if(!$rejected) { throw 'Native global map mutation accepted a duplicate primary key' }
            } finally { $db.Close() }
            }
            Mutate {{original}}
            Assert-Contents {{original}} $true
            $before=(Get-Item -LiteralPath {{source}}).Length
            Mutate {{source}}
            if((Get-Item -LiteralPath {{source}}).Length -le $before) { throw 'Native mutations did not exercise allocation beyond the installed map storage' }
            Assert-Contents {{source}} $true
            $engine.CompactDatabase({{source}},{{compacted}})
            Assert-Contents {{compacted}} $true
            Write-Output 'GLOBAL_MAP_NATIVE_MUTATION_OK'
            """;
    }
}
