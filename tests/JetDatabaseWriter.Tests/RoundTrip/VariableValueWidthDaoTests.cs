namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native field admission and index lookup independently constrain variable Text writes.</summary>
/// <param name="output">Records native engine evidence.</param>
public sealed class VariableValueWidthDaoTests(ITestOutputHelper output)
{
    [Theory(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable), SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public Task Dao36_WidthRefusal_RetainsNativeIndexAndCompact(DatabaseFormat format)
        => this.CheckAsync(format, useDao36: true);

    [Theory(Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason, SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable), SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_WidthRefusal_RetainsNativeIndexAndCompact(DatabaseFormat format)
        => this.CheckAsync(format, useDao36: false);

    private static string Literal(string value) => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value);

    private async Task CheckAsync(DatabaseFormat format, bool useDao36)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeWidths", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        this.RunEngine(
            $$"""
            $db=$engine.CreateDatabase({{Literal(session.SourcePath)}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE T ([Content] TEXT(5))',128)
                $db.Execute('CREATE UNIQUE INDEX IX ON T ([Content])',128)
                $rs=$db.OpenRecordset('T',2)
                try {
                    $rs.AddNew()
                    $refused=$false
                    try {
                        $rs.Fields.Item('Content').Value='ABCDEFG'
                        $rs.Update()
                    } catch {
                        if (@($engine.Errors | Where-Object {$_.Number -eq 3163}).Count -eq 0) { throw }
                        $refused=$true
                        $rs.CancelUpdate()
                    }
                    if (!$refused) { throw 'Native field accepted oversized Text' }
                    $rs.AddNew()
                    $rs.Fields.Item('Content').Value='ABCDE'
                    $rs.Update()
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output "ENGINE=$($engine.Version) WIDTH_REFUSAL=3163"
            Get-Process -Id $PID | ForEach-Object {$_.Modules} | Where-Object {$_.ModuleName -match '^(dao360|acedao)\.dll$'} | ForEach-Object {
                Write-Output "DAO_DLL=$($_.FileName) VERSION=$([Diagnostics.FileVersionInfo]::GetVersionInfo($_.FileName).FileVersion)"
            }
            """,
            session.WorkDir,
            useDao36);

        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(session.SourcePath, ct), ct);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            byte[] original = stream.ToArray();
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await writer.InsertRowAsync("T", ["ABCDEFG"], ct));
            Assert.Equal(JetErrorCode.ValueTooLarge, error.ErrorCode);
            Assert.Equal(original, stream.ToArray());
            await writer.InsertRowAsync("T", ["FGHIJ"], ct);
        }

        await File.WriteAllBytesAsync(session.SourcePath, stream.ToArray(), ct);
        this.RunEngine(
            $$"""
            function Assert-Values([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('T',1)
                    try {
                        $rs.MoveLast()
                        if($rs.RecordCount -ne 2){throw 'Refused value changed native row count'}
                        $rs.Index='IX'
                        foreach($key in @('ABCDE','FGHIJ')) {
                            $rs.Seek('=',$key)
                            if($rs.NoMatch -or $rs.Fields.Item('Content').Value -cne $key){throw "Native key mismatch: $key"}
                        }
                    } finally {$rs.Close()}
                } finally {$db.Close()}
            }
            Assert-Values {{Literal(session.SourcePath)}}
            $engine.CompactDatabase({{Literal(session.SourcePath)}},{{Literal(session.CompactedPath)}})
            Assert-Values {{Literal(session.CompactedPath)}}
            """,
            session.WorkDir,
            useDao36);

        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, ct);
        object[][] rows = await reader.Rows("T", cancellationToken: ct).ToArrayAsync(ct);
        Assert.Equal(["ABCDE", "FGHIJ"], rows.Select(static row => Assert.IsType<string>(row[0])).Order(StringComparer.Ordinal));
    }

    private void RunEngine(string script, string directory, bool useDao36)
    {
        if (useDao36)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, directory, TimeSpan.FromMinutes(2));
            output.WriteLine(result.StandardOutput);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}\n{result.StandardError}");
            return;
        }

        AccessRoundTripEnvironment.CompactResult ace = AccessRoundTripEnvironment.RunDaoEngineScript(script, directory, TimeSpan.FromMinutes(2));
        output.WriteLine(ace.StdOut);
        Assert.True(ace.ExitCode == 0, $"DAO120 failed: {ace.StdOut}\n{ace.StdErr}");
    }
}
