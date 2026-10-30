namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent native controls for calculated Text functions and stored dependencies.</summary>
public sealed class CalculatedTextFunctionDaoTests
{
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeTextFunctions_WriterRecalculationAndNativeCompactAgree()
    {
        (string? Text, double? Number)[] values = [(null, null), ("abcdef", 0.25d), (" \t x \n ", -0.25d), ("€Ÿ中", 1E100), ("ıßéς", 1.225d), ("ϴẞΩKÅ", 42d), ("œAEæ", -1.25d), ("𐐨𐐀", 1d)];
        (string Name, int Type, string Expression)[] definitions =
        [
            ("Length", 4, "Len([S])"),
            ("LeftText", 10, "Left([S],2.5)"),
            ("RightText", 10, "Right([S],2.5)"),
            ("Middle", 10, "Mid([S],1.5,2.5)"),
            ("Upper", 10, "UCase([S])"),
            ("Lower", 10, "LCase([S])"),
            ("Trimmed", 10, "Trim([S])"),
            ("Repeated", 10, "String(2,[S])"),
            ("Position", 4, "InStr(1,[S],\"oe\",1)"),
            ("NumericText", 10, "Str([N])"),
        ];
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.CalculatedTextFunctions");
        string subjectPath = Path.Combine(native.WorkDir, "writer.accdb");
        string subjectCompactedPath = Path.Combine(native.WorkDir, "writer-compacted.accdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string subject = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectPath);
        string sourceCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string subjectCompacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(subjectCompactedPath);
        string nativeDefinitions = string.Join(',', definitions.Select(static definition => $"@('{definition.Name}',{definition.Type},{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(definition.Expression)})"));
        string nativeValues = string.Join(',', values.Select(static value => $"@({AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value.Text is null ? "NULL" : Convert.ToBase64String(Encoding.Unicode.GetBytes(value.Text)))},{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(value.Number?.ToString("R", CultureInfo.InvariantCulture) ?? "NULL")})"));
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG,[S] TEXT(243),[N] DOUBLE)',128)
                $db.TableDefs.Refresh()
                $tdf=$db.TableDefs('T')
                foreach($definition in @({{nativeDefinitions}})) {
                    $field=$tdf.CreateField($definition[0],[int]$definition[1])
                    $field.Expression=[string]$definition[2]
                    $tdf.Fields.Append($field)
                }
            } finally { $db.Close() }
            Copy-Item -LiteralPath {{source}} -Destination {{subject}}
            $db=$engine.OpenDatabase({{source}})
            try {
                $rs=$db.OpenRecordset('T',2)
                try {
                    $values=@({{nativeValues}})
                    for($id=0;$id -lt $values.Count;$id++) {
                        $rs.AddNew()
                        $rs.Fields.Item('Id').Value=$id
                        if($values[$id][0] -eq 'NULL') { $rs.Fields.Item('S').Value=[DBNull]::Value }
                        else { $rs.Fields.Item('S').Value=[string][Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($values[$id][0])) }
                        if($values[$id][1] -eq 'NULL') { $rs.Fields.Item('N').Value=[DBNull]::Value }
                        else { $rs.Fields.Item('N').Value=[double]([double]::Parse($values[$id][1],[Globalization.CultureInfo]::InvariantCulture)) }
                        $rs.Update()
                    }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        object[][] expected = await RowsAsync(native.SourcePath);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(subjectPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowsAsync("T", values.Select((value, id) => new object?[] { id, value.Text, value.Number }.Concat(Enumerable.Repeat<object?>(null, definitions.Length)).ToArray()), TestContext.Current.CancellationToken);
        }

        Assert.Equal(expected, await RowsAsync(subjectPath));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(subjectPath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["S"] = "Æe", ["N"] = -1E100 }, TestContext.Current.CancellationToken));
        }

        AccessRoundTripEnvironment.CompactResult updated = native.RunDaoDatabaseScript(
            native.SourcePath,
            """
            $rs=$db.OpenRecordset('SELECT * FROM T WHERE Id=1',2)
            try { $rs.Edit(); $rs.Fields('S').Value=[string](''+[char]0xC6+'e'); $rs.Fields('N').Value=[double]-1E100; $rs.Update() }
            finally { $rs.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(updated.ExitCode == 0, updated.StdOut + updated.StdErr);
        expected = await RowsAsync(native.SourcePath);
        Assert.Equal(expected, await RowsAsync(subjectPath));
        AccessRoundTripEnvironment.CompactResult compacted = native.RunDaoEngineScript(
            $$"""
            function Rows([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rows=[Collections.Generic.List[object[]]]::new()
                    $rs=$db.OpenRecordset('SELECT * FROM T ORDER BY Id',2)
                    try {
                        while(!$rs.EOF) {
                            $row=[object[]]::new($rs.Fields.Count)
                            for($column=0;$column -lt $row.Length;$column++) { $row[$column]=$rs.Fields($column).Value }
                            $rows.Add($row)
                            $rs.MoveNext()
                        }
                    } finally { $rs.Close() }
                    return ,$rows
                } finally { $db.Close() }
            }
            function Compare-Rows([string]$left,[string]$right) {
                $a=Rows $left
                $b=Rows $right
                if($a.Count -ne $b.Count) { throw 'Native Text function row counts differ' }
                for($row=0;$row -lt $a.Count;$row++) {
                    for($column=0;$column -lt $a[$row].Length;$column++) {
                        if(![object]::Equals($a[$row][$column],$b[$row][$column])) { throw ('Native Text function mismatch at '+$row+':'+$column) }
                    }
                }
            }
            Compare-Rows {{source}} {{subject}}
            $engine.CompactDatabase({{source}},{{sourceCompacted}})
            $engine.CompactDatabase({{subject}},{{subjectCompacted}})
            Compare-Rows {{sourceCompacted}} {{subjectCompacted}}
            Write-Output 'NATIVE_TEXT_FUNCTIONS_UPDATED_AND_COMPACTED'
            """,
            TimeSpan.FromMinutes(2));
        Assert.True(compacted.ExitCode == 0, compacted.StdOut + compacted.StdErr);
        Assert.Contains("NATIVE_TEXT_FUNCTIONS_UPDATED_AND_COMPACTED", compacted.StdOut, StringComparison.Ordinal);
        Assert.Equal(expected, await RowsAsync(subjectCompactedPath));
    }

    private static async Task<object[][]> RowsAsync(string path)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        return (await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken)).OrderBy(static row => (int)row[0]).ToArray();
    }
}
