namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native engines match partial-Null composite keys and exempt only entirely Null tuples.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoPartialNullRelationshipTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>DAO120 verifies Jet4 and ACE relationship matching, cascades and refusal.</summary>
    /// <param name="format">The database format.</param>
    /// <returns>The asynchronous comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120_PartialNullKeys_MatchWriterAndCompact(DatabaseFormat format) => CompareAsync(format);

    /// <summary>DAO3.6 independently verifies Access97 relationship matching, cascades and refusal.</summary>
    /// <returns>The asynchronous comparison.</returns>
    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    public Task Dao36_PartialNullKeys_MatchWriterAndCompact() => CompareAsync(DatabaseFormat.Jet3Mdb);

    private static async Task CompareAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch
        {
            DatabaseFormat.Jet3Mdb => 32,
            DatabaseFormat.Jet4Mdb => 64,
            DatabaseFormat.AceAccdb => 128,
            _ => throw new ArgumentOutOfRangeException(nameof(format)),
        };
        VerifyAllNullParents(session, format, version);
        string setup = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $db.Execute('CREATE TABLE P (Id LONG CONSTRAINT PK_P PRIMARY KEY, A LONG, B LONG)',128)
                $db.Execute('CREATE UNIQUE INDEX UQ_P ON P (A,B)',128)
                $db.Execute('CREATE TABLE C (Id LONG CONSTRAINT PK_C PRIMARY KEY, PA LONG, PB LONG)',128)
                foreach ($row in @('1,1,1','2,NULL,1','3,1,NULL','4,NULL,NULL')) { $db.Execute(('INSERT INTO P VALUES ('+$row+')'),128) }
                foreach ($row in @('10,1,1','11,NULL,NULL','12,NULL,1','13,1,NULL')) { $db.Execute(('INSERT INTO C VALUES ('+$row+')'),128) }
                $rel = $db.CreateRelation('FK_C_P','P','C',4352)
                foreach ($name in @('A','B')) { $field=$rel.CreateField($name); $field.ForeignName='P'+$name; $rel.Fields.Append($field) }
                $db.Relations.Append($rel)
                $db.Execute('CREATE TABLE R (Id LONG CONSTRAINT PK_R PRIMARY KEY, A LONG, B LONG)',128)
                $db.Execute('CREATE UNIQUE INDEX UQ_R ON R (A,B)',128)
                $db.Execute('CREATE TABLE S (Id LONG CONSTRAINT PK_S PRIMARY KEY, PA LONG, PB LONG)',128)
                $db.Execute('INSERT INTO R VALUES (1,NULL,7)',128)
                $db.Execute('INSERT INTO S VALUES (1,NULL,7)',128)
                $rel=$db.CreateRelation('FK_S_R','R','S',0)
                foreach ($name in @('A','B')) { $field=$rel.CreateField($name); $field.ForeignName='P'+$name; $rel.Fields.Append($field) }
                $db.Relations.Append($rel)
                $db.Execute('CREATE TABLE Tree (A LONG,B LONG,PA LONG,PB LONG,CONSTRAINT PK_Tree PRIMARY KEY (A,B))',128)
                foreach ($row in @('1,1,1,1','2,1,1,1','3,1,NULL,NULL')) { $db.Execute(('INSERT INTO Tree VALUES ('+$row+')'),128) }
                $rel = $db.CreateRelation('FK_Tree_Self','Tree','Tree',256)
                foreach ($name in @('A','B')) { $field=$rel.CreateField($name); $field.ForeignName='P'+$name; $rel.Fields.Append($field) }
                $db.Relations.Append($rel)
            } finally { $db.Close() }
            """;
        RunNative(session, format, setup);
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        string operations = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                foreach ($sql in @('INSERT INTO C VALUES (20,NULL,9)','UPDATE C SET PA=NULL,PB=9 WHERE Id=10','UPDATE Tree SET A=5,PA=NULL WHERE A=1')) {
                    $refused = $false
                    try { $db.Execute($sql,128) } catch {
                        $codes=@($engine.Errors | ForEach-Object { $_.Number })
                        if ($codes.Count -ne 1 -or $codes[0] -ne 3201) { throw }
                        $refused=$true
                    }
                    if (-not $refused) { throw ('Expected missing-parent refusal: '+$sql) }
                }
                foreach ($sql in @('UPDATE Tree SET A=5,PA=NULL,PB=NULL WHERE A=1','INSERT INTO C VALUES (14,NULL,1)','INSERT INTO C VALUES (15,1,NULL)','UPDATE C SET PA=NULL WHERE Id=10','UPDATE C SET PA=NULL,PB=NULL WHERE Id=10','UPDATE P SET B=3 WHERE Id=2','UPDATE P SET A=5 WHERE Id=3','DELETE FROM P WHERE Id=2','DELETE FROM P WHERE Id=4')) { $db.Execute($sql,128) }
                foreach ($sql in @('UPDATE R SET B=8 WHERE Id=1','DELETE FROM R WHERE Id=1')) {
                    $refused=$false
                    try { $db.Execute($sql,128) } catch {
                        $codes=@($engine.Errors | ForEach-Object { $_.Number })
                        if ($codes.Count -ne 1 -or $codes[0] -ne 3200) { throw }
                        $refused=$true
                    }
                    if (-not $refused) { throw ('Expected referenced-parent refusal: '+$sql) }
                }
            } finally { $db.Close() }
            """;
        RunNative(session, format, operations);

        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("C", [20, null, 9], Ct));
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 10), new RowValues { ["PA"] = null, ["PB"] = 9 }, Ct));
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5, ["PA"] = null }, Ct));
            Assert.Equal(0, transaction.JournaledPageCount);
            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await using (AccessWriter writer = await AccessWriter.OpenAsync(writerPath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("Tree", RowCriteria.Where("A", 1), new RowValues { ["A"] = 5, ["PA"] = null, ["PB"] = null }, Ct));
            await writer.InsertRowsAsync("C", [[14, null, 1], [15, 1, null]], Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 10), new RowValues { ["PA"] = null }, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("C", RowCriteria.Where("Id", 10), new RowValues { ["PA"] = null, ["PB"] = null }, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 2), new RowValues { ["B"] = 3 }, Ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("P", RowCriteria.Where("Id", 3), new RowValues { ["A"] = 5 }, Ct));
            Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 2), Ct));
            Assert.Equal(1, await writer.DeleteRowsAsync("P", RowCriteria.Where("Id", 4), Ct));
            int pages = transaction.JournaledPageCount;
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("R", RowCriteria.Where("Id", 1), new RowValues { ["B"] = 8 }, Ct));
            _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.DeleteRowsAsync("R", RowCriteria.Where("Id", 1), Ct));
            Assert.Equal(pages, transaction.JournaledPageCount);
            await transaction.CommitAsync(Ct);
        }

        Dictionary<string, string[]> expected = new(StringComparer.Ordinal)
        {
            ["P"] = ["1|1|1", "3|5|"],
            ["C"] = ["13|5|", "15|5|"],
            ["Tree"] = ["2|1|5|1", "3|1||", "5|1||"],
            ["R"] = ["1||7"],
            ["S"] = ["1||7"],
        };
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string writerLiteral = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(writerPath);
        RunNative(session, format, $"$engine.CompactDatabase({writerLiteral}, {compacted})");
        foreach ((string table, string[] rows) in expected)
        {
            Assert.Equal(rows, await ReadRowsAsync(session.SourcePath, table));
            Assert.Equal(rows, await ReadRowsAsync(writerPath, table));
            Assert.Equal(rows, await ReadRowsAsync(session.CompactedPath, table));
            string read = $$"""
                $db=$engine.OpenDatabase({{compacted}})
                try {
                    $rs=$db.OpenRecordset('SELECT * FROM [{{table}}]',4)
                    try {
                        while(-not $rs.EOF) {
                            $values=for($i=0;$i -lt $rs.Fields.Count;$i++) { [string]$rs.Fields.Item($i).Value }
                            Write-Output ('ROW='+($values -join '|'))
                            $rs.MoveNext()
                        }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
                """;
            string output = RunNative(session, format, read);
            Assert.Equal(rows.Length, output.Split('\n').Count(line => line.StartsWith("ROW=", StringComparison.Ordinal)));
            foreach (string row in rows)
            {
                Assert.Contains("ROW=" + row, output, StringComparison.Ordinal);
            }
        }
    }

    private static void VerifyAllNullParents(AccessRoundTripSession session, DatabaseFormat format, int version)
    {
        string directory = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.WorkDir);
        string extension = format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb";
        string script = $$"""
            $caseCount=0
            foreach($width in @(1,2)) { foreach($duplicate in @($false,$true)) { foreach($cascade in @($false,$true)) { foreach($delete in @($false,$true)) {
                $path=Join-Path {{directory}} ('null-parent-'+[guid]::NewGuid().ToString('N')+'{{extension}}')
                $db=$engine.CreateDatabase($path,';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
                try {
                    $extra=if($width -eq 2){',B LONG'}else{''}
                    $columns=if($width -eq 2){'A,B'}else{'A'}
                    $nulls=if($width -eq 2){'NULL,NULL'}else{'NULL'}
                    $db.Execute(('CREATE TABLE P (Id LONG CONSTRAINT PK_P PRIMARY KEY,A LONG'+$extra+')'),128)
                    $db.Execute(('CREATE UNIQUE INDEX UQ_P ON P ('+$columns+')'),128)
                    $db.Execute(('CREATE TABLE C (Id LONG CONSTRAINT PK_C PRIMARY KEY,A LONG'+$extra+')'),128)
                    $db.Execute(('INSERT INTO P VALUES (1,'+$nulls+')'),128)
                    if($duplicate){$db.Execute(('INSERT INTO P VALUES (2,'+$nulls+')'),128)}
                    $db.Execute(('INSERT INTO C VALUES (10,'+$nulls+')'),128)
                    $rel=$db.CreateRelation('FK','P','C',$(if($cascade){4352}else{0}))
                    foreach($name in $columns.Split(',')) { $field=$rel.CreateField($name);$field.ForeignName=$name;$rel.Fields.Append($field) }
                    $db.Relations.Append($rel)
                    $sql=if($delete){'DELETE FROM P WHERE Id=1'}else{'UPDATE P SET A=8 WHERE Id=1'}
                    $refused=$false
                    try { $db.Execute($sql,128) } catch {
                        $codes=@($engine.Errors | ForEach-Object { $_.Number })
                        if($codes.Count -ne 1 -or $codes[0] -ne 3200){throw}
                        $refused=$true
                    }
                    if($refused -eq $cascade){throw 'Nullable parent dependency policy differs.'}
                    $rs=$db.OpenRecordset('SELECT * FROM C',4)
                    try {
                        if($cascade -and $delete) {
                            if(-not $rs.EOF){throw 'Cascade delete retained a Null foreign key.'}
                        } else {
                            if($rs.EOF){throw 'Nullable child disappeared.'}
                            $value=$rs.Fields('A').Value
                            if($cascade) { if($value -ne 8){throw 'Cascade update did not populate the Null foreign key.'} }
                            elseif($value -isnot [DBNull]){throw 'Restrictive refusal changed the child.'}
                            if($width -eq 2 -and $rs.Fields('B').Value -isnot [DBNull]){throw 'Unassigned Null component changed.'}
                            $rs.MoveNext()
                            if(-not $rs.EOF){throw 'Unexpected child row.'}
                        }
                    } finally { $rs.Close() }
                    $caseCount++
                } finally { $db.Close() }
            } } } }
            Write-Output ('NULL_PARENT_CASES='+$caseCount)
            """;
        Assert.Contains("NULL_PARENT_CASES=16", RunNative(session, format, script), StringComparison.Ordinal);
    }

    private static string RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, DaoTimeout);
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, result.StandardOutput + result.StandardError);
            return result.StandardOutput;
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(script, DaoTimeout);
        Assert.True(native.ExitCode == 0, native.StdOut + native.StdErr);
        return native.StdOut;
    }

    private static async Task<List<string>> ReadRowsAsync(string path, string table)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        var rows = new List<string>();
        await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
        {
            rows.Add(string.Join("|", row.Select(value => value is DBNull ? string.Empty : Convert.ToString(value, CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }
}
