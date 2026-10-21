namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Native composite cascade graphs exercise each supported General collation family.</summary>
/// <param name="output">Receives the native outcomes and stored sort-order evidence.</param>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCompositeCascadeGraphTests(ITestOutputHelper output)
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets native format, stored collation and independently runnable graph cases.</summary>
    public static TheoryData<DatabaseFormat, byte, string, string> DaoCases
    {
        get
        {
            var cases = new TheoryData<DatabaseFormat, byte, string, string>();
            foreach ((DatabaseFormat format, byte version) in new (DatabaseFormat, byte)[] { (DatabaseFormat.Jet4Mdb, 0), (DatabaseFormat.AceAccdb, 0), (DatabaseFormat.AceAccdb, 1) })
            {
                foreach (string graph in new[] { "chain", "cycle", "diamond" })
                {
                    foreach (string newCode in new[] { "Bravo", "ALPHA", "DELETE" })
                    {
                        cases.Add(format, version, graph, newCode);
                    }
                }
            }

            return cases;
        }
    }

    /// <summary>DAO120 compares case-equivalent composite keys through chains, cycles and diamonds.</summary>
    /// <param name="format">The native file format.</param>
    /// <param name="version">The expected stored collation version.</param>
    /// <param name="graph">The graph topology.</param>
    /// <param name="newCode">The replacement text component, or DELETE for cascade deletion.</param>
    /// <returns>The asynchronous graph comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [MemberData(nameof(DaoCases))]
    public Task DaoAndWriter_CompositeGraphs_MatchThroughRollbackAndCompact(DatabaseFormat format, byte version, string graph, string newCode) =>
        this.CompareGraphAsync(format, version, new TextSortOrder(0x0409, version, true), graph, newCode);

    /// <summary>DAO3.6 independently verifies the same composite graphs with General97 keys.</summary>
    /// <param name="graph">The graph topology.</param>
    /// <param name="newCode">The replacement text component, or DELETE for cascade deletion.</param>
    /// <returns>The asynchronous graph comparison.</returns>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData("chain", "DELETE")]
    [InlineData("cycle", "DELETE")]
    [InlineData("diamond", "DELETE")]
    [InlineData("chain", "Bravo")]
    [InlineData("chain", "ALPHA")]
    [InlineData("cycle", "Bravo")]
    [InlineData("cycle", "ALPHA")]
    [InlineData("diamond", "Bravo")]
    [InlineData("diamond", "ALPHA")]
    public Task Dao36AndWriter_CompositeGraphs_MatchThroughRollbackAndCompact(string graph, string newCode) =>
        this.CompareGraphAsync(DatabaseFormat.Jet3Mdb, 0, new TextSortOrder(0x0409, 0, false), graph, newCode);

    private static string[] Tables(string graph) => graph == "diamond" ? ["A", "B", "C", "D"] : ["A", "B", "C"];

    private static string ReadNativeRows(string graph) => $$"""
        foreach($table in @({{string.Join(',', Tables(graph).Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral))}})) {
            $rs=$db.OpenRecordset(('SELECT Code,Part FROM ['+$table+']'),4)
            try {
                while(-not $rs.EOF) {
                    Write-Output ('ROW='+$table+'|'+$rs.Fields('Code').Value+'|'+$rs.Fields('Part').Value)
                    $rs.MoveNext()
                }
            } finally {$rs.Close()}
        }
        """;

    private static List<string> NativeRows(string result) =>
        result.Split('\n').Where(line => line.StartsWith("ROW=", StringComparison.Ordinal)).Select(line => line[4..].TrimEnd('\r')).Order(StringComparer.Ordinal).ToList();

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

    private static async Task<List<string>> ReadRowsAsync(string path, string graph, TextSortOrder order)
    {
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(path, cancellationToken: Ct);
        Assert.Equal(order, reader.Database.Format.DefaultTextSortOrder);
        var rows = new List<string>();
        foreach (string table in Tables(graph))
        {
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync(table, Ct));
            TableDef definition = Assert.IsType<TableDef>(await reader.ReadTableDefAsync(entry.TDefPage, Ct));
            Assert.Equal(order, Assert.Single(definition.Columns, column => column.Name == "Code").TextSortOrder);
            await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
            {
                rows.Add(table + "|" + Assert.IsType<string>(row[0]) + "|" + Assert.IsType<int>(row[1]).ToString(CultureInfo.InvariantCulture));
            }
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private async Task CompareGraphAsync(DatabaseFormat format, byte version, TextSortOrder order, string graph, string newCode)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string open;
        if (version == 1)
        {
            // This Access-authored 2010 host carries General's version-1 metadata.
            File.Copy(TestDatabases.TestIndexCodesV2010, session.SourcePath);
            open = "$db=$engine.OpenDatabase(" + source + ")";
        }
        else
        {
            int nativeVersion = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
            open = "$db=$engine.CreateDatabase(" + source + ",';LANGID=0x0409;CP=1252;COUNTRY=0'," + nativeVersion.ToString(CultureInfo.InvariantCulture) + ")";
        }

        string edges = graph switch
        {
            "chain" => "'A,B','B,C'",
            "cycle" => "'A,B','B,C','C,A'",
            _ => "'A,B','A,C','B,D','C,D'",
        };
        string setup = $$"""
            {{open}}
            try {
                $codes=@('Alpha','aLPHa','ALpha','alphA')
                $index=0
                foreach($name in @({{string.Join(',', Tables(graph).Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral))}})) {
                    $db.Execute(('CREATE TABLE ['+$name+'] (Code TEXT(40), Part LONG, CONSTRAINT [PK_'+$name+'] PRIMARY KEY (Code,Part))'),128)
                    foreach($row in @(("'"+$codes[$index]+"',1"), ("'"+$codes[$index]+"',2"), "'Other',1")) {
                        $db.Execute(('INSERT INTO ['+$name+'] VALUES ('+$row+')'),128)
                    }
                    $index++
                }
                foreach($edge in @({{edges}})) {
                    $parts=$edge.Split(',')
                    $rel=$db.CreateRelation(('FK_'+$parts[0]+'_'+$parts[1]),$parts[0],$parts[1],4352)
                    foreach($name in @('Code','Part')) {
                        $field=$rel.CreateField($name);$field.ForeignName=$name;$rel.Fields.Append($field)
                    }
                    $db.Relations.Append($rel)
                }
                Write-Output ('LCID='+$db.CollatingOrder)
            } finally {$db.Close()}
            """;
        string created = RunNative(session, format, setup);
        Assert.Contains("LCID=1033", created, StringComparison.Ordinal);
        List<string> beforeRows = await ReadRowsAsync(session.SourcePath, graph, order);
        string writerPath = session.CreateDatabasePath("writer");
        File.Copy(session.SourcePath, writerPath);
        bool delete = newCode == "DELETE";
        bool accepted = delete || graph != "diamond";
        string sql = delete ? "DELETE FROM A WHERE Code='Alpha' AND Part=1" : "UPDATE A SET Code='" + newCode + "',Part=5 WHERE Code='Alpha' AND Part=1";

        // Updates move the numeric component even when the text component compares equal.
        string update = $$"""
            try {
                $db.Execute({{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(sql)}},128)
                Write-Output 'MUTATION=ACCEPTED'
            } catch {
                $codes=@($engine.Errors | ForEach-Object {$_.Number})
                if($codes.Count -ne 1 -or $codes[0] -ne 3397){throw}
                Write-Output ('MUTATION=REFUSED:'+($codes -join ','))
            }
            """;
        string mutation = $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $workspace=$engine.Workspaces.Item(0)
                $workspace.BeginTrans()
                try { {{update}} } finally {$workspace.Rollback()}
                {{ReadNativeRows(graph)}}
                {{update}}
            } finally {$db.Close()}
            """;
        string native = RunNative(session, format, mutation);
        Assert.Equal(beforeRows, NativeRows(native));
        Assert.Equal(2, native.Split('\n').Count(line => line.StartsWith(accepted ? "MUTATION=ACCEPTED" : "MUTATION=REFUSED:", StringComparison.Ordinal)));
        List<string> expected = await ReadRowsAsync(session.SourcePath, graph, order);
        if (accepted)
        {
            var expectedRows = beforeRows.Where(row => !row.EndsWith("|1", StringComparison.Ordinal) || row.Contains("|Other|", StringComparison.Ordinal)).ToList();
            if (!delete)
            {
                expectedRows.AddRange(Tables(graph).Select(table => table + "|" + newCode + "|5"));
            }

            expectedRows.Sort(StringComparer.Ordinal);
            Assert.Equal(expectedRows, expected);
        }
        else
        {
            Assert.Equal(beforeRows, expected);
        }

        byte[] before = await File.ReadAllBytesAsync(writerPath, Ct);
        var values = new RowValues { ["Code"] = newCode, ["Part"] = 5 };
        RowCriteria criteria = RowCriteria.Where("Part", 1).And("Code", "Alpha");
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(writerPath, cancellationToken: Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            if (accepted)
            {
                Assert.Equal(1, await (delete ? writer.DeleteRowsAsync("A", criteria, Ct) : writer.UpdateRowsAsync("A", criteria, values, Ct)));
            }
            else
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("A", criteria, values, Ct));
            }

            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(writerPath, cancellationToken: Ct))
        {
            if (accepted)
            {
                Assert.Equal(1, await (delete ? writer.DeleteRowsAsync("A", criteria, Ct) : writer.UpdateRowsAsync("A", criteria, values, Ct)));
            }
            else
            {
                _ = await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.UpdateRowsAsync("A", criteria, values, Ct));
            }
        }

        if (!accepted)
        {
            Assert.Equal(before, await File.ReadAllBytesAsync(writerPath, Ct));
        }

        Assert.Equal(expected, await ReadRowsAsync(writerPath, graph, order));
        foreach (string path in new[] { session.SourcePath, writerPath })
        {
            string compacted = session.CreateDatabasePath("compacted");
            string literal = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(path);
            string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(compacted);
            string verification = $$"""
                $db=$engine.OpenDatabase({{literal}})
                try { {{ReadNativeRows(graph)}} } finally {$db.Close()}
                $engine.CompactDatabase({{literal}},{{destination}})
                $db=$engine.OpenDatabase({{destination}})
                try { {{ReadNativeRows(graph)}} } finally {$db.Close()}
                """;
            string verified = RunNative(session, format, verification);
            var twice = expected.Concat(expected).Order(StringComparer.Ordinal).ToList();
            Assert.Equal(twice, NativeRows(verified));
            Assert.Equal(expected, await ReadRowsAsync(compacted, graph, order));
        }

        string operation = delete ? "delete" : newCode + "/5";
        output.WriteLine($"{format} LCID=0x{order.Value:X4} version={order.Version} hasVersion={order.HasVersion}: {graph}, {operation}, {(accepted ? "accepted" : "refused:3397")}; native rollback/reopen/compact and writer rollback/bytes/reopen/compact verified.");
    }
}
