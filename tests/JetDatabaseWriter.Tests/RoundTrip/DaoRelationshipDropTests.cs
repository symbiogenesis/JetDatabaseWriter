namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Compares related-table deletion with Microsoft's Access engine.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoRelationshipDropTests
{
    private const string DropMarker = "DROP=";
    private const string Refused = DropMarker + "REFUSED";
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoTableDefsDelete_OfRelatedNorthwindTable_IsRefused()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(Ct);

        Assert.StartsWith(Refused, RunTableDefsDelete(session, "Companies"), StringComparison.Ordinal);
        await AssertWriterRefusesAsync(session, "Companies");
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoTableDefsDelete_OfTableInRelationshipWithoutIntegrity_IsDeleted()
    {
        const string parent = "DaoDtParent";
        const string child = "DaoDtChild";
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync(parent, [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync(child, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(
                new RelationshipDefinition("FK_DaoDtChild_DaoDtParent", parent, "Id", child, "ParentId") { EnforceReferentialIntegrity = false },
                Ct);
            await writer.CreateTableAsync("WriterParent", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("WriterChild", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_WriterChild", "WriterParent", "Id", "WriterChild", "ParentId") { EnforceReferentialIntegrity = false }, Ct);
        }

        File.Copy(session.SourcePath, Path.Combine(session.WorkDir, "before-delete.accdb"));
        const string preflightScript = """
            foreach ($name in @('DaoDtParent', 'DaoDtChild')) {
                $tdf = $db.TableDefs($name)
                Write-Output "TABLE=$name FIELDS=$($tdf.Fields.Count) INDEXES=$($tdf.Indexes.Count)"
                $rs = $db.OpenRecordset($name, 2)
                try { Write-Output "READ=$name OK" } finally { $rs.Close() }
            }
            """;
        AccessRoundTripEnvironment.CompactResult preflight = session.RunDaoDatabaseScript(session.SourcePath, preflightScript, DaoTimeout);
        Assert.True(preflight.ExitCode == 0, $"DAO pre-delete failed: {preflight.StdOut}\n{preflight.StdErr}");
        Assert.Contains("TABLE=DaoDtParent FIELDS=1 INDEXES=1", preflight.StdOut, StringComparison.Ordinal);
        Assert.Contains("TABLE=DaoDtChild FIELDS=2 INDEXES=1", preflight.StdOut, StringComparison.Ordinal);
        Assert.Contains("READ=DaoDtParent OK", preflight.StdOut, StringComparison.Ordinal);
        Assert.Contains("READ=DaoDtChild OK", preflight.StdOut, StringComparison.Ordinal);
        Assert.Equal(DropMarker + "DELETED", RunTableDefsDelete(session, parent));
        Assert.Equal(DropMarker + "DELETED", RunTableDefsDelete(session, child));
        await using AccessWriter writer2 = await session.OpenWriterAsync(Ct);
        await writer2.DropTableAsync("WriterParent", Ct);
        await writer2.DropTableAsync("WriterChild", Ct);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoTableDefsDelete_OfSelfReferencingTable_IsDeleted()
    {
        const string tree = "DaoDtTree";
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync(tree, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_DaoDtTree", tree, "Id", tree, "ParentId"), Ct);
            await writer.CreateTableAsync("WriterTree", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_WriterTree", "WriterTree", "Id", "WriterTree", "ParentId"), Ct);
        }

        Assert.Equal(DropMarker + "DELETED", RunTableDefsDelete(session, tree));
        await using AccessWriter writer2 = await session.OpenWriterAsync(Ct);
        await writer2.DropTableAsync("WriterTree", Ct);
    }

    /// <summary>
    /// Runs DAO <c>TableDefs.Delete</c> on <paramref name="table"/> and returns
    /// <c>DROP=DELETED</c>, or <c>DROP=REFUSED</c> followed by DAO's message.
    /// </summary>
    /// <param name="session">The session whose source database to change.</param>
    /// <param name="table">The table to delete.</param>
    /// <returns>The outcome line.</returns>
    private static string RunTableDefsDelete(AccessRoundTripSession session, string table)
    {
        string name = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(table);
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(
            session.SourcePath,
            $"try {{ $db.TableDefs.Delete({name}); Write-Output '{DropMarker}DELETED' }} catch {{ Write-Output ('{Refused} ' + $_.Exception.Message) }}",
            DaoTimeout);

        string failureMessage =
            $"""
            DAO TableDefs.Delete('{table}') failed (exit={result.ExitCode}).
            --- stdout ---
            {result.StdOut}
            --- stderr ---
            {result.StdErr}
            """;
        Assert.True(result.ExitCode == 0, failureMessage);
        return result.StdOut
            .Split('\n')
            .Select(line => line.Trim())
            .Single(line => line.StartsWith(DropMarker, StringComparison.Ordinal));
    }

    private static async Task AssertWriterRefusesAsync(AccessRoundTripSession session, string table)
    {
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            JetOperationException ex = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropTableAsync(table, Ct));
            Assert.Equal(JetErrorCode.TableInRelationship, ex.ErrorCode);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        Assert.Contains(table, await reader.ListTablesAsync(Ct));
    }
}
