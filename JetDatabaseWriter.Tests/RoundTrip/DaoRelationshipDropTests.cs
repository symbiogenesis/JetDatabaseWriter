namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// DAO oracle for <see cref="AccessWriter.DropTableAsync"/> on a related
/// table. The writer refuses to drop a table that any <c>MSysRelationships</c>
/// row names, including a relationship that does not enforce referential
/// integrity and one that relates a table to itself, on the understanding
/// that Jet refuses the same <c>TableDefs.Delete</c> with error 3303
/// ("Could not delete; currently participates in one or more
/// relationships"). These tests record what DAO actually does on a host with
/// Microsoft Access, and that the writer agrees.
/// </summary>
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
    public async Task DaoTableDefsDelete_OfTableInRelationshipWithoutIntegrity_IsRefused()
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
        }

        Assert.StartsWith(Refused, RunTableDefsDelete(session, parent), StringComparison.Ordinal);
        Assert.StartsWith(Refused, RunTableDefsDelete(session, child), StringComparison.Ordinal);
        await AssertWriterRefusesAsync(session, parent);
        await AssertWriterRefusesAsync(session, child);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task DaoTableDefsDelete_OfSelfReferencingTable_IsRefused()
    {
        const string tree = "DaoDtTree";
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateFromNorthwindAsync(Ct);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.CreateTableAsync(tree, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("ParentId", typeof(int))], Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_DaoDtTree", tree, "Id", tree, "ParentId"), Ct);
        }

        Assert.StartsWith(Refused, RunTableDefsDelete(session, tree), StringComparison.Ordinal);
        await AssertWriterRefusesAsync(session, tree);
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
            InvalidOperationException ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await writer.DropTableAsync(table, Ct));
            Assert.Contains("participates in", ex.Message, StringComparison.Ordinal);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        Assert.Contains(table, await reader.ListTablesAsync(Ct));
    }
}
