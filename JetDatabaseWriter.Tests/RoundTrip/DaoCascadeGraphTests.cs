namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Compares simple cycles and overlapping diamonds with DAO-authored graphs.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoCascadeGraphTests
{
    private static readonly TimeSpan DaoTimeout = TimeSpan.FromMinutes(2);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DaoAndWriter_CascadeGraph_HaveMatchingOutcomes(bool diamond)
    {
        foreach (bool native in new[] { true, false })
        {
            await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(Ct);
            string setup = "$diamond = " + (diamond ? "$true" : "$false") + "\n" + """
                $names = if ($diamond) { @('A','B','C','D') } else { @('A','B') }
                foreach ($name in $names) {
                    $db.Execute("CREATE TABLE [$name] ([Id] LONG CONSTRAINT [PK_$name] PRIMARY KEY)", 128)
                    $db.Execute("INSERT INTO [$name] ([Id]) VALUES (1)", 128)
                }
                $edges = if ($diamond) { @('A,B','A,C','B,D','C,D') } else { @('A,B','B,A') }
                foreach ($edge in $edges) {
                    $parts = $edge.Split(',')
                    $rel = $db.CreateRelation(('FK_' + $parts[1] + '_' + $parts[0]), $parts[0], $parts[1], 256)
                    $field = $rel.CreateField('Id')
                    $field.ForeignName = 'Id'
                    $rel.Fields.Append($field)
                    $db.Relations.Append($rel)
                }
                Write-Output 'GRAPH=CREATED'
                """;
            AccessRoundTripEnvironment.CompactResult created = session.RunDaoDatabaseScript(session.SourcePath, setup, DaoTimeout);
            Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
            Assert.Contains("GRAPH=CREATED", created.StdOut, StringComparison.Ordinal);

            if (native)
            {
                const string update = """
                    try { $db.Execute('UPDATE [A] SET [Id]=5 WHERE [Id]=1', 128); Write-Output 'UPDATE=ACCEPTED' }
                    catch { Write-Output 'UPDATE=REFUSED' }
                    """;
                AccessRoundTripEnvironment.CompactResult result = session.RunDaoDatabaseScript(session.SourcePath, update, DaoTimeout);
                Assert.True(result.ExitCode == 0, result.StdOut + result.StdErr);
                Assert.Contains(diamond ? "UPDATE=REFUSED" : "UPDATE=ACCEPTED", result.StdOut, StringComparison.Ordinal);
            }
            else
            {
                byte[] before = await File.ReadAllBytesAsync(session.SourcePath, Ct);
                await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
                {
                    if (diamond)
                    {
                        JetConstraintException exception = await Assert.ThrowsAsync<JetConstraintException>(async () =>
                            await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
                        Assert.Equal(JetErrorCode.ForeignKeyRestrictUpdate, exception.ErrorCode);
                    }
                    else
                    {
                        Assert.Equal(1, await writer.UpdateRowsAsync("A", RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 5 }, Ct));
                    }
                }

                if (diamond)
                {
                    Assert.Equal(before, await File.ReadAllBytesAsync(session.SourcePath, Ct));
                }
            }

            await AssertRowsAsync(session.SourcePath, diamond);
            session.RunDaoCompact();
            await AssertRowsAsync(session.CompactedPath, diamond);
        }
    }

    private static async Task AssertRowsAsync(string path, bool diamond)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: Ct);
        foreach (string table in diamond ? new[] { "A", "B", "C", "D" } : new[] { "A", "B" })
        {
            int count = 0;
            await foreach (object[] row in reader.Rows(table, cancellationToken: Ct))
            {
                Assert.Equal(diamond ? 1 : 5, Assert.IsType<int>(row[0]));
                count++;
            }

            Assert.Equal(1, count);
        }
    }
}
