namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft DAO acceptance of independent historical flat ACE encryption providers.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoNativeAceProviderTests
{
    /// <summary>DAO reads and changes library-mutated provider pages, then compacts their database.</summary>
    /// <param name="fixture">The native provider fixture.</param>
    /// <param name="password">The fixture password.</param>
    /// <param name="table">The original table.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("Upstream-db2007-oldenc.accdb", "Test123", "Table1")]
    [InlineData("Upstream-db2007-enc.accdb", "Test123", "Table1")]
    [InlineData("Upstream-db2013-enc.accdb", "1234", "Customers")]
    [InlineData("Upstream-db-nonstandard.accdb", "password", "Table_One")]
    public async Task HistoricalProvider_DaoReadsWritesAndCompacts(string fixture, string password, string table)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: ".accdb");
        File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), session.SourcePath);
        int initialRows;
        string field;
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            using DataTable rows = await reader.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
            initialRows = rows.Rows.Count;
            field = rows.Columns[1].ColumnName;
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync(table, [null, "Library provider"], TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, ';PWD={{password}}')
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [{{table}}]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [{{table}}] SET [{{field}}]='DAO provider' WHERE [{{field}}]='Library provider'", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, ';LANGID=0x0409;CP=1252;COUNTRY=0;PWD={{password}}', 0, ';PWD={{password}}')
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains($"ROWS={initialRows + 1}", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reopened = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable compacted = await reopened.ReadTableAsync(table, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(initialRows + 1, compacted.Rows.Count);
        Assert.Equal("DAO provider", Assert.Single(compacted.Select($"[{field}] = 'DAO provider'"))[field]);
    }
}
