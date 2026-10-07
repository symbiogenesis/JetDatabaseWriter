namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft DAO verifies updates to native encrypted database pages.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoNativeEncryptionTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("NativeJet4Rc4.mdb")]
    [InlineData("NativeAceAgile.accdb")]
    public async Task NativeEncryptedMutation_DaoReadsWritesAndCompacts(string fixture)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: Path.GetExtension(fixture));
        File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), session.SourcePath);
        var options = new AccessWriterOptions("Native123") { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, options, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [8, "Library inserted"], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("Added", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", [1], TestContext.Current.CancellationToken);
            await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Added", [2], TestContext.Current.CancellationToken);
            await transaction.RollbackAsync(TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, ';PWD=Native123')
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [T]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [Added]')
                try { Write-Output "ADDED=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='DAO updated' WHERE [Id]=8", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, ';LANGID=0x0409;CP=1252;COUNTRY=0;PWD=Native123', 0, ';PWD=Native123')
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROWS=2", result.StdOut, StringComparison.Ordinal);
        Assert.Contains("ADDED=1", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions("Native123") { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow updated = Assert.Single(rows.Select("Id = 8"));
        Assert.Equal("DAO updated", updated["Label"]);
    }
}
