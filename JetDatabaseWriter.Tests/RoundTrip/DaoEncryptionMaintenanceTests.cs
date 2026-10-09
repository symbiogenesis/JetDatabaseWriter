namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Microsoft DAO validates public encrypted creation and file maintenance.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoEncryptionMaintenanceTests
{
    /// <summary>Directly encrypted bootstrap output opens, accepts native writes, and compacts.</summary>
    /// <param name="format">The native database format.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateEncrypted_DaoReadsWritesAndCompacts(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: format == DatabaseFormat.Jet4Mdb ? ".mdb" : ".accdb");
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(session.SourcePath, format, new AccessWriterOptions("Native123") { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Label", typeof(string), 60)], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [7, "Created encrypted"], TestContext.Current.CancellationToken);
        }

        await AssertDaoAsync(session, "Native123");
    }

    /// <summary>Every public maintenance operation emits a native file that DAO can update and compact.</summary>
    /// <param name="fixture">The Microsoft-produced original.</param>
    /// <param name="operation">The public maintenance operation.</param>
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("NativeJet4Rc4.mdb", "change")]
    [InlineData("NativeJet4Rc4.mdb", "decrypt")]
    [InlineData("NativeJet4Rc4.mdb", "encrypt")]
    [InlineData("NativeAceAgile.accdb", "change")]
    [InlineData("NativeAceAgile.accdb", "decrypt")]
    [InlineData("NativeAceAgile.accdb", "encrypt")]
    public async Task MaintainEncrypted_DaoReadsWritesAndCompacts(string fixture, string operation)
    {
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: Path.GetExtension(fixture));
        File.Copy(Path.Combine(TestDatabases.EncryptedRoot, fixture), session.SourcePath);
        var options = new AccessWriterOptions { UseLockFile = false };
        string password = "Changed123";
        if (operation == "change")
        {
            await AccessDatabaseEncryption.ChangePasswordAsync(session.SourcePath, "Native123".AsMemory(), password.AsMemory(), options, TestContext.Current.CancellationToken);
        }
        else
        {
            await AccessDatabaseEncryption.DecryptAsync(session.SourcePath, "Native123".AsMemory(), options, TestContext.Current.CancellationToken);
            if (operation == "encrypt")
            {
                await AccessDatabaseEncryption.EncryptAsync(session.SourcePath, password.AsMemory(), options: options, cancellationToken: TestContext.Current.CancellationToken);
            }
            else
            {
                password = string.Empty;
            }
        }

        await AssertDaoAsync(session, password);
    }

    private static async Task AssertDaoAsync(AccessRoundTripSession session, string password)
    {
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string connection = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(password.Length == 0 ? string.Empty : ";PWD=" + password);
        string locale = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(";LANGID=0x0409;CP=1252;COUNTRY=0" + (password.Length == 0 ? string.Empty : ";PWD=" + password));
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, {{connection}})
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [T]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='DAO maintenance update' WHERE [Id]=7", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, {{locale}}, 0, {{connection}})
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROWS=1", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("DAO maintenance update", Assert.Single(rows.Select())["Label"]);
    }
}
