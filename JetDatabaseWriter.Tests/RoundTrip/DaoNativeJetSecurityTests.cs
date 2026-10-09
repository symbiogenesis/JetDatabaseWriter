namespace JetDatabaseWriter.Tests.RoundTrip;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>DAO validates native Jet4 password security transformations.</summary>
[Trait("Category", "RequiresMicrosoftAccess")]
public sealed class DaoNativeJetSecurityTests
{
    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData("NativeJet4Rc4.mdb", "Changed123", 305419896u)]
    [InlineData("NativeJet4Rc4.mdb", "", 0u)]
    [InlineData("NativeJet4Password.mdb", "Changed123", 305419896u)]
    public async Task SecurityTransformation_DaoAuthenticatesWritesAndCompacts(string fixture, string password, uint encodingKey)
    {
        ArgumentNullException.ThrowIfNull(password);
        await using var session = AccessRoundTripSession.CreateEmpty(databaseExtension: ".mdb");
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), TestContext.Current.CancellationToken);
        byte[] oldHeader = image.AsSpan(0, 4096).ToArray();
        byte[] newHeader = (byte[])oldHeader.Clone();
        EncryptionManager.WriteNativeJet4EncryptionHeader(newHeader, encodingKey, password);
        using (IPageCodec sourceCodec = PageCodecFactory.Open(oldHeader, DatabaseFormat.Jet4Mdb, "Native123".AsMemory(), "password", cancellationToken: TestContext.Current.CancellationToken))
        using (IPageCodec targetCodec = PageCodecFactory.Open(newHeader, DatabaseFormat.Jet4Mdb, password.AsMemory(), "password", cancellationToken: TestContext.Current.CancellationToken))
        {
            for (int page = 1; page < image.Length / 4096; page++)
            {
                sourceCodec.Decode(image, page * 4096, page, 4096);
                targetCodec.Encode(image, page * 4096, page, 4096);
            }
        }

        newHeader.CopyTo(image, 0);
        await File.WriteAllBytesAsync(session.SourcePath, image, TestContext.Current.CancellationToken);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(session.SourcePath, new AccessWriterOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            await NativeJetSecurity.RewriteAsync(harness.Database.Format, harness.Database.TableDefs, harness.Database.OwnedPages, harness.Pager, oldHeader, newHeader, TestContext.Current.CancellationToken);
        }

        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string destination = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string connection = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(password.Length == 0 ? string.Empty : ";PWD=" + password);
        string locale = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(";LANGID=0x0409;CP=1252;COUNTRY=0" + (password.Length == 0 ? string.Empty : ";PWD=" + password));
        string script = $$"""
            $db = $engine.OpenDatabase({{source}}, $false, $false, {{connection}})
            try {
                $rs = $db.OpenRecordset('SELECT Count(*) AS N FROM [T]')
                try { Write-Output "ROWS=$($rs.Fields('N').Value)" } finally { $rs.Close() }
                $db.Execute("UPDATE [T] SET [Label]='DAO password update' WHERE [Id]=7", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{destination}}, {{locale}}, 64, {{connection}})
            """;
        AccessRoundTripEnvironment.CompactResult result = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        Assert.True(result.ExitCode == 0, $"DAO failed: {result.StdOut}\n{result.StdErr}");
        Assert.Contains("ROWS=1", result.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(session.CompactedPath, new AccessReaderOptions(password) { UseLockFile = false }, TestContext.Current.CancellationToken);
        using System.Data.DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("DAO password update", Assert.Single(rows.Select())["Label"]);
    }
}
