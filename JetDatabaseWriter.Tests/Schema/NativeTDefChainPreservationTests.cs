namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Native continuation headers and opaque padding survive logical chain write-back.</summary>
public sealed class NativeTDefChainPreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task Dao120Chain_WriteBackPreservesNativePagesAndSurvivesCompact(DatabaseFormat format)
        => VerifyAsync(format);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Dao36Chain_WriteBackPreservesNativePagesAndSurvivesCompact()
        => VerifyAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.NativeTDefChain", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        string createScript = $$"""
            $db = $engine.CreateDatabase({{source}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            try {
                $table = $db.CreateTableDef('Native')
                $table.Fields.Append($table.CreateField('Id', 4))
                $table.Fields.Append($table.CreateField('Label', 10, 40))
                for ($i = 0; $i -lt 200; $i++) { $table.Fields.Append($table.CreateField(('ContinuationColumn' + $i), 4)) }
                $db.TableDefs.Append($table)
                $db.Execute("INSERT INTO [Native] ([Id],[Label]) VALUES (1,'native payload')", 128)
            } finally { $db.Close() }
            """;
        (int exitCode, string stdout, string stderr) = RunNative(session, format, createScript);
        Assert.True(exitCode == 0, $"Native create failed: {stdout}\n{stderr}");

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, Ct))
        {
            long root = (await writer.Services.Catalog.ResolveRequiredTableAsync("Native", Ct)).Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            Assert.True(chain.PageNumbers.Count > 1);
            var before = new Dictionary<long, byte[]>();
            foreach (long page in chain.PageNumbers)
            {
                before.Add(page, await writer.Database.Pages.ReadPageCopyAsync(page, Ct));
            }

            await chain.WriteAsync(chain.Bytes, checked(Ri32(chain.Bytes, 8) + 8), writer.Services.PageAllocator.AllocatePageAsync, writer.Pager.WritePageAsync, writer.Services.PageAllocator.DeallocatePageAsync, writer.Database.Format.WritesTDefFreeSpace, Ct);
            foreach ((long page, byte[] original) in before)
            {
                byte[] actual = await writer.Database.Pages.ReadPageCopyAsync(page, Ct);

                // The root's free-space hint is the only normalized field.
                if (page == root && writer.Database.Format.WritesTDefFreeSpace)
                {
                    actual[2] = original[2];
                    actual[3] = original[3];
                }

                Assert.Equal(original, actual);
            }
        }

        string verificationScript = $$"""
            $db = $engine.OpenDatabase({{source}})
            try {
                if ($db.TableDefs('Native').Fields.Count -ne 202) { throw 'Lost native columns.' }
                $rs = $db.OpenRecordset('Native')
                try {
                    if ($rs.Fields('Label').Value -ne 'native payload') { throw 'Lost native value.' }
                } finally { $rs.Close() }
                $db.Execute("INSERT INTO [Native] ([Id],[Label]) VALUES (2,'after write-back')", 128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}}, {{compacted}}, ';LANGID=0x0409;CP=1252;COUNTRY=0', {{version}})
            $db = $engine.OpenDatabase({{compacted}}, $false, $true)
            try {
                $rs = $db.OpenRecordset('SELECT * FROM [Native] WHERE [Id]=2')
                try {
                    if ($rs.EOF -or $rs.Fields('Label').Value -ne 'after write-back') { throw 'Lost compacted value.' }
                } finally { $rs.Close() }
            } finally { $db.Close() }
            Write-Output 'NATIVE_CHAIN_COMPACT_OK'
            """;
        (exitCode, stdout, stderr) = RunNative(session, format, verificationScript);
        Assert.True(exitCode == 0, $"Native reopen/write/compact failed: {stdout}\n{stderr}");
        Assert.Contains("NATIVE_CHAIN_COMPACT_OK", stdout, StringComparison.Ordinal);
    }

    private static (int ExitCode, string Stdout, string Stderr) RunNative(AccessRoundTripSession session, DatabaseFormat format, string script)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            JetDatabaseWriter.TestSupport.PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            return (result.ExitCode, result.StandardOutput, result.StandardError);
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
        return (native.ExitCode, native.StdOut, native.StdErr);
    }
}
