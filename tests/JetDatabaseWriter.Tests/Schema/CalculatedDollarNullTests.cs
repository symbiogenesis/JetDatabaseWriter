namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native dollar Text calls distinguish invalid use of Null from type mismatch.</summary>
public sealed class CalculatedDollarNullTests
{
    public static TheoryData<string, int> Cases => new()
    {
        { "Len(UCase$(Null))", 13 },
        { "Len(LCase$(Null))", 13 },
        { "Len(Trim$(Null))", 13 },
        { "Len(Mid$(Null,1,1))", 13 },
        { "Len(Right$(Null,1))", 13 },
        { "Len(String$(1,Null))", 94 },
        { "Len(Left$(Null,1))", 13 },
        { "Len(Str$(Null))", 94 },
        { "Len(String$(Null,\"a\"))", 13 },
        { "Len(Left$(\"a\",Null))", 13 },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Writer_DollarNullMatchesNativeError(string expression, int expectedCode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, Enums.DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new("Value", typeof(int)) { IsCalculated = true, CalculationExpression = expression }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [DBNull.Value], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        Assert.Equal(expectedCode, Assert.IsType<CalculatedError>(row[0]).NativeCode);
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Native_DollarNullErrorCodesSurviveCompact()
    {
        await using AccessRoundTripSession native = await AccessRoundTripSession.CreateDaoAccdbAsync(TestContext.Current.CancellationToken, "JetDatabaseWriter.CalculatedDollarNull");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string definitions = string.Join(',', Cases.Select(static item => AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(item.Data.Item1)));
        AccessRoundTripEnvironment.CompactResult authored = native.RunDaoEngineScript(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                $db.Execute('CREATE TABLE [T] ([Id] LONG)',128)
                $db.TableDefs.Refresh()
                $tdf=$db.TableDefs('T')
                $expressions=@({{definitions}})
                for($index=0;$index -lt $expressions.Count;$index++) {
                    $field=$tdf.CreateField('V'+$index,4)
                    $field.Expression=[string]$expressions[$index]
                    $tdf.Fields.Append($field)
                }
                $db.Execute('INSERT INTO [T] ([Id]) VALUES (1)',128)
            } finally { $db.Close() }
            $engine.CompactDatabase({{source}},{{compacted}})
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(authored.ExitCode == 0, authored.StdOut + authored.StdErr);
        foreach (string path in new[] { native.SourcePath, native.CompactedPath })
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
            int[] actual = row[1..].Select(static value => Assert.IsType<CalculatedError>(value).NativeCode).ToArray();
            Assert.Equal(Cases.Select(static item => item.Data.Item2), actual);
        }
    }
}
