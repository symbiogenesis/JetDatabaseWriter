namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Calculated operators preserve native argument evaluation, subtypes and cached errors.</summary>
public sealed class CalculatedOperatorStatusTests
{
    [Theory]
    [InlineData("native-operators.accdb", "079445EF0BC57C9E41949D1E9A0AFE6EF357163F2F6F3C0165C1BAC11C984434")]
    [InlineData("native-operators-compacted.accdb", "73593FF49BBE2C01B93FA2A8368F069D5F14C3F7625BBE41161CC3B8054820C3")]
    public async Task NativeOperatorFixtures_PreserveStatusesAndUnaryTextIdentity(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedOperators", fixture);
        byte[] bytes = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, TestContext.Current.CancellationToken);
        object[][] unary;
        object[] membership;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            unary = (await reader.Rows("Unary", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray();
            membership = Assert.Single(await reader.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }

        Assert.Equal(8, unary.Length);
        for (int index = 0; index < unary.Length; index++)
        {
            object[] row = unary[index];
            if (row[1] is string text)
            {
                Assert.Equal(Encoding.Unicode.GetBytes(text), Assert.IsType<byte[]>(row[4]));
                Assert.Equal(text, row[6]);
            }

            if (index is 0 or 1)
            {
                int code = index == 0 ? 13 : 6;
                foreach (int ordinal in new[] { 2, 3, 5 })
                {
                    Assert.Equal(code, Assert.IsType<CalculatedError>(row[ordinal]).NativeCode);
                }
            }
        }

        Assert.Equal(1E100d, unary[2][2]);
        Assert.Equal(-1E100d, unary[2][3]);
        Assert.Equal(Convert.FromHexString("7DC39425AD49B2D4"), Assert.IsType<byte[]>(unary[2][5]));
        Assert.All(unary[5][2..], value => Assert.Equal(DBNull.Value, value));
        Assert.Equal(11, Assert.IsType<CalculatedError>(membership[2]).NativeCode);
        Assert.Equal(DBNull.Value, membership[3]);
        Assert.Equal(-1d, membership[1]);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            foreach (object[] row in unary)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("Unary", "Id", row[0], new Dictionary<string, object?> { ["S"] = row[1] }, TestContext.Current.CancellationToken));
            }

            Assert.Equal(1, await writer.UpdateRowsAsync("Membership", "Id", 1, new Dictionary<string, object?> { ["Id"] = 1 }, TestContext.Current.CancellationToken));
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(unary, (await reopened.Rows("Unary", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)).OrderBy(row => row[0]).ToArray());
        Assert.Equal(membership, Assert.Single(await reopened.Rows("Membership", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)));
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    public async Task Dao120_OperatorsMatchNativeCachedValues()
    {
        string[] expressions = [
            "1 In (1,1/0)", "1 In (2,1/0)", "Null In (1/0,1)",
            "1 In (1/0,1)", "1 In (1/0,2)", "Null In (1,1/0)",
            "1 In (1,Null)", "1 In (2,Null)", "Null In (1,2)",
            "1 In (1,Sqr(-1))", "1 In (Sqr(-1),1)", "(1/0) In (1,2)",
            "+\"bad\"", "-\"bad\"", "+\"1E999\"", "-\"1E999\"",
            "+\"1E100\"", "-\"1E100\"", "+\"1.2345678901234567\"", "-\"1.2345678901234567\"",
        ];
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CalculatedInErrors");
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string cases = string.Join(",", expressions.Select(AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral));
        AccessRoundTripEnvironment.CompactResult created = session.RunDaoEngineScript(
            $$"""
            $db=$engine.CreateDatabase({{path}},';LANGID=0x0409;CP=1252;COUNTRY=0',128)
            try {
                $cases=@({{cases}})
                for($index=0;$index -lt $cases.Count;$index++) {
                    $table=$db.CreateTableDef('T'+$index)
                    $table.Fields.Append($table.CreateField('Id',4))
                    $db.TableDefs.Append($table)
                    $field=$table.CreateField('V',7)
                    $field.Expression=$cases[$index]
                    $table.Fields.Append($field)
                    $db.Execute('INSERT INTO T'+$index+' (Id) VALUES (1)',128)
                }
            } finally {$db.Close()}
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(created.ExitCode == 0, created.StdOut + created.StdErr);
        var expected = new List<object>();
        await using (AccessReader reader = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                object[] row = Assert.Single(await reader.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
                expected.Add(row[1]);
            }
        }

        Assert.Equal(13, Assert.IsType<CalculatedError>(expected[12]).NativeCode);
        Assert.Equal(13, Assert.IsType<CalculatedError>(expected[13]).NativeCode);
        Assert.Equal(6, Assert.IsType<CalculatedError>(expected[14]).NativeCode);
        Assert.Equal(6, Assert.IsType<CalculatedError>(expected[15]).NativeCode);

        await using (AccessWriter writer = await AccessWriter.OpenAsync(session.SourcePath, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            for (int index = 0; index < expressions.Length; index++)
            {
                Assert.Equal(1, await writer.UpdateRowsAsync("T" + index, "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
            }
        }

        await using AccessReader reopened = await AccessReader.OpenAsync(session.SourcePath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        for (int index = 0; index < expressions.Length; index++)
        {
            object[] row = Assert.Single(await reopened.Rows("T" + index, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
            Assert.True(Equals(expected[index], row[1]), expressions[index] + " native=" + expected[index] + " managed=" + row[1]);
        }
    }
}
