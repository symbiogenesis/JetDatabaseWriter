namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Expressions;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native GUID literals retain their value and Access text projection.</summary>
public sealed class CalculatedGuidLiteralTests
{
    private const string Expression = "{guid {12345678-1234-1234-1234-123456789abc}}";
    private const string Text = "{12345678-1234-1234-1234-123456789ABC}";
    private static readonly Guid Value = new("12345678-1234-1234-1234-123456789abc");

    [Fact]
    public void GuidLiteral_ParsesAsGuid()
    {
        CalculatedExpressionNode plan = CalculatedExpressionParser.Parse(Expression);
        Assert.Equal(Value, Assert.IsType<Guid>(plan.Evaluate(new CalculatedExpressionEvaluationContext(new TableDef(), [], []))));
    }

    [Fact]
    public async Task GuidLiteral_CreateInsertUpdateAndReopenRetainGuidAndNativeText()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", Columns(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            Assert.Equal(1, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 2 }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await AssertValuesAsync(reader, 2);
    }

    [Theory]
    [InlineData("{guid {bad}}")]
    [InlineData("{guid {12345678-1234-1234-1234-123456789abc}")]
    [InlineData("{guid 12345678-1234-1234-1234-123456789abc}")]
    [InlineData("{other {12345678-1234-1234-1234-123456789abc}}")]
    public void MalformedGuidLiteral_Refuses(string expression)
        => Assert.Throws<ArgumentException>(() => CalculatedExpressionParser.Parse(expression));

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task DaoGuidLiteral_AcceptsWriterCachesAndCompacts()
    {
        await using AccessRoundTripSession session = await AccessRoundTripSession.CreateDaoAccdbAsync(
            TestContext.Current.CancellationToken, "JetDatabaseWriter.Tests.CalculatedGuidLiteral");
        await using (AccessWriter writer = await session.OpenWriterAsync(TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", Columns(), TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [1, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
        }

        AccessRoundTripEnvironment.CompactResult native = session.RunDaoDatabaseScriptThenCompact(
            """
            $rs = $db.OpenRecordset('SELECT [GuidValue],[TextValue] FROM [T]')
            try {
                if ($rs.Fields(0).Value -ne '{guid {12345678-1234-1234-1234-123456789ABC}}') { throw 'Writer GUID cache differs.' }
                if ($rs.Fields(1).Value -ne '{12345678-1234-1234-1234-123456789ABC}') { throw 'Writer GUID text differs.' }
            } finally { $rs.Close() }
            $db.Execute('UPDATE [T] SET [Id]=2',128)
            Write-Output 'NATIVE_GUID_CACHES=OK'
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(native.ExitCode == 0, $"DAO GUID validation failed: {native.StdOut}\n{native.StdErr}");
        Assert.Contains("NATIVE_GUID_CACHES=OK", native.StdOut, StringComparison.Ordinal);
        await using AccessReader reader = await AccessReader.OpenAsync(
            session.CompactedPath, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        await AssertValuesAsync(reader, 2);
    }

    private static ColumnDefinition[] Columns() =>
    [
        new("Id", typeof(int)),
        new("GuidValue", typeof(Guid)) { IsCalculated = true, CalculationExpression = Expression },
        new("TextValue", typeof(string), maxLength: 80) { IsCalculated = true, CalculationExpression = Expression },
    ];

    private static async Task AssertValuesAsync(AccessReader reader, int id)
    {
        object[] row = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(id, row[0]);
        Assert.Equal(Value, row[1]);
        Assert.Equal(Text, row[2]);
    }
}
