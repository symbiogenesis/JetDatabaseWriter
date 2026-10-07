namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>CLR defaults use the persisted literal's conversions in every writer session.</summary>
public sealed class DeclaredDefaultConversionTests
{
    /// <summary>Declaring and reopened writers apply the same Boolean, text and date defaults.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MixedTypeDefaults_AreConsistentAcrossWriterSessions(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Defaults",
                [
                    new("Id", typeof(int)),
                    new("BooleanNumber", typeof(int)) { DefaultValue = true },
                    new("BooleanText", typeof(string), 50) { DefaultValue = true },
                    new("DateText", typeof(string), 50) { DefaultValue = new DateTime(2024, 2, 29, 8, 30, 15).AddMilliseconds(125) },
                    new("NumericText", typeof(int)) { DefaultValue = "42" },
                    new("DateValue", typeof(DateTime)) { DefaultValue = new DateTime(2024, 2, 29, 8, 30, 15).AddMilliseconds(125) },
                ],
                ct);
            await writer.InsertRowAsync("Defaults", new RowValues { ["Id"] = 1 }, ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, ct))
        {
            await writer.InsertRowAsync("Defaults", new RowValues { ["Id"] = 2 }, ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        DataTable rows = await reader.ReadTableAsync("Defaults", cancellationToken: ct);
        Assert.Equal(2, rows.Rows.Count);
        foreach (DataRow row in rows.Rows)
        {
            Assert.Equal(-1, row["BooleanNumber"]);
            Assert.Equal("-1", row["BooleanText"]);
            Assert.Equal("2/29/2024 8:30:15 AM", row["DateText"]);
            Assert.Equal(42, row["NumericText"]);
            Assert.Equal(new DateTime(2024, 2, 29, 8, 30, 15), row["DateValue"]);
        }
    }

    /// <summary>An invalid declared default refuses inserts in the declaring and reopened writer.</summary>
    /// <param name="format">The database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task InvalidDefault_RefusesWithoutChangingBytes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("InvalidDefault", [new ColumnDefinition("Value", typeof(int)) { DefaultValue = "not a number" }], ct);
            byte[] before = stream.ToArray();
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("InvalidDefault", new RowValues(), ct));
            Assert.Equal(before, stream.ToArray());
        }

        byte[] reopenedBefore = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, ct))
        {
            _ = await Assert.ThrowsAsync<JetValidationRuleException>(async () => await writer.InsertRowAsync("InvalidDefault", new RowValues(), ct));
        }

        Assert.Equal(reopenedBefore, stream.ToArray());
    }

    /// <summary>Schema rewrites and reopened writers retain converted defaults.</summary>
    [Fact]
    public async Task Defaults_SurviveSchemaRewriteAndHyperlinkProjection()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions { UseLockFile = false };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, options, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                "Defaults",
                [
                    new("Flag", typeof(int)) { DefaultValue = true },
                    new("Link", typeof(Hyperlink)) { DefaultValue = "Docs#https://example.com" },
                ],
                ct);
            await writer.InsertRowAsync("Defaults", [DbDefault.Value, DbDefault.Value], ct);
            await writer.AddColumnAsync("Defaults", new ColumnDefinition("Extra", typeof(int)), ct);
            await writer.InsertRowAsync("Defaults", [DbDefault.Value, DbDefault.Value, 1], ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, ct))
        {
            await writer.InsertRowAsync("Defaults", [DbDefault.Value, DbDefault.Value, 2], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        DataTable rows = await reader.ReadTableAsync("Defaults", cancellationToken: ct);
        Assert.Equal(3, rows.Rows.Count);
        foreach (DataRow row in rows.Rows)
        {
            Assert.Equal(-1, row["Flag"]);
            Assert.Equal(new Hyperlink("Docs", "https://example.com"), Assert.IsType<Hyperlink>(row["Link"]));
        }
    }
}
