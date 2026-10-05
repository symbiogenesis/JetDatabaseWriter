namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Declaration refusals leave the database unchanged.</summary>
public sealed class ColumnDeclarationSafetyTests
{
    /// <summary>Gets declarations rejected before any schema writes.</summary>
    public static TheoryData<string> RejectedDeclarations => new()
    {
        "ByteAutoNumber", "LongAutoNumber", "TextAutoNumber", "BinaryDefault", "TimeSpanDefault", "DateTimeOffsetDefault",
    };

    /// <summary>Both declaration entry points leave bytes and catalog intact.</summary>
    /// <param name="kind">The invalid declaration.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(RejectedDeclarations))]
    public async Task UnsupportedDeclaration_DoesNotWrite(string kind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Existing", [new("Id", typeof(int))], ct);
            await writer.InsertRowAsync("Existing", [1], ct);
            byte[] before = stream.ToArray();
            ColumnDefinition column = kind switch
            {
                "ByteAutoNumber" => new("Bad", typeof(byte)) { IsAutoIncrement = true },
                "LongAutoNumber" => new("Bad", typeof(long)) { IsAutoIncrement = true },
                "TextAutoNumber" => new("Bad", typeof(string)) { IsAutoIncrement = true },
                "BinaryDefault" => new("Bad", typeof(byte[])) { DefaultValue = new byte[] { 1, 2 } },
                "TimeSpanDefault" => new("Bad", typeof(string)) { DefaultValue = TimeSpan.FromMinutes(1) },
                "DateTimeOffsetDefault" => new("Bad", typeof(string)) { DefaultValue = DateTimeOffset.UnixEpoch },
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown declaration."),
            };

            if (kind == "TextAutoNumber")
            {
                ArgumentException create = await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.CreateTableAsync("Refused", [column], ct));
                Assert.Equal("columns", create.ParamName);
                ArgumentException add = await Assert.ThrowsAnyAsync<ArgumentException>(async () => await writer.AddColumnAsync("Existing", column, ct));
                Assert.Equal("column", add.ParamName);
            }
            else
            {
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.CreateTableAsync("Refused", [column], ct));
                await Assert.ThrowsAsync<NotSupportedException>(async () => await writer.AddColumnAsync("Existing", column, ct));
            }

            Assert.Equal(before, stream.ToArray());
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        Assert.Equal(["Existing"], await reader.ListTablesAsync(ct));
        DataTable table = await reader.ReadDataTableAsync("Existing", cancellationToken: ct);
        Assert.Single(table.Columns.Cast<DataColumn>());
        Assert.Equal(1, Assert.Single(table.Rows.Cast<DataRow>())[0]);
    }

    /// <summary>A blank expression uses the CLR literal in every writer session.</summary>
    /// <param name="expression">The absent expression spelling.</param>
    /// <returns>The asynchronous test.</returns>
    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    public async Task BlankDefaultExpression_UsesClrDefaultAfterReopen(string expression)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new("Value", typeof(int)) { DefaultValue = 42, DefaultValueExpression = expression }], ct);
            await writer.InsertRowAsync("Items", [DbDefault.Value], ct);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.InsertRowAsync("Items", [DbDefault.Value], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        DataTable table = await reader.ReadDataTableAsync("Items", cancellationToken: ct);
        Assert.Equal(2, table.Rows.Count);
        Assert.All(table.Rows.Cast<DataRow>(), row => Assert.Equal(42, row[0]));
    }
}
