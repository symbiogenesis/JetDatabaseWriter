namespace JetDatabaseWriter.Tests.Writer;

using System.Data;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Binary strings must retain their exact database code-page bytes.</summary>
public sealed class BinaryEncodingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "中文")]
    [InlineData(DatabaseFormat.Jet4Mdb, "中文")]
    [InlineData(DatabaseFormat.AceAccdb, "中文")]
    [InlineData(DatabaseFormat.Jet3Mdb, "Łódź")]
    [InlineData(DatabaseFormat.Jet4Mdb, "Łódź")]
    [InlineData(DatabaseFormat.AceAccdb, "Łódź")]
    [InlineData(DatabaseFormat.Jet3Mdb, "😀")]
    [InlineData(DatabaseFormat.Jet4Mdb, "😀")]
    [InlineData(DatabaseFormat.AceAccdb, "😀")]
    public async Task UnrepresentableString_InsertAndUpdateRefuseBeforeWriting(DatabaseFormat format, string text)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new("Id", typeof(int)), new("B", typeof(byte[]), 20), new("Memo", typeof(string))], Ct);
            await writer.InsertRowAsync("T", [1, new byte[] { 0, 128, 255 }, "original"], Ct);
            byte[] before = stream.ToArray();

            await Assert.ThrowsAsync<EncoderFallbackException>(async () => await writer.InsertRowAsync("T", [2, text, new string('m', 5000)], Ct));
            Assert.Equal(before, stream.ToArray());
            await Assert.ThrowsAsync<EncoderFallbackException>(async () => await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["B"] = text, ["Memo"] = new string('m', 5000) }, Ct));
            Assert.Equal(before, stream.ToArray());

            await writer.InsertRowAsync("T", [2, "é€?", "inserted"], Ct);
            Assert.Equal(1, await writer.UpdateRowsAsync("T", RowCriteria.Where("Id", 1), new RowValues { ["B"] = new byte[] { 255, 128, 0 } }, Ct));
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        DataTable table = await reader.ReadDataTableAsync("T", cancellationToken: Ct);
        Assert.Equal(2, table.Rows.Count);
        DataRow original = Assert.Single(table.AsEnumerable(), row => row.Field<int>("Id") == 1);
        Assert.Equal(new byte[] { 255, 128, 0 }, original.Field<byte[]>("B"));
        Assert.Equal("original", original.Field<string>("Memo"));
        DataRow inserted = Assert.Single(table.AsEnumerable(), row => row.Field<int>("Id") == 2);
        Assert.Equal(new byte[] { 0xE9, 0x80, 0x3F }, inserted.Field<byte[]>("B"));
    }
}
