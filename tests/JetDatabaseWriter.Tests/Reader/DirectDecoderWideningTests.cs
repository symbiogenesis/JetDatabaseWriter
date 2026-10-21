namespace JetDatabaseWriter.Tests.Reader;

using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1812 // Test POCOs are instantiated by the compiled direct decoder.

public sealed class DirectDecoderWideningTests
{
    private const string TableName = "Widen";

    [Fact]
    public async Task Rows_WideningTargets_DecodeLosslessly()
    {
        await using var stream = new MemoryStream();
        await WriteSampleAsync(stream);

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        WidenedRow? row = null;
        await foreach (WidenedRow r in reader.Rows<WidenedRow>(TableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            row = r;
            break;
        }

        Assert.NotNull(row);
        Assert.Equal(200L, row.ByteCol); // byte -> long
        Assert.Equal(-12345L, row.ShortCol); // short -> long (sign-extended)
        Assert.Equal(1_000_000m, row.IntCol); // int -> decimal
        Assert.Equal(5_000_000_000m, row.LongCol); // long -> decimal
        Assert.Equal(1.5d, row.FloatCol); // float -> double (exact)
    }

    [Fact]
    public async Task Rows_NullableWideningTargets_DecodeLosslessly()
    {
        await using var stream = new MemoryStream();
        await WriteSampleAsync(stream);

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        WidenedNullableRow? row = null;
        await foreach (WidenedNullableRow r in reader.Rows<WidenedNullableRow>(TableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            row = r;
            break;
        }

        Assert.NotNull(row);
        Assert.Equal(200L, row.ByteCol); // byte -> long?
        Assert.Equal(-12345d, row.ShortCol); // short -> double?
        Assert.Equal(1_000_000L, row.IntCol); // int -> long?
        Assert.Equal(5_000_000_000m, row.LongCol); // long -> decimal?
        Assert.Equal(1.5d, row.FloatCol); // float -> double?
    }

    private static async Task WriteSampleAsync(MemoryStream stream)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        await writer.CreateTableAsync(
            TableName,
            [
                new ColumnDefinition("ByteCol", typeof(byte)),
                new ColumnDefinition("ShortCol", typeof(short)),
                new ColumnDefinition("IntCol", typeof(int)),
                new ColumnDefinition("LongCol", typeof(long)),
                new ColumnDefinition("FloatCol", typeof(float)),
            ],
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync(
            TableName,
            [(byte)200, (short)-12345, 1_000_000, 5_000_000_000L, 1.5f],
            TestContext.Current.CancellationToken);
    }

    private sealed class WidenedRow
    {
        public long ByteCol { get; set; }

        public long ShortCol { get; set; }

        public decimal IntCol { get; set; }

        public decimal LongCol { get; set; }

        public double FloatCol { get; set; }
    }

    private sealed class WidenedNullableRow
    {
        public long? ByteCol { get; set; }

        public double? ShortCol { get; set; }

        public long? IntCol { get; set; }

        public decimal? LongCol { get; set; }

        public double? FloatCol { get; set; }
    }
}
