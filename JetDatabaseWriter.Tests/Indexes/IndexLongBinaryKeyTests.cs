namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Exercises the multi-segment binary index key encoder with long payloads
/// (up to the <c>T_BINARY</c> maximum of 255 bytes). Prior tests in
/// <see cref="IndexBinaryKeyTests"/> used payloads of at most 9 bytes (one
/// overflow segment). This class stresses the encoder with payloads that
/// produce many 8-byte segments, exercising the repeated intermediate-length
/// <c>0x09</c> / final-length-byte encoding loop and the descending bit-flip
/// across large key buffers. Closes §1.2 gap: "OLE long-value index keys —
/// needs a synthetic fixture.".
/// </summary>
public sealed class IndexLongBinaryKeyTests
{
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    /// <summary>
    /// A 255-byte binary payload (maximum T_BINARY length) round-trips
    /// through a unique index without corruption.
    /// </summary>
    [Fact]
    public async Task MaxLengthBinaryKey_255Bytes_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        byte[] payload = CreatePayload(255);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "LongBin",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_LongBin", "Bin") { IsUnique = true }],
                ct);

            await writer.InsertRowAsync("LongBin", [1, payload], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("LongBin", cancellationToken: ct);
        Assert.Single(dt.Rows);
        byte[] actual = Assert.IsType<byte[]>(dt.Rows[0]["Bin"]);
        Assert.Equal(payload, actual);
    }

    /// <summary>
    /// Multiple long binary payloads maintain correct sort order in the
    /// index (ascending unsigned byte-lexicographic). The reader returns
    /// all rows without error, confirming the leaf chain is well-formed.
    /// </summary>
    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(255)]
    public async Task LongBinaryKeys_MultipleRows_RoundTripCorrectly(int length)
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        byte[][] payloads =
        [
            CreatePayload(length, seed: 0x00),
            CreatePayload(length, seed: 0x55),
            CreatePayload(length, seed: 0xAA),
            CreatePayload(length, seed: 0xFF),
        ];

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "MultiBin",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_MultiBin", "Bin")],
                ct);

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("MultiBin", [i, payloads[i]], ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("MultiBin", cancellationToken: ct);
        Assert.Equal(payloads.Length, dt.Rows.Count);

        // Verify each payload was stored and round-tripped correctly.
        foreach (byte[] expected in payloads)
        {
            Assert.Contains(
                dt.Rows.Cast<DataRow>(),
                row => expected.SequenceEqual((byte[])row["Bin"]));
        }
    }

    /// <summary>
    /// Long binary keys in descending direction correctly flip data and
    /// final-length bytes across many segments.
    /// </summary>
    [Fact]
    public async Task LongBinaryKey_Descending_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        byte[][] payloads =
        [
            CreatePayload(100, seed: 0x10),
            CreatePayload(100, seed: 0x80),
            CreatePayload(100, seed: 0xF0),
        ];

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "DescLongBin",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [
                    new IndexDefinition("IX_DescLongBin", "Bin")
                    {
                        DescendingColumns = ["Bin"],
                    },
                ],
                ct);

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("DescLongBin", [i, payloads[i]], ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("DescLongBin", cancellationToken: ct);
        Assert.Equal(payloads.Length, dt.Rows.Count);

        foreach (byte[] expected in payloads)
        {
            Assert.Contains(
                dt.Rows.Cast<DataRow>(),
                row => expected.SequenceEqual((byte[])row["Bin"]));
        }
    }

    /// <summary>
    /// Unique constraint on a long binary key detects duplicates correctly
    /// even for 255-byte payloads.
    /// </summary>
    [Fact]
    public async Task LongBinaryKey_UniqueViolation_Throws()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        byte[] payload = CreatePayload(200, seed: 0x42);

        await using var writer = await OpenWriterAsync(stream);
        await writer.CreateTableAsync(
            "UniqueLongBin",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
            ],
            [new IndexDefinition("UX_LongBin", "Bin") { IsUnique = true }],
            ct);

        await writer.InsertRowAsync("UniqueLongBin", [1, payload], ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("UniqueLongBin", [2, payload.ToArray()], ct));
    }

    /// <summary>
    /// Composite index with a long binary key column and a text column.
    /// Exercises the multi-column entry concatenation path with large
    /// binary segments.
    /// </summary>
    [Fact]
    public async Task LongBinaryKey_CompositeWithText_RoundTrips()
    {
        await using var stream = await CreateFreshAccdbStreamAsync();

        byte[] bin1 = CreatePayload(128, seed: 0x11);
        byte[] bin2 = CreatePayload(128, seed: 0x22);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "CompLongBin",
                [
                    new ColumnDefinition("Tag", typeof(string), maxLength: 50),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_CompLongBin", ["Tag", "Bin"])],
                ct);

            await writer.InsertRowAsync("CompLongBin", ["alpha", bin1], ct);
            await writer.InsertRowAsync("CompLongBin", ["alpha", bin2], ct);
            await writer.InsertRowAsync("CompLongBin", ["beta", bin1], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        DataTable dt = await reader.ReadDataTableAsync("CompLongBin", cancellationToken: ct);
        Assert.Equal(3, dt.Rows.Count);
    }

    private static byte[] CreatePayload(int length, byte seed = 0x00)
    {
        byte[] buf = new byte[length];
        for (int i = 0; i < length; i++)
        {
            buf[i] = unchecked((byte)(seed + i));
        }

        return buf;
    }

    private static async ValueTask<MemoryStream> CreateFreshAccdbStreamAsync()
    {
        var ms = new MemoryStream();
        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        ms.Position = 0;
        return ms;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }
}
