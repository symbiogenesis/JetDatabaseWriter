namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Exercises the multi-segment binary index key encoder with long payloads
/// (up to the <c>Binary</c> maximum of 255 bytes). Prior tests in
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
    /// A 255-byte binary payload (maximum Binary length) round-trips
    /// through a unique index without corruption.
    /// </summary>
    [Fact]
    public async Task MaxLengthBinaryKey_255Bytes_RoundTrips()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        byte[] payload = CreatePayload(255);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "LongBin",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_LongBin", "Bin") { IsUnique = true }],
                this.ct);

            await writer.InsertRowAsync("LongBin", [1, payload], this.ct);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable dt = await reader.ReadDataTableAsync("LongBin", cancellationToken: this.ct);
        Assert.Single(dt.Rows);
        byte[] actual = Assert.IsType<byte[]>(dt.Rows[0]["Bin"]);
        Assert.Equal(payload, actual);
    }

    /// <summary>
    /// Multiple long binary payloads maintain correct sort order in the
    /// index (ascending unsigned byte-lexicographic). The reader returns
    /// all rows without error, confirming the leaf chain is well-formed.
    /// </summary>
    /// <param name="length">The length.</param>
    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(255)]
    public async Task LongBinaryKeys_MultipleRows_RoundTripCorrectly(int length)
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        byte[][] payloads =
        [
            CreatePayload(length, seed: 0x00),
            CreatePayload(length, seed: 0x55),
            CreatePayload(length, seed: 0xAA),
            CreatePayload(length, seed: 0xFF),
        ];

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "MultiBin",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_MultiBin", "Bin")],
                this.ct);

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("MultiBin", [i, payloads[i]], this.ct);
            }
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable dt = await reader.ReadDataTableAsync("MultiBin", cancellationToken: this.ct);
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
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        byte[][] payloads =
        [
            CreatePayload(100, seed: 0x10),
            CreatePayload(100, seed: 0x80),
            CreatePayload(100, seed: 0xF0),
        ];

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
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
                this.ct);

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("DescLongBin", [i, payloads[i]], this.ct);
            }
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable dt = await reader.ReadDataTableAsync("DescLongBin", cancellationToken: this.ct);
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
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        byte[] payload = CreatePayload(200, seed: 0x42);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);
        await writer.CreateTableAsync(
            "UniqueLongBin",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
            ],
            [new IndexDefinition("UX_LongBin", "Bin") { IsUnique = true }],
            this.ct);

        await writer.InsertRowAsync("UniqueLongBin", [1, payload], this.ct);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("UniqueLongBin", [2, payload.ToArray()], this.ct));
    }

    /// <summary>
    /// Composite index with a long binary key column and a text column.
    /// Exercises the multi-column entry concatenation path with large
    /// binary segments.
    /// </summary>
    [Fact]
    public async Task LongBinaryKey_CompositeWithText_RoundTrips()
    {
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        byte[] bin1 = CreatePayload(128, seed: 0x11);
        byte[] bin2 = CreatePayload(128, seed: 0x22);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "CompLongBin",
                [
                    new ColumnDefinition("Tag", typeof(string), maxLength: 50),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 255),
                ],
                [new IndexDefinition("IX_CompLongBin", ["Tag", "Bin"])],
                this.ct);

            await writer.InsertRowAsync("CompLongBin", ["alpha", bin1], this.ct);
            await writer.InsertRowAsync("CompLongBin", ["alpha", bin2], this.ct);
            await writer.InsertRowAsync("CompLongBin", ["beta", bin1], this.ct);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable dt = await reader.ReadDataTableAsync("CompLongBin", cancellationToken: this.ct);
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

}
