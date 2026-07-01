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
/// Tests for binary-key index support:
/// <c>Binary (0x09)</c> is a fully supported index key column type.
/// Variable-length raw binary keys are encoded via the same Jackcess
/// "general binary entry" packing already used for <c>Guid</c> — 8-byte
/// zero-padded segments, intermediate length byte <c>0x09</c>, final length
/// byte = remaining valid count, with descending flipping data bytes and
/// the final length byte but leaving intermediate length bytes unflipped.
/// <para>
/// Prior to binary-key index support an <see cref="IndexDefinition"/> over a <c>Binary</c>
/// column would throw <see cref="NotSupportedException"/> from
/// <c>IndexKeyEncoder.EncodeEntry</c> on the first row insert that
/// triggered <c>MaintainIndexesAsync</c>. These tests pin the new
/// happy-path behaviour: create-table → bulk insert → reader round-trip
/// → unique-violation detection → multi-column composite key → descending
/// direction.
/// </para>
/// </summary>
public sealed class IndexBinaryKeyTests
{
    private static readonly string[] CompositeKeyColumns = ["Tag", "Bin"];
    private static readonly string[] BinDescendingColumns = ["Bin"];
    private readonly CancellationToken ct = TestContext.Current.CancellationToken;

    [Fact]
    public async Task CreateTable_IndexOnBinaryColumn_BulkInsertRoundTrips()
    {
        // Binary is byte[] with MaxLength in [1, 255]. The bulk maintenance
        // loop encodes every snapshot row through IndexKeyEncoder, which must
        // accept Binary so create-then-insert on a binary-key index round-trips.
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "BinIdx",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 32),
                ],
                [new IndexDefinition("IX_Bin", "Bin")],
                this.ct);

            byte[][] payloads =
            [
                [0x01, 0x02, 0x03],
                [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80],
                [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90],
                [0xFF],
            ];

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("BinIdx", [i, payloads[i]], this.ct);
            }
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable rows = await reader.ReadDataTableAsync("BinIdx", cancellationToken: this.ct);
        Assert.Equal(4, rows.Rows.Count);
    }

    [Fact]
    public async Task CreateTable_UniqueIndexOnBinary_DetectsDuplicate()
    {
        // The post-write unique check post-write unique check runs as part of the bulk
        // maintenance loop and uses the same encoder. With Binary now
        // supported the check fires; without binary-key indexes it would throw
        // NotSupportedException before reaching the duplicate detection.
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct);

        await writer.CreateTableAsync(
            "BinUnique",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Bin", typeof(byte[]), maxLength: 16),
            ],
            [new IndexDefinition("UX_Bin", "Bin") { IsUnique = true }],
            this.ct);

        byte[] payload = [0xCA, 0xFE, 0xBA, 0xBE];
        await writer.InsertRowAsync("BinUnique", [1, payload], this.ct);

        // Inserting the same byte payload again must trip the unique check.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("BinUnique", [2, payload.ToArray()], this.ct));
    }

    [Fact]
    public async Task CreateTable_MultiColumnIndexWithBinary_BulkInsertRoundTrips()
    {
        // Composite index over (Text, Binary). The maintenance loop
        // concatenates per-column entry blocks, so the binary block must
        // round-trip alongside text without the multi-column path bailing
        // back to the schema-only fall-through.
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "BinComposite",
                [
                    new ColumnDefinition("Tag", typeof(string), maxLength: 50),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 16),
                ],
                [new IndexDefinition("IX_Composite", CompositeKeyColumns)],
                this.ct);

            await writer.InsertRowAsync("BinComposite", ["alpha", new byte[] { 0x01, 0x02 }], this.ct);
            await writer.InsertRowAsync("BinComposite", ["beta", new byte[] { 0x03, 0x04, 0x05 }], this.ct);
            await writer.InsertRowAsync("BinComposite", ["alpha", "\t"u8.ToArray()], this.ct);
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable rows = await reader.ReadDataTableAsync("BinComposite", cancellationToken: this.ct);
        Assert.Equal(3, rows.Rows.Count);
    }

    [Fact]
    public async Task CreateTable_DescendingBinaryIndex_BulkInsertRoundTrips()
    {
        // Descending binary keys exercise the post-loop bulk bit-flip path
        // (data bytes + final length byte flip; intermediate 0x09 stays).
        await using MemoryStream stream = await InMemoryAccessDatabase.CreateFreshAceAccdbStreamAsync(this.ct);

        await using (AccessWriter writer = await InMemoryAccessDatabase.OpenWriterAsync(stream, this.ct))
        {
            await writer.CreateTableAsync(
                "BinDesc",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 16),
                ],
                [
                    new IndexDefinition("IX_BinDesc", "Bin")
                    {
                        DescendingColumns = BinDescendingColumns,
                    },
                ],
                this.ct);

            byte[][] payloads =
            [
                [0x10],
                [0x20, 0x30],
                [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33],
            ];

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("BinDesc", [i, payloads[i]], this.ct);
            }
        }

        await using AccessReader reader = await InMemoryAccessDatabase.OpenReaderAsync(stream, this.ct);
        DataTable rows = await reader.ReadDataTableAsync("BinDesc", cancellationToken: this.ct);
        Assert.Equal(3, rows.Rows.Count);
    }
}
