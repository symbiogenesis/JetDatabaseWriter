namespace JetDatabaseWriter.Tests.Core;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention.

/// <summary>
/// Tests for binary-key index support:
/// <c>T_BINARY (0x09)</c> is a fully supported index key column type.
/// Variable-length raw binary keys are encoded via the same Jackcess
/// "general binary entry" packing already used for <c>T_GUID</c> — 8-byte
/// zero-padded segments, intermediate length byte <c>0x09</c>, final length
/// byte = remaining valid count, with descending flipping data bytes and
/// the final length byte but leaving intermediate length bytes unflipped.
/// <para>
/// Prior to binary-key index support an <see cref="IndexDefinition"/> over a <c>T_BINARY</c>
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

    [Fact]
    public async Task CreateTable_IndexOnBinaryColumn_BulkInsertRoundTrips()
    {
        // T_BINARY is byte[] with MaxLength in [1, 255]. The bulk maintenance
        // loop encodes every snapshot row through IndexKeyEncoder; binary-key indexes
        // wires T_BINARY into that switch so the create-then-insert flow
        // no longer throws NotSupportedException.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "BinIdx",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 32),
                ],
                [new IndexDefinition("IX_Bin", "Bin")],
                ct);

            byte[][] payloads =
            [
                [0x01, 0x02, 0x03],
                [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80],
                [0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90],
                [0xFF],
            ];

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("BinIdx", [i, payloads[i]], ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        var rows = await reader.ReadDataTableAsync("BinIdx", cancellationToken: ct);
        Assert.Equal(4, rows.Rows.Count);
    }

    [Fact]
    public async Task CreateTable_UniqueIndexOnBinary_DetectsDuplicate()
    {
        // The post-write unique check post-write unique check runs as part of the bulk
        // maintenance loop and uses the same encoder. With T_BINARY now
        // supported the check fires; without binary-key indexes it would throw
        // NotSupportedException before reaching the duplicate detection.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using var writer = await OpenWriterAsync(stream);

        await writer.CreateTableAsync(
            "BinUnique",
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Bin", typeof(byte[]), maxLength: 16),
            ],
            [new IndexDefinition("UX_Bin", "Bin") { IsUnique = true }],
            ct);

        byte[] payload = [0xCA, 0xFE, 0xBA, 0xBE];
        await writer.InsertRowAsync("BinUnique", [1, payload], ct);

        // Inserting the same byte payload again must trip the unique check.
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await writer.InsertRowAsync("BinUnique", [2, payload.ToArray()], ct));
    }

    [Fact]
    public async Task CreateTable_MultiColumnIndexWithBinary_BulkInsertRoundTrips()
    {
        // Composite index over (Text, Binary). The maintenance loop
        // concatenates per-column entry blocks, so the binary block must
        // round-trip alongside text without the multi-column path bailing
        // back to the schema-only fall-through.
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "BinComposite",
                [
                    new ColumnDefinition("Tag", typeof(string), maxLength: 50),
                    new ColumnDefinition("Bin", typeof(byte[]), maxLength: 16),
                ],
                [new IndexDefinition("IX_Composite", CompositeKeyColumns)],
                ct);

            await writer.InsertRowAsync("BinComposite", ["alpha", new byte[] { 0x01, 0x02 }], ct);
            await writer.InsertRowAsync("BinComposite", ["beta", new byte[] { 0x03, 0x04, 0x05 }], ct);
            await writer.InsertRowAsync("BinComposite", ["alpha", new byte[] { 0x09 }], ct);
        }

        await using var reader = await OpenReaderAsync(stream);
        var rows = await reader.ReadDataTableAsync("BinComposite", cancellationToken: ct);
        Assert.Equal(3, rows.Rows.Count);
    }

    [Fact]
    public async Task CreateTable_DescendingBinaryIndex_BulkInsertRoundTrips()
    {
        // Descending binary keys exercise the post-loop bulk bit-flip path
        // (data bytes + final length byte flip; intermediate 0x09 stays).
        await using var stream = await CreateFreshAccdbStreamAsync();
        var ct = TestContext.Current.CancellationToken;

        await using (var writer = await OpenWriterAsync(stream))
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
                ct);

            byte[][] payloads =
            [
                [0x10],
                [0x20, 0x30],
                [0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33],
            ];

            for (int i = 0; i < payloads.Length; i++)
            {
                await writer.InsertRowAsync("BinDesc", [i, payloads[i]], ct);
            }
        }

        await using var reader = await OpenReaderAsync(stream);
        var rows = await reader.ReadDataTableAsync("BinDesc", cancellationToken: ct);
        Assert.Equal(3, rows.Rows.Count);
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
