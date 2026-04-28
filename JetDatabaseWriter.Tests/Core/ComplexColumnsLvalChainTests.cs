namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Round-trip tests: attachment payloads larger than the legacy
/// 256-byte inline-OLE cap are pushed onto freshly-allocated LVAL data pages
/// (single-page <c>0x40</c> form for sub-page payloads, chained <c>0x00</c>
/// form for multi-page payloads) and re-read by
/// <see cref="IAccessReader.GetAttachmentsAsync"/>.
/// </summary>
public sealed class ComplexColumnsLvalChainTests
{
    [Fact]
    public async Task AddAttachmentAsync_PayloadLargerThanInlineCap_RoundTripsViaSinglePageLval()
    {
        // 1 KB binary payload — well above the 256-byte inline-OLE cap, but
        // small enough that the wrapper header + payload fit in a single LVAL
        // row (bitmask 0x40). Use a .jpg extension so AttachmentWrapper.Encode
        // skips deflate (typeFlag = 0x00) and the bytes round-trip verbatim.
        byte[] payload = BuildDeterministicPayload(1024);

        await RoundTripAttachmentAsync("photo.jpg", payload);
    }

    [Fact]
    public async Task AddAttachmentAsync_PayloadLargerThanOnePage_RoundTripsViaChainedLval()
    {
        // 16 KB binary payload — across a 4 KB page that's at least 4 chained
        // LVAL pages (bitmask 0x00). Use .jpg so the wrapper stores raw bytes.
        byte[] payload = BuildDeterministicPayload(16 * 1024);

        await RoundTripAttachmentAsync("video-frame.jpg", payload);
    }

    [Fact]
    public async Task AddAttachmentAsync_LargeDeflatedTextPayload_RoundTrips()
    {
        // 2 KB highly-compressible text payload — covers the deflate path
        // (typeFlag = 0x01) over LVAL pages.
        var bytes = new byte[2048];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)'a';
        }

        await RoundTripAttachmentAsync("notes.txt", bytes);
    }

    private static async Task RoundTripAttachmentAsync(string fileName, byte[] payload)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(
            ms,
            DatabaseFormat.AceAccdb,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "Documents",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true },
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "Documents",
                [1, DBNull.Value],
                TestContext.Current.CancellationToken);

            await writer.AddAttachmentAsync(
                "Documents",
                "Files",
                new Dictionary<string, object> { ["Id"] = 1 },
                new AttachmentInput(fileName, payload),
                TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(
            ms,
            leaveOpen: true,
            cancellationToken: TestContext.Current.CancellationToken);

        var attachments = await reader.GetAttachmentsAsync(
            "Documents",
            "Files",
            TestContext.Current.CancellationToken);

        var single = Assert.Single(attachments);
        Assert.Equal(fileName, single.FileName);
        Assert.Equal(payload, single.FileData);
    }

    private static byte[] BuildDeterministicPayload(int length)
    {
        var bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            // Mix two strides so simple page-boundary copy bugs surface as
            // mismatched bytes rather than aligned-zero gaps. Unchecked because
            // the test project enables checked arithmetic by default.
            unchecked
            {
                bytes[i] = (byte)((i * 31) ^ (i >> 8));
            }
        }

        return bytes;
    }
}
