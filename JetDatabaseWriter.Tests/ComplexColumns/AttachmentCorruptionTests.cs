namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

public sealed class AttachmentCorruptionTests
{
    [Fact]
    public async Task PublicAttachmentRead_EnforcesConfiguredContentBudget()
    {
        await using MemoryStream database = await CreateAttachmentDatabaseAsync();
        await using AccessReader reader = await AccessReader.OpenAsync(
            database,
            new AccessReaderOptions { UseLockFile = false, MaxAttachmentContentBytes = 20 },
            leaveOpen: true,
            cancellationToken: Ct);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.GetAttachmentsAsync("Docs", "Files", Ct));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Fact]
    public async Task PublicAttachmentRead_RejectsChecksumCorruption()
    {
        await using MemoryStream database = await CreateAttachmentDatabaseAsync();
        byte[] bytes = database.ToArray();
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        int offset = bytes.AsSpan().IndexOf(wrapped);
        Assert.True(offset >= 0);
        bytes[offset + wrapped.Length - 1] ^= 1;
        await using var corrupted = new MemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(
            corrupted, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await reader.GetAttachmentsAsync("Docs", "Files", Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PublicRowRead_CorruptAttachmentObeysParsingPolicy(bool strict)
    {
        await using MemoryStream database = await CreateAttachmentDatabaseAsync();
        byte[] bytes = database.ToArray();
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        int offset = bytes.AsSpan().IndexOf(wrapped);
        Assert.True(offset >= 0);
        bytes[offset + wrapped.Length - 1] ^= 1;
        await using var corrupted = new MemoryStream(bytes);
        await using AccessReader reader = await AccessReader.OpenAsync(
            corrupted,
            new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, DiagnosticsEnabled = true },
            leaveOpen: true,
            cancellationToken: Ct);

        if (strict)
        {
            await Assert.ThrowsAsync<InvalidDataException>(async () =>
                await reader.ReadTableAsync("Docs", cancellationToken: Ct));
        }
        else
        {
            using System.Data.DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: Ct);
            Assert.Equal(DBNull.Value, table.Rows[0]["Files"]);
        }
    }

    private static async Task<MemoryStream> CreateAttachmentDatabaseAsync()
    {
        var stream = new MemoryStream();
        try
        {
            await using (AccessWriter writer = await CreateWriterAsync(stream))
            {
                await writer.CreateTableAsync(
                    "Docs",
                    [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }],
                    Ct);
                await writer.InsertRowAsync("Docs", [1, DBNull.Value], Ct);
                await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("a.txt", [1, 2, 3]), Ct);
            }

            stream.Position = 0;
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    [Fact]
    public void CompressedAttachment_RejectsExpansionBeyondDeclaredLength()
    {
        byte[] wrapped = AttachmentWrapper.Encode("txt", new byte[1024 * 1024]);
        BinaryPrimitives.WriteUInt32LittleEndian(wrapped.AsSpan(4), 14);

        Assert.False(AttachmentWrapper.TryDecode(wrapped, out _, out _));
    }

    [Fact]
    public void CompressedAttachment_RejectsInvalidChecksum()
    {
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        wrapped[^1] ^= 1;

        Assert.False(AttachmentWrapper.TryDecode(wrapped, out _, out _));
    }

    [Fact]
    public void CompressedAttachment_RejectsMissingZlibFraming()
    {
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        byte[] malformed = new byte[wrapped.Length - 6];
        wrapped.AsSpan(0, 8).CopyTo(malformed);
        wrapped.AsSpan(10, wrapped.Length - 14).CopyTo(malformed.AsSpan(8));

        Assert.False(AttachmentWrapper.TryDecode(malformed, out _, out _));
    }
}
