namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using JetDatabaseWriter.ComplexColumns.Models;
using Xunit;

public sealed class AttachmentCorruptionTests
{
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
    public void CompressedAttachment_RejectsRawDeflateLegacyWrapper()
    {
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        byte[] legacy = new byte[wrapped.Length - 6];
        wrapped.AsSpan(0, 8).CopyTo(legacy);
        wrapped.AsSpan(10, wrapped.Length - 14).CopyTo(legacy.AsSpan(8));

        Assert.False(AttachmentWrapper.TryDecode(legacy, out _, out _));
    }
}