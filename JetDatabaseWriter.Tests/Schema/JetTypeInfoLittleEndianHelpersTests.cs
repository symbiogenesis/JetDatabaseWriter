namespace JetDatabaseWriter.Tests.Schema;

using System;
using JetDatabaseWriter.Schema;
using Xunit;

public sealed class JetTypeInfoLittleEndianHelpersTests
{
    [Fact]
    public void Ri16_And_Ru16_ReadExpectedValues()
    {
        byte[] buffer = [0xAA, 0x34, 0x12, 0xCC, 0xFF, 0xFF];

        Assert.Equal(0x1234, JetTypeInfo.Ru16(buffer, 1));
        Assert.Equal(0x1234, JetTypeInfo.Ru16(buffer.AsSpan(), 1));
        Assert.Equal(-1, JetTypeInfo.Ri16(buffer, 4));
        Assert.Equal(-1, JetTypeInfo.Ri16(buffer.AsSpan(), 4));
    }

    [Fact]
    public void Ri32_And_Ru32_ReadExpectedValues()
    {
        byte[] buffer = [0xAA, 0x78, 0x56, 0x34, 0x12, 0xCC, 0xFF, 0xFF, 0xFF, 0xFF];

        Assert.Equal(0x12345678, JetTypeInfo.Ri32(buffer, 1));
        Assert.Equal(0x12345678u, JetTypeInfo.Ru32(buffer, 1));
        Assert.Equal(-1, JetTypeInfo.Ri32(buffer, 6));
        Assert.Equal(uint.MaxValue, JetTypeInfo.Ru32(buffer, 6));

        Assert.Equal(0x12345678, JetTypeInfo.Ri32(buffer.AsSpan(), 1));
        Assert.Equal(0x12345678u, JetTypeInfo.Ru32(buffer.AsSpan(), 1));
        Assert.Equal(-1, JetTypeInfo.Ri32(buffer.AsSpan(), 6));
        Assert.Equal(uint.MaxValue, JetTypeInfo.Ru32(buffer.AsSpan(), 6));
    }

    [Fact]
    public void Ri64_ReadsExpectedValues()
    {
        byte[] buffer = [
            0xAA,
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01,
            0xCC,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
        ];

        Assert.Equal(0x0102030405060708L, JetTypeInfo.Ri64(buffer, 1));
        Assert.Equal(-1L, JetTypeInfo.Ri64(buffer, 10));

        Assert.Equal(0x0102030405060708L, JetTypeInfo.Ri64(buffer.AsSpan(), 1));
        Assert.Equal(-1L, JetTypeInfo.Ri64(buffer.AsSpan(), 10));
    }

    [Fact]
    public void Wi16_And_Wu16_WriteExpectedBytes()
    {
        byte[] buffer = new byte[6];

        JetTypeInfo.Wi16(buffer, 0, -2);
        JetTypeInfo.Wu16(buffer, 2, 0xBEEF);
        JetTypeInfo.Wi16(buffer.AsSpan(), 4, 0x1234);

        Assert.Equal(new byte[] { 0xFE, 0xFF, 0xEF, 0xBE, 0x34, 0x12 }, buffer);
    }

    [Fact]
    public void Wi16_OutOfRange_ThrowsOverflowException()
    {
        byte[] buffer = new byte[2];

        Assert.Throws<OverflowException>(() => JetTypeInfo.Wi16(buffer, 0, short.MaxValue + 1));
        Assert.Throws<OverflowException>(() => JetTypeInfo.Wi16(buffer.AsSpan(), 0, short.MinValue - 1));
    }

    [Fact]
    public void Wu16_OutOfRange_ThrowsOverflowException()
    {
        byte[] buffer = new byte[2];

        Assert.Throws<OverflowException>(() => JetTypeInfo.Wu16(buffer, 0, -1));
        Assert.Throws<OverflowException>(() => JetTypeInfo.Wu16(buffer.AsSpan(), 0, 0x1_0000));
    }

    [Fact]
    public void Wi32_And_Wi64_WriteExpectedBytes()
    {
        byte[] buffer = new byte[16];

        JetTypeInfo.Wi32(buffer, 0, -2);
        JetTypeInfo.Wi32(buffer.AsSpan(), 4, 0x12345678);
        JetTypeInfo.Wi64(buffer, 8, -1L);

        byte[] expectedWi64 =
        [
            0xFE, 0xFF, 0xFF, 0xFF,
            0x78, 0x56, 0x34, 0x12,
            0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF,
        ];

        Assert.Equal(expectedWi64, buffer);
    }

    [Fact]
    public void Wi64_SpanOverload_WritesExpectedBytes()
    {
        byte[] buffer = new byte[8];

        JetTypeInfo.Wi64(buffer.AsSpan(), 0, 0x0102030405060708L);

        Assert.Equal(new byte[] { 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01 }, buffer);
    }

    [Fact]
    public void Wu32_UintAndIntOverloads_WriteExpectedBitPatterns()
    {
        byte[] buffer = new byte[8];

        JetTypeInfo.Wu32(buffer, 0, 0x12345678u);
        JetTypeInfo.Wu32(buffer.AsSpan(), 4, -1);

        Assert.Equal(new byte[] { 0x78, 0x56, 0x34, 0x12, 0xFF, 0xFF, 0xFF, 0xFF }, buffer);
    }
}
