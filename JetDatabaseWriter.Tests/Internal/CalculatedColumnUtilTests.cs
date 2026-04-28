namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Buffers.Binary;
using System.Text;
using JetDatabaseWriter;
using JetDatabaseWriter.Internal.Helpers;
using Xunit;

/// <summary>
/// Round-trip tests for the 23-byte calculated-value envelope helper used by
/// Access 2010+ calculated (expression) columns. Translated from Jackcess
/// <c>CalculatedColumnUtil</c>; see
/// <c>docs/design/calculated-columns-format-notes.md</c>.
/// </summary>
public sealed class CalculatedColumnUtilTests
{
    [Fact]
    public void Wrap_Then_Unwrap_RoundTrips_FixedPayload()
    {
        byte[] payload = [0x2A, 0x00, 0x00, 0x00]; // int32 little-endian: 42
        byte[] wrapped = CalculatedColumnUtil.Wrap(payload);

        Assert.Equal(Constants.CalculatedColumn.ExtraDataLen + payload.Length, wrapped.Length);
        Assert.Equal(
            payload.Length,
            BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(Constants.CalculatedColumn.DataLenOffset, 4)));

        byte[] unwrapped = CalculatedColumnUtil.Unwrap(wrapped);
        Assert.Equal(payload, unwrapped);
    }

    [Fact]
    public void Wrap_Then_Unwrap_RoundTrips_VariablePayload()
    {
        byte[] payload = Encoding.Unicode.GetBytes("hello");
        byte[] wrapped = CalculatedColumnUtil.Wrap(payload);
        byte[] unwrapped = CalculatedColumnUtil.Unwrap(wrapped);
        Assert.Equal(payload, unwrapped);
    }

    [Fact]
    public void Wrap_EmptyPayload_ProducesBareWrapper()
    {
        byte[] wrapped = CalculatedColumnUtil.Wrap([]);
        Assert.Equal(Constants.CalculatedColumn.ExtraDataLen, wrapped.Length);
        Assert.Equal(0, BinaryPrimitives.ReadInt32LittleEndian(wrapped.AsSpan(Constants.CalculatedColumn.DataLenOffset, 4)));
        Assert.Equal([], CalculatedColumnUtil.Unwrap(wrapped));
    }

    [Fact]
    public void Unwrap_TooShortInput_ReturnsInputUnchanged()
    {
        // Defensive parity with Jackcess CalculatedColumnUtil.unwrapCalculatedValue:
        // when the input is shorter than the 20-byte header, return as-is.
        byte[] tiny = [1, 2, 3];
        Assert.Same(tiny, CalculatedColumnUtil.Unwrap(tiny));
    }

    [Fact]
    public void Unwrap_TruncatedPayload_ReturnsAvailableBytes()
    {
        // Caller corrupted the wrapper by claiming a payload longer than what is
        // actually present in the buffer — we must clamp to the available bytes
        // (Math.Min(remaining, declaredLen)) and never over-read.
        byte[] wrapped = new byte[Constants.CalculatedColumn.ExtraDataLen + 8];
        BinaryPrimitives.WriteInt32LittleEndian(
            wrapped.AsSpan(Constants.CalculatedColumn.DataLenOffset, 4),
            64); // declared length far exceeds the buffer's tail (only 11 bytes after offset 20)

        byte[] unwrapped = CalculatedColumnUtil.Unwrap(wrapped);
        Assert.Equal(wrapped.Length - Constants.CalculatedColumn.DataOffset, unwrapped.Length);
    }

    [Fact]
    public void Constants_MatchJackcessValues()
    {
        // Pin the on-disk constants so a regression in Constants.cs can't silently
        // break round-trips with files Microsoft Access produced.
        Assert.Equal(23, Constants.CalculatedColumn.ExtraDataLen);
        Assert.Equal(16, Constants.CalculatedColumn.DataLenOffset);
        Assert.Equal(20, Constants.CalculatedColumn.DataOffset);
        Assert.Equal(39, Constants.CalculatedColumn.FixedFieldLen);
        Assert.Equal(0xC0, Constants.CalculatedColumn.ExtFlagMask);
    }
}
