namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class LongValueCorruptionTests
{
    /// <summary>All six high length bits survive descriptor serialization independently of the storage mode.</summary>
    /// <param name="length">The native 30-bit length.</param>
    /// <param name="mode">The supported storage mode.</param>
    [Theory]
    [InlineData(0x1000000, (byte)0)]
    [InlineData(0x1000000, (byte)0x40)]
    [InlineData(0x1000000, (byte)0x80)]
    [InlineData(0x3FFFFFFF, (byte)0)]
    [InlineData(0x3FFFFFFF, (byte)0x40)]
    [InlineData(0x3FFFFFFF, (byte)0x80)]
    public void NativeLengthHighBits_SurviveDescriptorRoundTrip(int length, byte mode)
    {
        var expected = new LongValueDescriptor(length, mode, 256, 42);
        Assert.True(LongValueDescriptor.TryRead(expected.ToHeaderBytes(), out LongValueDescriptor actual));
        Assert.Equal(expected, actual);
    }

    /// <summary>A native length beyond the default budget is refused before page reads or payload allocation.</summary>
    [Fact]
    public async Task NativeLengthHighBits_TriggerConfiguredBudgetBeforePageRead()
    {
        byte[] descriptor = LongValueDescriptor.Chained(0, 256, 0).ToHeaderBytes();
        descriptor[3] = 1;
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!);
        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await decoder.ReadLongValueBytesExactAsync(descriptor, 0, descriptor.Length, TestContext.Current.CancellationToken));
        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    /// <summary>The reserved LVAL mode cannot become an empty value or trigger page traversal.</summary>
    /// <param name="length">The declared payload size.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task ReservedStorageMode_IsRejectedBeforePageRead(int length)
    {
        byte[] descriptor = LongValueDescriptor.Chained(length, 256, 0).ToHeaderBytes();
        descriptor[3] = Constants.LongValue.StorageModeMask;
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await decoder.ReadLongValueBytesExactAsync(descriptor, 0, descriptor.Length, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task ExternalValue_RefusesConfiguredBudgetBeforePageRead()
    {
        byte[] descriptor = LongValueDescriptor.Chained(100, 256, 0).ToHeaderBytes();
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!, maxValueBytes: 10);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await decoder.ReadLongValueBytesExactAsync(descriptor, 0, descriptor.Length, TestContext.Current.CancellationToken));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InlineValue_CannotReadFollowingColumnBytes(bool raw)
    {
        // The page has enough bytes, but the long-value column slice does not.
        byte[] row = new byte[40];
        LongValueDescriptor.Inline(8).WriteTo(row);
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!);

        await Assert.ThrowsAsync<InvalidDataException>(async () =>
        {
            if (raw)
            {
                _ = await decoder.ReadLongValueRawBytesAsync(row, 0, 14, TestContext.Current.CancellationToken);
            }
            else
            {
                _ = await decoder.ReadLongValueAsync(row, 0, 14, isOle: false, TestContext.Current.CancellationToken);
            }
        });
    }

    [Fact]
    public async Task InlineValue_ReadsExactlyDeclaredPayload()
    {
        byte[] row = new byte[40];
        LongValueDescriptor.Inline(3).WriteTo(row);
        byte[] expected = [1, 2, 3];
        expected.CopyTo(row.AsSpan(12));
        row[15] = 99;
        var decoder = new LongValueDecoder(JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb), null!);

        byte[] actual = await decoder.ReadLongValueBytesExactAsync(row, 0, 16, TestContext.Current.CancellationToken);

        Assert.Equal(expected, actual);
    }
}
