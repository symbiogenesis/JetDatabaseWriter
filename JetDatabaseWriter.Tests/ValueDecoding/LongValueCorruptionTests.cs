namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

public sealed class LongValueCorruptionTests
{
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