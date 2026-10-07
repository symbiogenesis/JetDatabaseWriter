namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using JetDatabaseWriter.Encryption;
using Xunit;

/// <summary>Characterizes page cipher bytes and page-store/cache ownership.</summary>
public sealed class PageCodecTests
{
    /// <summary>Pins native Jet RC4 encoding-key XOR page-number stream bytes.</summary>
    [Fact]
    public void Jet4_RecordedVector()
    {
        using var codec = new Jet4Rc4PageCodec(0x12345678);
        byte[] page = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        codec.Encode(page, 0, 7, page.Length);
        Assert.Equal("C8220B25A404F94DFE920996B12C9BBA", Convert.ToHexString(page));
        codec.Decode(page, 0, 7, page.Length);
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, page);
    }

    /// <summary>Jet3 masking uses the absolute page offset and repeats its mask.</summary>
    [Fact]
    public void Jet3_RecordedVector()
    {
        using var codec = new Jet3XorPageCodec([1, 2, 3]);
        byte[] page = [0, 1, 2, 3];
        codec.Encode(page, 0, 2, page.Length);
        Assert.Equal(new byte[] { 2, 2, 3, 1 }, page);
        codec.Decode(page, 0, 2, page.Length);
        Assert.Equal(new byte[] { 0, 1, 2, 3 }, page);
    }
}
