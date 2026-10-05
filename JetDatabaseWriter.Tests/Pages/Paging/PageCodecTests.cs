namespace JetDatabaseWriter.Tests.Pages.Paging;

using System;
using System.IO;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>Characterizes page cipher bytes and page-store/cache ownership.</summary>
public sealed class PageCodecTests
{
    /// <summary>The NIST AES-128 ECB vector pins the legacy page cipher.</summary>
    [Fact]
    public void Aes_RecordedVector_AndHeaderBypass()
    {
        using var codec = new AesEcbPageCodec(Convert.FromHexString("000102030405060708090A0B0C0D0E0F"));
        byte[] page = Convert.FromHexString("00112233445566778899AABBCCDDEEFF");
        codec.Encode(page, 0, 1, page.Length);
        Assert.Equal("69C4E0D86A7B0430D8CDB78070B4C55A", Convert.ToHexString(page));
        codec.Decode(page, 0, 1, page.Length);
        Assert.Equal("00112233445566778899AABBCCDDEEFF", Convert.ToHexString(page));
        codec.Encode(page, 0, 0, page.Length);
        Assert.Equal("00112233445566778899AABBCCDDEEFF", Convert.ToHexString(page));
    }

    /// <summary>Pins the legacy RC4 page-key derivation and stream bytes.</summary>
    [Fact]
    public void Jet4_RecordedVector()
    {
        using var codec = new Jet4Rc4PageCodec(0x12345678);
        byte[] page = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        codec.Encode(page, 0, 7, page.Length);
        Assert.Equal("759D1298FE6A97217A760F40B0E6BA52", Convert.ToHexString(page));
        codec.Decode(page, 0, 7, page.Length);
        Assert.Equal(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 }, page);
    }

    /// <summary>CFB magic is only valid on the synthetic ACE page format.</summary>
    [Fact]
    public void CompoundHeader_OnJet_RefusesBeforePasswordValidation()
        => Assert.Throws<InvalidDataException>(() => PageCodecFactory.Open(new byte[128], DatabaseFormat.Jet4Mdb, true, default, "password"));

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
