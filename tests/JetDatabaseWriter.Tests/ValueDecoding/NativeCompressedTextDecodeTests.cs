namespace JetDatabaseWriter.Tests.ValueDecoding;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native Text payloads use one-byte mode transitions while assigned field bounds remain authoritative.</summary>
public sealed class NativeCompressedTextDecodeTests
{
    [Theory]
    [InlineData("native-mixed-transition.bin", "abc\u4E2Ddef", "BEEC04B7D7AA7314C130288890FF8F16831837769227CDDAF1DF2117122F8BEB")]
    [InlineData("native-longer-transition.bin", "abcdef\u4E2Dghijkl", "9342E1D4480F908B2246A7A5AA273D2D5A12205F8E4E0237B35B52310A638FDA")]
    [InlineData("native-low-zero-plain.bin", "abc\u0100def", "DB8D73AD60C9693311E98AD18F2D6DB3F1A3FBE3FDC3FC45E0ABDD03563818E8")]
    [InlineData("specified-low-zero-compressed.bin", "\u0001bcDEFG", "0ABDDC4869A6B1DA84FD0A5DC95536A691C3D740FC8444BE9B9B452EE0F5BC74")]
    [InlineData("specified-mode-only-suffix.bin", "abcd\u4E2Ddef", "B0E04A1094C56FF95A3E499DE343EF2E85A99A927089572A0417C822B15D7A6C")]
    [InlineData("specified-odd-unicode-tail.bin", "abcde\u4E2Ddef", "589C0421CCE1533B04D805C029E9D34441857D875684C99D449E1270C9E6E42C")]
    public async Task NativeFieldBytes_UseSingleZeroTransitionsWithoutReadingOutsideTheSlice(string fixture, string expected, string sha256)
    {
        byte[] stored = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "NativeText", fixture), TestContext.Current.CancellationToken);
        Assert.Equal(sha256, Convert.ToHexString(SHA256.HashData(stored)));
        byte[] guarded = [0xCC, .. stored, 0xBB];
        Assert.Equal(expected, JetFormat.DecodeJet4Text(guarded, 1, stored.Length));
    }
}
