namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using JetDatabaseWriter.Schema.Expressions;
using Xunit;

/// <summary>Pins native calculated casing across every nonzero nonsurrogate BMP code unit.</summary>
public sealed class CalculatedTextCaseTests
{
    [Theory]
    [InlineData("native-case.accdb", "FC3613EC3A98F15FFD4AA7D429641E3C0DD1F82A4FAE623599ACC5BBD2119A3A")]
    [InlineData("native-case-compacted.accdb", "46E46BADA9CDF9C2D77245A63F9BEFAB0B9086C30805E0640D19DA19A4207AD3")]
    public async Task NativeAlphabet_CasingMatchesFrozenNativeResults(string name, string hash)
        => await AssertCasingAsync(name, hash, supplementary: false);

    [Theory]
    [InlineData("native-supplementary-case.accdb", "2CBA417BA8561A386C6AC6FF23CC868BBBC8AB73134863C1AE2A20554225C69D")]
    [InlineData("native-supplementary-case-compacted.accdb", "427A1CB1FFFB73341817F42E681F2F8F32E19C8021DCC496F79970082A3B9180")]
    public async Task NativeSupplementaryAlphabet_CasingMatchesFrozenNativeResults(string name, string hash)
        => await AssertCasingAsync(name, hash, supplementary: true);

    private static async Task AssertCasingAsync(string name, string hash, bool supplementary)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedTextCase", name), TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        List<object[]> rows = await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken);
        var characters = new HashSet<char>();
        var differences = new List<string>();
        var scalars = new HashSet<int>();
        foreach (object[] row in rows)
        {
            string text = Assert.IsType<string>(row[1]);
            string expectedUpper = Assert.IsType<string>(row[2]);
            string expectedLower = Assert.IsType<string>(row[3]);
            string upper = CalculatedExpressionTextFunctions.ChangeCase(text, upper: true);
            string lower = CalculatedExpressionTextFunctions.ChangeCase(text, upper: false);
            for (int index = 0; index < text.Length; index++)
            {
                if (expectedUpper[index] != upper[index] || expectedLower[index] != lower[index])
                {
                    differences.Add($"{(int)text[index]:X4}: native={(int)expectedUpper[index]:X4}/{(int)expectedLower[index]:X4}, actual={(int)upper[index]:X4}/{(int)lower[index]:X4}");
                }
            }

            foreach (char character in text)
            {
                if (!supplementary)
                {
                    Assert.False(character == '\0' || char.IsSurrogate(character));
                    Assert.True(characters.Add(character));
                }
            }

            if (supplementary)
            {
                foreach (System.Text.Rune scalar in text.EnumerateRunes())
                {
                    Assert.True(scalar.Value >= 0x10000);
                    Assert.True(scalars.Add(scalar.Value));
                }
            }
        }

        Assert.Equal(supplementary ? 1536 : 63487, supplementary ? scalars.Count : characters.Count);
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
        Assert.Equal(bytes, stream.ToArray());
    }
}
