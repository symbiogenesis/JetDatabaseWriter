namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

/// <summary>Independently authored native en-US profiles retain the complete calculation result set.</summary>
public sealed class CalculatedEnglishProfileTests
{
    [Theory]
    [InlineData("native-profile.accdb", "791DD25255861189A738FB3995D4FD779A1056768E6017228AFFEA38A3260980")]
    [InlineData("native-profile-compacted.accdb", "0B080A8E5085B3C2C3A4925E99B5F8E3A3E59DC845F6374968BB71D4E2961FF2")]
    public async Task NativeProfile_AllCachedValuesMatchWriterInsertAndRecalculation(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedEnglishProfile", fixture);
        byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(original)));
        object[] expected;
        await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            expected = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
            Assert.Equal(26, expected.Length);
        }

        await using var stream = new MemoryStream();
        stream.Write(original);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = CultureInfo.GetCultureInfo("en-US") }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", new object?[] { 1, 1234.5d, new DateTime(2020, 3, 4, 13, 45, 0), "iIıİäÄéÉßœ漢" }.Concat(Enumerable.Repeat<object?>(null, 22)).ToArray(), TestContext.Current.CancellationToken);
            Assert.Equal(2, await writer.UpdateRowsAsync("T", "N", 1234.5d, new Dictionary<string, object?> { ["N"] = 1234.5d }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader actualReader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] rows = await actualReader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Length);
        foreach (object[] row in rows)
        {
            for (int column = 0; column < expected.Length; column++)
            {
                Assert.True(Equals(expected[column], row[column]), $"Native profile mismatch at column {column}: expected {expected[column]}, actual {row[column]}.");
            }
        }
    }
}
