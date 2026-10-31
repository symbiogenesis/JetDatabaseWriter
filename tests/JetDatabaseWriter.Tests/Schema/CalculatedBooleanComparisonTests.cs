namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native comparisons project intrinsic Boolean results to signed integers before text conversion.</summary>
public sealed class CalculatedBooleanComparisonTests
{
    [Theory]
    [InlineData("native-comparison.accdb", "7311AD9297128B442881B4B557318BF488BAD130F9A98288688C6470FCDA0001")]
    [InlineData("native-comparison-compacted.accdb", "7B0CDF39814A5DFC2CEB8608FE56CDA7FCC1958866B8392360E796DA8880B58C")]
    public async Task NativeBooleanComparisons_InsertAndUpdateMatchEveryCache(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedBooleanComparison", fixture);
        byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(original)));
        object[] expected;
        await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
        {
            expected = Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken));
        }

        await using var stream = new MemoryStream();
        stream.Write(original);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            object?[] values = new object?[expected.Length];
            values[0] = 1;
            values[1] = true;
            await writer.InsertRowAsync("T", values, TestContext.Current.CancellationToken);
            Assert.Equal(2, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Flag"] = true }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] rows = await actual.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Length);
        foreach (object[] row in rows)
        {
            Assert.Equal(expected, row);
        }
    }
}
