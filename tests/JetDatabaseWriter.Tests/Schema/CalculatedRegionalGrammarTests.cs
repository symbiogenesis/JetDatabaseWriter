namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native table calculations retain date punctuation and their distinct text comparison rules.</summary>
public sealed class CalculatedRegionalGrammarTests
{
    [Theory]
    [InlineData("native-grammar.accdb", "F66DF2125AC5617EA31B16BC7CD0D676B35072A0D2318EB8C6D7B5094B3C39C6")]
    [InlineData("native-grammar-compacted.accdb", "ABC4A13E10C8D9414E1A3FA59C6CE216DDF9D08DDCBDB6F5717F68BDEE296BA2")]
    public async Task NativeDateAndLogicalGrammar_InsertAndUpdatePreserveAllCaches(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalGrammar", fixture);
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
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false, CalculatedExpressionCulture = System.Globalization.CultureInfo.GetCultureInfo("en-US") }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            object?[] inserted = new object?[expected.Length];
            inserted[0] = 1;
            await writer.InsertRowAsync("T", inserted, TestContext.Current.CancellationToken);
            Assert.Equal(2, await writer.UpdateRowsAsync("T", "Id", 1, new Dictionary<string, object?> { ["Id"] = 1 }, TestContext.Current.CancellationToken));
        }

        stream.Position = 0;
        await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        object[][] rows = await actual.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Length);
        var differences = new List<string>();
        foreach (object[] row in rows)
        {
            for (int column = 0; column < expected.Length; column++)
            {
                if (!Equals(expected[column], row[column]))
                {
                    differences.Add($"Column {column}: native {expected[column]}, writer {row[column]}");
                }
            }
        }

        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences.Distinct()));
    }
}
