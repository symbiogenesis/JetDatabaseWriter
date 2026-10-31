namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native ANSI conversion and Unicode casing remain independent of user regional profiles.</summary>
public sealed class CalculatedRegionalAnsiTests
{
    [Theory]
    [InlineData("ar-SA")]
    [InlineData("de-DE")]
    [InlineData("en-US")]
    [InlineData("fr-FR")]
    [InlineData("ja-JP")]
    [InlineData("tr-TR")]
    public async Task NativeAnsiAndCaseCaches_MatchInsertAndUpdate(string culture)
    {
        foreach (string fixture in new[] { "native-profile.accdb", "native-profile-compacted.accdb" })
        {
            string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalProfiles", culture, fixture);
            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            var controls = new Dictionary<string, object[]>();
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                foreach (string table in new[] { "AnsiStrings0", "AnsiStrings128", "RegionalCase" })
                {
                    controls.Add(table, Assert.Single(await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)));
                }
            }

            await using var writable = new MemoryStream();
            writable.Write(original);
            writable.Position = 0;
            await using (AccessWriter writer = await AccessWriter.OpenAsync(writable, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
            {
                foreach ((string table, object[] expected) in controls)
                {
                    object?[] input = [.. expected.Take(4), .. Enumerable.Repeat<object?>(null, expected.Length - 4)];
                    await writer.InsertRowAsync(table, input, TestContext.Current.CancellationToken);
                    Assert.Equal(2, await writer.UpdateRowsAsync(table, "Id", 1, new Dictionary<string, object?> { ["N"] = 1234.5d }, TestContext.Current.CancellationToken));
                }
            }

            writable.Position = 0;
            await using AccessReader actual = await AccessReader.OpenAsync(writable, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            foreach ((string table, object[] expected) in controls)
            {
                object[][] rows = await actual.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
                Assert.Equal(2, rows.Length);
                foreach (object[] row in rows)
                {
                    for (int column = 0; column < expected.Length; column++)
                    {
                        Assert.True(Equals(expected[column], row[column]), $"{culture}/{fixture}/{table}, column {column}: expected {expected[column]}, actual {row[column]}.");
                    }
                }
            }
        }
    }
}
