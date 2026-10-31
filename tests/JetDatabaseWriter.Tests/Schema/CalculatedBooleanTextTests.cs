namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native text functions preserve their distinct intrinsic Boolean conversion rules.</summary>
public sealed class CalculatedBooleanTextTests
{
    [Theory]
    [InlineData("native-text.accdb", "B20FFA010EA32E20C43F9DE278C52E9EF19590551212E84F7095FFFED7DD3077")]
    [InlineData("native-text-compacted.accdb", "E32C16F4DC78D432C6708B72EB22AD024A7FA55CA874BF35EE464EDBDDFAF8C0")]
    public async Task NativeBooleanTextFunctions_InsertAndUpdateMatchEveryCache(string fixture, string hash)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedBooleanText", fixture);
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
