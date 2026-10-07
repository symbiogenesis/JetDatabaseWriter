namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native encrypted Jet3 fixture coverage using the independently published Jackcess Encrypt db97-enc.mdb.</summary>
public sealed class NativeJet3EncryptionTests
{
    /// <summary>The native encoding key enables RC4 without a password or raw-byte flag.</summary>
    [Fact]
    public async Task NativeJet3_ReadsUpstreamRowsWithoutPassword()
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, "UpstreamJet3Rc4.mdb");
        Assert.Equal(AccessEncryptionFormat.Jet3Rc4, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["ID"], 1) && Equals(row["col1"], "hello") && Equals(row["col2"], 0));
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["ID"], 2) && Equals(row["col1"], "world") && Equals(row["col2"], 42));
    }

    /// <summary>Updating encrypted Jet3 data preserves its original encoding key and header.</summary>
    [Fact]
    public async Task NativeJet3_WriterPreservesHeaderAndUpdatesPages()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "UpstreamJet3Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("Table1", [3, "native write", 43], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0, Constants.PageSizes.Jet3).ToArray(), stream.ToArray().AsSpan(0, Constants.PageSizes.Jet3).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["col1"], "native write"));
    }

    /// <summary>The native single-byte password field accepts its full 20 bytes and rejects missing or mismatched passwords.</summary>
    [Fact]
    public async Task NativeJet3_HeaderPasswordIsIndependentOfPageEncryption()
    {
        const string password = "12345678901234567890";
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "UpstreamJet3Rc4.mdb"), TestContext.Current.CancellationToken);
        EncryptionManager.TransformHeaderMask(fixture, DatabaseFormat.Jet3Mdb);
        for (int i = 0; i < password.Length; i++)
        {
            fixture[Constants.DatabaseHeader.Password + i] = checked((byte)password[i]);
        }

        EncryptionManager.TransformHeaderMask(fixture, DatabaseFormat.Jet3Mdb);
        foreach (string? supplied in new string?[] { null, "wrong", password + "x", password + "\0" })
        {
            await using var refused = new MemoryStream(fixture, writable: false);
            await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            {
                await using AccessReader reader = await AccessReader.OpenAsync(refused, new AccessReaderOptions(supplied) { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
            });
        }

        await using var stream = new MemoryStream(fixture, writable: false);
        await using AccessReader accepted = await AccessReader.OpenAsync(stream, new AccessReaderOptions(password) { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await accepted.ReadDataTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
    }
}
