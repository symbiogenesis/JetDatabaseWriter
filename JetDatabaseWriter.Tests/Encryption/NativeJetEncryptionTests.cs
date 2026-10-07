namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Reads immutable fixtures created by DAO.DBEngine.120 on 2026-10-06 with dbVersion40 (64), dbEncrypt (2), locale LANGID=0x0409/CP=1252 and password Native123.</summary>
public sealed class NativeJetEncryptionTests
{
    /// <summary>Password protection is independent of the native page encoding key.</summary>
    /// <param name="fixture">The Microsoft DAO-created fixture.</param>
    [Theory]
    [InlineData("NativeJet4Rc4.mdb")]
    [InlineData("NativeJet4Password.mdb")]
    public async Task NativeJet4_ReadsKnownRow(string fixture)
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, fixture);
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Native123") { UseLockFile = false }, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(rows.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Id"]);
        Assert.Equal("Native encrypted row", row["Label"]);
    }

    /// <summary>The native header password is verified even when no pages are encrypted.</summary>
    /// <param name="fixture">The Microsoft DAO-created fixture.</param>
    [Theory]
    [InlineData("NativeJet4Rc4.mdb")]
    [InlineData("NativeJet4Password.mdb")]
    public async Task NativeJet4_WrongPasswordIsRefused(string fixture)
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, fixture);
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("wrong") { UseLockFile = false }, TestContext.Current.CancellationToken);
        });
    }

    /// <summary>Updating native encrypted pages preserves the original native header key and password.</summary>
    [Fact]
    public async Task NativeJet4_WriterUpdatesEncryptedPages()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [8, "Library updated native file"], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0x3E, 4).ToArray(), stream.ToArray().AsSpan(0x3E, 4).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 8) && Equals(row["Label"], "Library updated native file"));
    }

    /// <summary>Unsupported native password conversion fails without changing the caller's bytes.</summary>
    /// <param name="operation">The public conversion operation.</param>
    [Theory]
    [InlineData("Decrypt")]
    [InlineData("ChangePassword")]
    public async Task NativeJet4_PasswordConversionLeavesSourceUntouched(string operation)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            if (operation == "Decrypt")
            {
                await AccessWriter.DecryptAsync(stream, "Native123".AsMemory(), TestContext.Current.CancellationToken);
            }
            else
            {
                await AccessWriter.ChangePasswordAsync(stream, "Native123".AsMemory(), "changed".AsMemory(), cancellationToken: TestContext.Current.CancellationToken);
            }
        });

        Assert.Equal(original, stream.ToArray());
    }
}
