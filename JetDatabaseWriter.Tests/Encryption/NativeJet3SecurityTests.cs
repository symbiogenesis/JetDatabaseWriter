namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>DAO36-produced Jet3 headers establish native ANSI password maintenance.</summary>
public sealed class NativeJet3SecurityTests
{
    [Fact]
    public async Task PasswordHeader_MatchesDaoNewPassword()
    {
        byte[] source = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "UpstreamJet3Rc4.mdb"), TestContext.Current.CancellationToken);
        byte[] oracle = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet3Password.mdb"), TestContext.Current.CancellationToken);
        byte[] unmasked = source.AsSpan(0, 2048).ToArray();
        EncryptionManager.TransformHeaderMask(unmasked, DatabaseFormat.Jet3Mdb);
        uint key = BinaryPrimitives.ReadUInt32LittleEndian(unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey, 4));
        EncryptionManager.WriteNativeJet3EncryptionHeader(source, key, "Native123");
        Assert.Equal(oracle.AsSpan(0x18, Constants.DatabaseHeader.Jet3MaskLength).ToArray(), source.AsSpan(0x18, Constants.DatabaseHeader.Jet3MaskLength).ToArray());
        Assert.Equal(oracle.AsSpan(2048).ToArray(), source.AsSpan(2048).ToArray());
        await using var stream = new MemoryStream(source, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using System.Data.DataTable rows = await reader.ReadTableAsync("Table1", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
    }

    [Fact]
    public async Task DaoCreatedJet3_UsesNativeCodePagePassword()
    {
        await using AccessReader reader = await AccessReader.OpenAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet3Rc4.mdb"), new AccessReaderOptions("Pássword") { UseLockFile = false }, TestContext.Current.CancellationToken);
        using System.Data.DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal("Native encrypted row", Assert.Single(rows.Select())["Label"]);
    }

    [Theory]
    [InlineData("Bad\0Password")]
    [InlineData("123456789012345678901")]
    [InlineData("漢字")]
    public async Task UnrepresentablePassword_LeavesHeaderUnchanged(string password)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet3Rc4.mdb"), TestContext.Current.CancellationToken);
        byte[] header = image.AsSpan(0, 2048).ToArray();
        byte[] before = (byte[])header.Clone();
        Assert.ThrowsAny<Exception>(() => EncryptionManager.WriteNativeJet3EncryptionHeader(header, 1, password));
        Assert.Equal(before, header);
    }
}
