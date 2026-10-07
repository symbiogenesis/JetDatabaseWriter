namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native encryption conversion refuses unsupported security changes before writing.</summary>
public sealed class EncryptionMutationTests
{
    /// <summary>Native encryption creation is refused without changing a plaintext database.</summary>
    /// <param name="format">The native database format.</param>
    /// <param name="encryption">The requested encryption provider.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, AccessEncryptionFormat.Jet4Rc4)]
    [InlineData(DatabaseFormat.AceAccdb, AccessEncryptionFormat.AccdbAgile)]
    public async Task EncryptAsync_UnsupportedNativeCreation_LeavesSourceUntouched(DatabaseFormat format, AccessEncryptionFormat encryption)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
        }

        byte[] original = stream.ToArray();
        await Assert.ThrowsAsync<NotSupportedException>(async () => await AccessWriter.EncryptAsync(stream, "Native123".AsMemory(), encryption, TestContext.Current.CancellationToken));
        Assert.Equal(original, stream.ToArray());
    }

    /// <summary>Opening and disposing native encrypted files does not rewrite the complete ciphertext.</summary>
    /// <param name="fixture">The independently created native fixture.</param>
    [Theory]
    [InlineData("NativeJet4Rc4.mdb")]
    [InlineData("NativeAceAgile.accdb")]
    public async Task OpenDispose_NativeFixture_LeavesCiphertextUntouched(string fixture)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
        }

        Assert.Equal(original, stream.ToArray());
    }
}
