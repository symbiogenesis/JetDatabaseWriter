namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.CompoundFile;
using JetDatabaseWriter.Encryption;
using Xunit;

public sealed class CompoundEncryptionCorruptionTests
{
    [Fact]
    public async Task EncryptedCompoundProbe_RejectsTruncatedCfbInsteadOfPlaintextFallback()
    {
        byte[] bytes = new byte[512];
        Constants.CompoundFile.Signature.CopyTo(bytes);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            EncryptionManager.TryDecryptCompoundFileWithFormatAsync(
                stream, bytes, "password".AsMemory(), "Password", TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task EncryptedCompoundProbe_RejectsMissingEncryptionStreams()
    {
        byte[] bytes = CompoundFileWriter.Build([new KeyValuePair<string, byte[]>("Unexpected", [1, 2, 3])]);
        await using var stream = new MemoryStream(bytes);

        await Assert.ThrowsAsync<InvalidDataException>(() =>
            EncryptionManager.TryDecryptCompoundFileWithFormatAsync(
                stream, bytes, "password".AsMemory(), "Password", TestContext.Current.CancellationToken).AsTask());
    }
}
