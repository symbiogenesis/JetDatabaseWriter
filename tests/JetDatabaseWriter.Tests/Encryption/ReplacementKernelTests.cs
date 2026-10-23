namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Verification;
using Xunit;

/// <summary>Checks the executable replacement evidence boundary and its production admission.</summary>
public sealed class ReplacementKernelTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Every byte position retains its full unsigned bits, including signed-negative encodings.</summary>
    [Fact]
    public void DecodeReplacementLength_AllBytePositions_MatchesBinaryPrimitives()
    {
        byte[] bytes = new byte[8];
        for (int position = 0; position < bytes.Length; position++)
        {
            for (int value = 0; value <= byte.MaxValue; value++)
            {
                Array.Clear(bytes);
                bytes[position] = checked((byte)value);
                Assert.Equal(BinaryPrimitives.ReadUInt64LittleEndian(bytes), Decode(bytes));
            }
        }

        foreach (ulong value in new ulong[] { 0, 2147483648, 2147483649, long.MaxValue, 9223372036854775808, ulong.MaxValue })
        {
            BinaryPrimitives.WriteUInt64LittleEndian(bytes, value);
            ulong decoded = Decode(bytes);
            Assert.Equal(value, decoded);
            long signed = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            Assert.Equal(signed is >= 0 and <= 2147483648, RecoveryKernel.IsDatabaseLength(decoded));
        }
    }

    /// <summary>A partial decision cannot grant commit authority, regardless of its validation flag.</summary>
    [Fact]
    public void ReplacementAdmission_EveryIncompleteDecision_SelectsPrepared()
    {
        for (ulong length = 256; length < 352; length++)
        {
            Assert.True(RecoveryKernel.IsReplacementIntentLength(length));
            Assert.Equal((byte)1, RecoveryKernel.ReplacementAdmission(length, true, 0, 2147483648, false));
            Assert.Equal((byte)1, RecoveryKernel.ReplacementAdmission(length, true, 2147483648, 0, true));
        }

        Assert.Equal((byte)2, RecoveryKernel.ReplacementAdmission(352, true, 0, 2147483648, true));
    }

    /// <summary>Each malformed framing, signed length or complete evidence class refuses.</summary>
    [Fact]
    public void ReplacementAdmission_InvalidEvidence_CannotBorrowCommitAuthority()
    {
        foreach (ulong length in new ulong[] { 0, 255, 353, long.MaxValue, ulong.MaxValue })
        {
            Assert.False(RecoveryKernel.IsReplacementIntentLength(length));
            Assert.Equal((byte)0, RecoveryKernel.ReplacementAdmission(length, true, 0, 0, true));
        }

        foreach (ulong length in new ulong[] { 256, 257, 351, 352 })
        {
            Assert.Equal((byte)0, RecoveryKernel.ReplacementAdmission(length, false, 0, 0, true));
            foreach (ulong invalid in new ulong[] { 2147483649, long.MaxValue, 9223372036854775808, ulong.MaxValue })
            {
                Assert.Equal((byte)0, RecoveryKernel.ReplacementAdmission(length, true, invalid, 0, true));
                Assert.Equal((byte)0, RecoveryKernel.ReplacementAdmission(length, true, 0, invalid, true));
            }
        }

        Assert.Equal((byte)0, RecoveryKernel.ReplacementAdmission(352, true, 0, 0, false));
    }

    /// <summary>Checksummed signed-field edge encodings refuse before mutation and retain identical evidence across retries.</summary>
    /// <param name="offset">The old or new length field.</param>
    /// <param name="encoded">The signed value whose bits are serialized.</param>
    [Theory]
    [InlineData(64, long.MinValue)]
    [InlineData(72, long.MinValue)]
    [InlineData(64, long.MaxValue)]
    [InlineData(72, long.MaxValue)]
    [InlineData(64, 2147483649L)]
    [InlineData(72, 2147483649L)]
    public async Task Recover_InvalidEncodedLength_PreservesAllEvidence(int offset, long encoded)
    {
        using var directory = new EvidenceDirectory();
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        string stage = directory.Path + ".input.tmp";
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, directory.Path, phase =>
        {
            if (phase == "committed")
            {
                throw new IOException("Retain complete replacement evidence.");
            }
        }));

        string intent = EncryptionReplacementIntent.IntentPath(directory.Path);
        byte[] bytes = await File.ReadAllBytesAsync(intent, Ct);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(offset), encoded);
        SHA256.HashData(bytes.AsSpan(0, 224)).CopyTo(bytes, 224);
        await File.WriteAllBytesAsync(intent, bytes, Ct);
        Dictionary<string, string> expected = Snapshot(directory.Path);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            Assert.Throws<JetCorruptDataException>(() => EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
            Assert.Equal(expected, Snapshot(directory.Path));
        }
    }

    /// <summary>Checksum-only reserved fields and arbitrary incomplete decisions retain the documented selection.</summary>
    /// <param name="decisionLength">The complete or incomplete suffix length.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(48)]
    [InlineData(95)]
    [InlineData(96)]
    public async Task Recover_ReservedFieldsAndArbitraryPartialDecision_SelectsRecordedImage(int decisionLength)
    {
        using var directory = new EvidenceDirectory();
        byte[] original = [1, 2, 3];
        byte[] replacement = [4, 5, 6, 7];
        string stage = directory.Path + ".input.tmp";
        await File.WriteAllBytesAsync(directory.Path, original, Ct);
        await File.WriteAllBytesAsync(stage, replacement, Ct);
        Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(stage, directory.Path, phase =>
        {
            if (phase == "committed")
            {
                throw new IOException("Retain complete replacement evidence.");
            }
        }));

        string intent = EncryptionReplacementIntent.IntentPath(directory.Path);
        byte[] bytes = await File.ReadAllBytesAsync(intent, Ct);
        bytes.AsSpan(144, 80).Fill(0xA5);
        SHA256.HashData(bytes.AsSpan(0, 224)).CopyTo(bytes, 224);
        if (decisionLength == 96)
        {
            bytes.AsSpan(312, 8).Fill(0xA5);
            SHA256.HashData(bytes.AsSpan(256, 64)).CopyTo(bytes, 320);
        }
        else
        {
            Array.Resize(ref bytes, 256 + decisionLength);
            bytes.AsSpan(256).Fill(0xA5);
        }

        await File.WriteAllBytesAsync(intent, bytes, Ct);
        Assert.True(EncryptionReplacementIntent.Recover(directory.Path, cancellationToken: Ct));
        Assert.Equal(decisionLength == 96 ? replacement : original, await File.ReadAllBytesAsync(directory.Path, Ct));
        Assert.False(File.Exists(intent));
    }

    private static ulong Decode(byte[] bytes) => RecoveryKernel.DecodeReplacementLength(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);

    private static Dictionary<string, string> Snapshot(string path) => Directory.GetFiles(Path.GetDirectoryName(path)!)
        .Order(StringComparer.Ordinal).ToDictionary(static file => file, DigestFile, StringComparer.Ordinal);

    private static string DigestFile(string path)
    {
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        return Convert.ToHexString(SHA256.HashData(input));
    }

    private sealed class EvidenceDirectory : IDisposable
    {
        private readonly string directory = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "jdw-replacement-kernel-" + Guid.NewGuid().ToString("N"));

        internal EvidenceDirectory()
        {
            Directory.CreateDirectory(this.directory);
            this.Path = System.IO.Path.Combine(this.directory, "database.accdb");
        }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(this.directory, recursive: true);
    }
}
