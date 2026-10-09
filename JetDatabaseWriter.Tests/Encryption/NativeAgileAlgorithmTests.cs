namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Independent parameter-vector checks; these synthetic vectors do not establish Microsoft producer interoperability.</summary>
public sealed class NativeAgileAlgorithmTests
{
    /// <summary>Password and data hash algorithms and key lengths are used independently.</summary>
    /// <param name="passwordHash">The password digest.</param>
    /// <param name="dataHash">The page digest.</param>
    /// <param name="passwordBits">The password key size.</param>
    /// <param name="dataBits">The page key size.</param>
    /// <param name="chaining">The declared cipher chaining.</param>
    [Theory]
    [InlineData("SHA1", "SHA256", 256, 128, "ChainingModeCBC")]
    [InlineData("SHA256", "SHA384", 192, 256, "ChainingModeCBC")]
    [InlineData("SHA384", "SHA512", 128, 192, "ChainingModeCFB")]
    [InlineData("SHA512", "SHA1", 256, 256, "ChainingModeCFB")]
    public async Task NativePageUsesDeclaredAlgorithms(string passwordHash, string dataHash, int passwordBits, int dataBits, string chaining)
    {
        (byte[] header, byte[] salt, byte[] dataKey, uint encoding) = await CreateVariantHeaderAsync(passwordHash, dataHash, passwordBits, dataBits, chaining);
        byte[] ivInput = new byte[20];
        salt.CopyTo(ivInput, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(ivInput.AsSpan(16), encoding ^ 17u);
        byte[] iv = Hash(ivInput, dataHash).AsSpan(0, 16).ToArray();
        byte[] page = new byte[4096];
        for (int i = 0; i < page.Length; i++)
        {
            page[i] = (byte)(i & 255);
        }

        byte[] expected = Encrypt(page, dataKey, iv, chaining);
        using NativeAgilePageCodec codec = OfficeCryptoAgile.CreateFlatPageCodec(header, "vector", 7, 4096, cancellationToken: TestContext.Current.CancellationToken);
        byte[] transformed = (byte[])page.Clone();
        codec.Encode(transformed, 0, 17, 4096);
        Assert.Equal(expected, transformed);
        codec.Decode(transformed, 0, 17, 4096);
        Assert.Equal(page, transformed);
        Assert.Throws<UnauthorizedAccessException>(() => OfficeCryptoAgile.CreateFlatPageCodec(header, "wrong", 7, 4096, cancellationToken: TestContext.Current.CancellationToken));
    }

    /// <summary>Immutable Microsoft compact outputs retain the declared alternative Agile algorithms.</summary>
    /// <param name="fixture">The DAO compacted database.</param>
    /// <param name="expectedHash">The pinned SHA-256 digest.</param>
    [Theory]
    [InlineData("NativeAceAgileMixedCbc.accdb", "FB86804BD61188FDE4C684BC538D75E3ECE4FD7998BAC045D371B532B56A313E")]
    [InlineData("NativeAceAgileMixedCfb8.accdb", "18D9535FD1C8AD88435669B659397B9BC7099C2E468A505D1DA8275F658A6DD5")]
    public async Task DaoCompactedVariantReadsAndMutates(string fixture, string expectedHash)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, fixture), TestContext.Current.CancellationToken);
        Assert.Equal(expectedHash, Convert.ToHexString(Hash(original, "SHA256")));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("vector") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(2, rows.Rows.Count);
            Assert.Equal("Native encrypted row", Assert.Single(rows.Select("Id = 7"))["Label"]);
            Assert.Equal("DAO updated", Assert.Single(rows.Select("Id = 8"))["Label"]);
            using DataTable added = await reader.ReadTableAsync("Added", cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(1, Assert.Single(added.Select())["Id"]);
        }

        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("vector") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [9, "After native compact"], TestContext.Current.CancellationToken);
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken))
            {
                await writer.InsertRowAsync("T", [10, "Rolled back"], TestContext.Current.CancellationToken);
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }

            await writer.CreateTableAsync("AfterCompact", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("AfterCompact", [11], TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("Maintenance", [new ColumnDefinition("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Maintenance", [new byte[32_000]], TestContext.Current.CancellationToken);
            long populatedLength = stream.Length;
            await writer.DropTableAsync("Maintenance", TestContext.Current.CancellationToken);
            Assert.True(await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken) > 0);
            Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
            Assert.True(stream.Length < populatedLength);
        }

        Assert.Equal(original.AsSpan(0, 4096).ToArray(), stream.ToArray().AsSpan(0, 4096).ToArray());
        stream.Position = 0;
        await using AccessReader reopened = await AccessReader.OpenAsync(stream, new AccessReaderOptions("vector") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable updated = await reopened.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(3, updated.Rows.Count);
        Assert.Equal("After native compact", Assert.Single(updated.Select("Id = 9"))["Label"]);
        using DataTable created = await reopened.ReadTableAsync("AfterCompact", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(11, Assert.Single(created.Select())["Id"]);
    }

    /// <summary>Builds an independently encrypted full-file vector, without native producer provenance.</summary>
    /// <param name="passwordHash">The password digest.</param>
    /// <param name="dataHash">The page digest.</param>
    /// <param name="passwordBits">The password key size.</param>
    /// <param name="dataBits">The page key size.</param>
    /// <param name="chaining">The declared chaining.</param>
    /// <returns>The complete synthetic native-page vector.</returns>
    internal static async Task<byte[]> CreateVariantAsync(string passwordHash, string dataHash, int passwordBits, int dataBits, string chaining)
    {
        byte[] native = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        using (NativeAgilePageCodec original = OfficeCryptoAgile.CreateFlatPageCodec(native, "Native123", 100_000, 4096, cancellationToken: TestContext.Current.CancellationToken))
        {
            for (int offset = 4096; offset < native.Length; offset += 4096)
            {
                original.Decode(native, offset, offset / 4096, 4096);
            }
        }

        (byte[] header, byte[] salt, byte[] dataKey, uint encoding) = await CreateVariantHeaderAsync(passwordHash, dataHash, passwordBits, dataBits, chaining);
        header.CopyTo(native, 0);
        for (int offset = 4096; offset < native.Length; offset += 4096)
        {
            byte[] ivInput = new byte[20];
            salt.CopyTo(ivInput, 0);
            BinaryPrimitives.WriteUInt32LittleEndian(ivInput.AsSpan(16), encoding ^ checked((uint)(offset / 4096)));
            byte[] iv = Hash(ivInput, dataHash).AsSpan(0, 16).ToArray();
            Encrypt(native.AsSpan(offset, 4096).ToArray(), dataKey, iv, chaining).CopyTo(native, offset);
        }

        return native;
    }

    private static async Task<(byte[] Header, byte[] Salt, byte[] DataKey, uint Encoding)> CreateVariantHeaderAsync(string passwordHash, string dataHash, int passwordBits, int dataBits, string chaining)
    {
        byte[] native = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        byte[] header = native.AsSpan(0, 4096).ToArray();
        byte[] salt = new byte[16];
        salt.AsSpan().Fill(0x12);
        byte[] verifier = new byte[16];
        verifier.AsSpan().Fill(0x34);
        byte[] dataKey = new byte[dataBits / 8];
        dataKey.AsSpan().Fill(0x56);
        byte[] verifierHash = Hash(verifier, passwordHash);
        byte[] inputKey = Derive(salt, "vector", passwordHash, passwordBits, Convert.FromHexString("FEA7D2763B4B9E79"));
        byte[] hashKey = Derive(salt, "vector", passwordHash, passwordBits, Convert.FromHexString("D7AA0F6D3061344E"));
        byte[] valueKey = Derive(salt, "vector", passwordHash, passwordBits, Convert.FromHexString("146E0BE7ABACD0D6"));
        string xml = $"<encryption xmlns=\"http://schemas.microsoft.com/office/2006/encryption\" xmlns:p=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\"><keyData saltSize=\"16\" blockSize=\"16\" keyBits=\"{dataBits}\" hashSize=\"{Hash(salt, dataHash).Length}\" cipherAlgorithm=\"AES\" cipherChaining=\"{chaining}\" hashAlgorithm=\"{dataHash}\" saltValue=\"{Convert.ToBase64String(salt)}\"/><keyEncryptors><keyEncryptor uri=\"http://schemas.microsoft.com/office/2006/keyEncryptor/password\"><p:encryptedKey spinCount=\"7\" saltSize=\"16\" blockSize=\"16\" keyBits=\"{passwordBits}\" hashSize=\"{verifierHash.Length}\" cipherAlgorithm=\"AES\" cipherChaining=\"{chaining}\" hashAlgorithm=\"{passwordHash}\" saltValue=\"{Convert.ToBase64String(salt)}\" encryptedVerifierHashInput=\"{Convert.ToBase64String(Encrypt(verifier, inputKey, salt, chaining))}\" encryptedVerifierHashValue=\"{Convert.ToBase64String(Encrypt(Pad(verifierHash), hashKey, salt, chaining))}\" encryptedKeyValue=\"{Convert.ToBase64String(Encrypt(Pad(dataKey), valueKey, salt, chaining))}\"/></keyEncryptor></keyEncryptors></encryption>";
        byte[] descriptor = Encoding.UTF8.GetBytes(xml);
        int originalLength = BinaryPrimitives.ReadUInt16LittleEndian(header.AsSpan(0x299));
        Array.Clear(header, 0x299, 2 + Math.Max(originalLength, descriptor.Length + 8));
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0x299), checked((ushort)(descriptor.Length + 8)));
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(0x29B), 0x0000004000040004UL);
        descriptor.CopyTo(header, 0x2A3);
        byte[] unmasked = (byte[])header.Clone();
        EncryptionManager.TransformHeaderMask(unmasked);
        uint encoding = BinaryPrimitives.ReadUInt32LittleEndian(unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey));
        return (header, salt, dataKey, encoding);
    }

    private static byte[] Derive(byte[] salt, string password, string algorithm, int bits, byte[] blockKey)
    {
        byte[] passwordBytes = Encoding.Unicode.GetBytes(password);
        byte[] initial = new byte[salt.Length + passwordBytes.Length];
        salt.CopyTo(initial, 0);
        passwordBytes.CopyTo(initial, salt.Length);
        byte[] h = Hash(initial, algorithm);
        for (int i = 0; i < 7; i++)
        {
            byte[] next = new byte[4 + h.Length];
            BinaryPrimitives.WriteInt32LittleEndian(next, i);
            h.CopyTo(next, 4);
            h = Hash(next, algorithm);
        }

        byte[] final = new byte[h.Length + blockKey.Length];
        h.CopyTo(final, 0);
        blockKey.CopyTo(final, h.Length);
        byte[] result = new byte[bits / 8];
        result.AsSpan().Fill(0x36);
        h = Hash(final, algorithm);
        h.AsSpan(0, Math.Min(h.Length, result.Length)).CopyTo(result);
        return result;
    }

    private static byte[] Hash(byte[] bytes, string algorithm)
    {
        using var hash = IncrementalHash.CreateHash(new HashAlgorithmName(algorithm));
        hash.AppendData(bytes);
        return hash.GetHashAndReset();
    }

    private static byte[] Pad(byte[] bytes)
    {
        byte[] padded = new byte[(bytes.Length + 15) / 16 * 16];
        bytes.CopyTo(padded, 0);
        return padded;
    }

    private static byte[] Encrypt(byte[] bytes, byte[] key, byte[] iv, string chaining)
    {
        using var aes = Aes.Create();
#pragma warning disable CA5358, RS0030 // Native Office Agile compatibility vectors mandate CBC or CFB.
        aes.Mode = chaining == "ChainingModeCFB" ? CipherMode.CFB : CipherMode.CBC;
#pragma warning restore CA5358, RS0030 // Native Office Agile compatibility vectors mandate CBC or CFB.
        aes.FeedbackSize = 8;
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        aes.IV = iv;
#pragma warning disable CA5401 // This is an independent deterministic interoperability vector, not encryption of user data.
        using ICryptoTransform encryptor = aes.CreateEncryptor();
#pragma warning restore CA5401 // This is an independent deterministic interoperability vector, not encryption of user data.
        return encryptor.TransformFinalBlock(bytes, 0, bytes.Length);
    }
}
