namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
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
        Array.Clear(header, 0x299, header.Length - 0x299);
        BinaryPrimitives.WriteUInt16LittleEndian(header.AsSpan(0x299), checked((ushort)(descriptor.Length + 8)));
        BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(0x29B), 0x0000004000040004UL);
        descriptor.CopyTo(header, 0x2A3);
        byte[] unmasked = (byte[])header.Clone();
        EncryptionManager.TransformHeaderMask(unmasked);
        uint encoding = BinaryPrimitives.ReadUInt32LittleEndian(unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey));
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
