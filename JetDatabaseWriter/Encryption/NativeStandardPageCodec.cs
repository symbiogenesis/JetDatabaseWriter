namespace JetDatabaseWriter.Encryption;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using JetDatabaseWriter.Exceptions;

/// <summary>Flat ACE CryptoAPI RC4 and ECMA-376 Standard AES page providers.</summary>
/// <param name="passwordHash">The owned iterated password digest.</param>
/// <param name="encodingKey">The owned native encoding key.</param>
/// <param name="keyBits">The provider key size in bits.</param>
/// <param name="rc4">Whether the provider uses RC4.</param>
internal sealed class NativeStandardPageCodec(byte[] passwordHash, byte[] encodingKey, int keyBits, bool rc4) : IPageCodec
{
    /// <inheritdoc/>
    public bool HasEncryption => true;

    /// <summary>Authenticates a flat ACE binary provider from page zero.</summary>
    /// <param name="header">The raw database header.</param>
    /// <param name="password">The supplied password.</param>
    /// <param name="maxSpinCount">The permitted password iterations.</param>
    /// <param name="maxDescriptorBytes">The permitted descriptor bytes.</param>
    /// <param name="workBudget">The aggregate password hashing budget.</param>
    /// <param name="cancellationToken">Cancellation for password hashing.</param>
    /// <returns>The unlocked original provider.</returns>
    /// <exception cref="InvalidDataException">The descriptor is malformed.</exception>
    /// <exception cref="NotSupportedException">The provider algorithm is unsupported.</exception>
    /// <exception cref="UnauthorizedAccessException">The password is incorrect.</exception>
    /// <exception cref="JetLimitationException">The configured cryptographic budget is exceeded.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A configured budget is invalid.</exception>
    internal static NativeStandardPageCodec Open(byte[] header, ReadOnlySpan<char> password, int maxSpinCount, int maxDescriptorBytes, EncryptionWorkBudget? workBudget = null, CancellationToken cancellationToken = default)
    {
        if (maxSpinCount is < 0 or > 10_000_000)
        {
            throw new ArgumentOutOfRangeException(nameof(maxSpinCount));
        }

        JetDatabaseWriter.Infrastructure.Guard.Positive(maxDescriptorBytes, nameof(maxDescriptorBytes));

        cancellationToken.ThrowIfCancellationRequested();
        byte[] unmasked = (byte[])header.Clone();
        EncryptionManager.TransformHeaderMask(unmasked);
        const int infoOffset = 0x29B;
        if (unmasked.Length < infoOffset + 12)
        {
            throw new InvalidDataException("The native ACE encryption descriptor is truncated.");
        }

        int length = BinaryPrimitives.ReadUInt16LittleEndian(unmasked.AsSpan(0x299));
        if (length < 12 || length > Math.Min(unmasked.Length, 4096) - infoOffset)
        {
            throw new InvalidDataException("The native ACE encryption descriptor length is invalid.");
        }

        if (length > maxDescriptorBytes)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The native encryption descriptor exceeds the configured byte budget.");
        }

        ReadOnlySpan<byte> info = unmasked.AsSpan(infoOffset, length);
        int major = BinaryPrimitives.ReadUInt16LittleEndian(info);
        int minor = BinaryPrimitives.ReadUInt16LittleEndian(info[2..]);
        uint flags = BinaryPrimitives.ReadUInt32LittleEndian(info[4..]);
        if (major is not 2 and not 3 and not 4 || minor != 2 || (flags & 0x14) != 4)
        {
            throw new NotSupportedException("The native ACE binary encryption provider is unsupported.");
        }

        int headerSize = BinaryPrimitives.ReadInt32LittleEndian(info[8..]);
        if (headerSize < 32 || headerSize > info.Length - 12 || (headerSize & 1) != 0)
        {
            throw new InvalidDataException("The native ACE EncryptionHeader size is invalid.");
        }

        ReadOnlySpan<byte> provider = info.Slice(12, headerSize);
        uint providerFlags = BinaryPrimitives.ReadUInt32LittleEndian(provider);
        int algorithm = BinaryPrimitives.ReadInt32LittleEndian(provider[8..]);
        int hashAlgorithm = BinaryPrimitives.ReadInt32LittleEndian(provider[12..]);
        int bits = BinaryPrimitives.ReadInt32LittleEndian(provider[16..]);
        if (providerFlags != flags || BinaryPrimitives.ReadUInt32LittleEndian(provider[4..]) != 0 || hashAlgorithm is not 0 and not 0x8004)
        {
            throw new InvalidDataException("The native ACE provider flags, extra size or hash algorithm are invalid.");
        }

        if (algorithm == 0)
        {
            algorithm = (flags & 0x20) != 0 ? 0x660E : 0x6801;
        }

        bool isRc4 = algorithm == 0x6801;
        int aesBits = algorithm switch { 0x660E => 128, 0x660F => 192, 0x6610 => 256, _ => 0 };
        if (!isRc4 && aesBits == 0)
        {
            throw new NotSupportedException($"Native ACE encryption algorithm 0x{algorithm:X} is unsupported.");
        }

        if ((isRc4 && (flags & 0x20) != 0) || (!isRc4 && major == 2))
        {
            throw new InvalidDataException("The native ACE provider algorithm contradicts its version or AES flag.");
        }

        if (bits == 0)
        {
            string csp = Encoding.Unicode.GetString(provider[32..]).TrimEnd('\0').Trim();
            int rc4Default = csp.Length == 0 || csp.Contains(" base ", StringComparison.OrdinalIgnoreCase) ? 40 : 128;
            bits = isRc4 ? rc4Default : aesBits;
        }

        if (isRc4 ? bits is < 40 or > 128 || bits % 8 != 0 : bits != aesBits)
        {
            throw new InvalidDataException("The native ACE provider key length is invalid.");
        }

        // Compatibility-mode AES descriptors omit fAES and use no iterations.
        int iterations = isRc4 || (flags & 0x20) == 0 ? 0 : 50_000;
        if (iterations > maxSpinCount)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The native Standard password hashing exceeds the configured spin budget.");
        }

        ReadOnlySpan<byte> verifier = info[(12 + headerSize)..];
        int hashLength = isRc4 ? 20 : 32;
        if (verifier.Length != 40 + hashLength || BinaryPrimitives.ReadInt32LittleEndian(verifier) != 16 || BinaryPrimitives.ReadInt32LittleEndian(verifier[36..]) != 20)
        {
            throw new InvalidDataException("The native ACE password verifier sizes are invalid.");
        }

        workBudget?.Charge(iterations);
        ReadOnlySpan<char> nativePassword = password[..Math.Min(password.Length, 255)];
        byte[] passwordBytes = new byte[Encoding.Unicode.GetByteCount(nativePassword)];
        _ = Encoding.Unicode.GetBytes(nativePassword, passwordBytes);
        byte[] input = new byte[16 + passwordBytes.Length];
        verifier.Slice(4, 16).CopyTo(input);
        passwordBytes.CopyTo(input, 16);
        byte[] hash = OfficeCryptoPrimitives.Sha1(input);
        CryptographicOperations.ZeroMemory(input);
        CryptographicOperations.ZeroMemory(passwordBytes);
        byte[] iterated = new byte[24];
        try
        {
            for (int i = 0; i < iterations; i++)
            {
                if ((i & 255) == 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                }

                BinaryPrimitives.WriteInt32LittleEndian(iterated, i);
                hash.CopyTo(iterated, 4);
                OfficeCryptoPrimitives.HashSha1(iterated, hash);
            }

            cancellationToken.ThrowIfCancellationRequested();
            byte[] encoding = unmasked.AsSpan(Constants.DatabaseHeader.EncodingKey, 4).ToArray();
            var codec = new NativeStandardPageCodec(hash, encoding, bits, isRc4);
            byte[] key = codec.DeriveKey(0);
            byte[] encrypted = new byte[16 + hashLength];
            verifier.Slice(20, 16).CopyTo(encrypted);
            verifier[40..].CopyTo(encrypted.AsSpan(16));
            byte[]? plain = null;
            byte[]? expected = null;
            try
            {
                plain = codec.TransformBytes(encrypted, key, encrypt: false);
                expected = OfficeCryptoPrimitives.Sha1(plain.AsSpan(0, 16));
                if (!OfficeCryptoPrimitives.FixedTimeEquals(expected, plain.AsSpan(16), 20))
                {
                    codec.Dispose();
                    throw new UnauthorizedAccessException("The provided password is incorrect for this database.");
                }

                return codec;
            }
            finally
            {
                CryptographicOperations.ZeroMemory(key);
                OfficeCryptoPrimitives.ZeroIfNotNull(plain);
                OfficeCryptoPrimitives.ZeroIfNotNull(expected);
            }
        }
        catch
        {
            CryptographicOperations.ZeroMemory(hash);
            throw;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(iterated);
            CryptographicOperations.ZeroMemory(unmasked);
        }
    }

    /// <inheritdoc/>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize) => this.TransformPage(data, offset, pageNumber, pageSize, encrypt: false);

    /// <inheritdoc/>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize) => this.TransformPage(data, offset, pageNumber, pageSize, encrypt: true);

    /// <inheritdoc/>
    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(passwordHash);
        CryptographicOperations.ZeroMemory(encodingKey);
    }

    private byte[] DeriveKey(uint block)
    {
        byte[] input = new byte[24];
        passwordHash.CopyTo(input, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(input.AsSpan(20), block);
        byte[] hash = OfficeCryptoPrimitives.Sha1(input);
        try
        {
            if (rc4)
            {
                byte[] key = new byte[keyBits == 40 ? 16 : keyBits / 8];
                hash.AsSpan(0, keyBits / 8).CopyTo(key);
                return key;
            }

            byte[] pad = new byte[64];
            byte[] combined = new byte[40];
            try
            {
                pad.AsSpan().Fill(0x36);
                for (int i = 0; i < hash.Length; i++)
                {
                    pad[i] ^= hash[i];
                }

                OfficeCryptoPrimitives.HashSha1(pad, combined.AsSpan(0, 20));
                pad.AsSpan().Fill(0x5C);
                for (int i = 0; i < hash.Length; i++)
                {
                    pad[i] ^= hash[i];
                }

                OfficeCryptoPrimitives.HashSha1(pad, combined.AsSpan(20));
                return combined.AsSpan(0, keyBits / 8).ToArray();
            }
            finally
            {
                CryptographicOperations.ZeroMemory(pad);
                CryptographicOperations.ZeroMemory(combined);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(input);
            CryptographicOperations.ZeroMemory(hash);
        }
    }

    private void TransformPage(byte[] data, int offset, long pageNumber, int pageSize, bool encrypt)
    {
        if (pageNumber == 0)
        {
            return;
        }

        if (pageSize != 4096)
        {
            throw new InvalidDataException("Native ACE encryption requires a complete 4096-byte page.");
        }

        byte[] key = this.DeriveKey(checked((uint)pageNumber) ^ BinaryPrimitives.ReadUInt32LittleEndian(encodingKey));
        byte[] input = data.AsSpan(offset, pageSize).ToArray();
        byte[]? output = null;
        try
        {
            output = this.TransformBytes(input, key, encrypt);
            output.CopyTo(data, offset);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
            CryptographicOperations.ZeroMemory(input);
            OfficeCryptoPrimitives.ZeroIfNotNull(output);
        }
    }

    private byte[] TransformBytes(byte[] input, byte[] key, bool encrypt)
    {
        if (rc4)
        {
            byte[] result = (byte[])input.Clone();
            EncryptionManager.Rc4Transform(result, 0, result.Length, key);
            return result;
        }

        using var aes = Aes.Create();
#pragma warning disable CA5358, RS0030 // Native ECMA-376 Standard mandates AES ECB with a separately derived page key.
        aes.Mode = CipherMode.ECB;
#pragma warning restore CA5358, RS0030 // Native ECMA-376 Standard mandates AES ECB with a separately derived page key.
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        using ICryptoTransform transform = OfficeCryptoPrimitives.CreateAesTransform(aes, encrypt);
        return transform.TransformFinalBlock(input, 0, input.Length);
    }
}
