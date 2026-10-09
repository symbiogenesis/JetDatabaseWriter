namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;
using JetDatabaseWriter.Infrastructure;

internal static class OfficeCryptoPrimitives
{
    public const int Sha1HashBytes = 20;

    public const int Sha256HashBytes = 32;

    public const int Sha512HashBytes = 64;

    /// <summary>Returns the digest size of a supported Office provider hash.</summary>
    /// <param name="algorithm">The declared digest.</param>
    /// <exception cref="NotSupportedException">The digest is unsupported.</exception>
    public static int HashSize(string algorithm) => algorithm.ToUpperInvariant() switch
    {
        "SHA1" => 20,
        "SHA256" => 32,
        "SHA384" => 48,
        "SHA512" => 64,
        _ => throw new NotSupportedException($"Office encryption hash '{algorithm}' is unsupported."),
    };

    /// <summary>Hashes provider data with its declared digest.</summary>
    /// <param name="source">The digest input.</param>
    /// <param name="algorithm">The declared digest.</param>
    public static byte[] Hash(ReadOnlySpan<byte> source, string algorithm)
    {
        using var hash = IncrementalHash.CreateHash(HashName(algorithm));
        hash.AppendData(source);
        return hash.GetHashAndReset();
    }

    /// <summary>Computes the package authentication hash.</summary>
    /// <param name="key">The authentication key.</param>
    /// <param name="source">The authenticated input.</param>
    /// <param name="algorithm">The declared digest.</param>
    public static byte[] Hmac(byte[] key, byte[] source, string algorithm)
    {
        using var hash = IncrementalHash.CreateHMAC(HashName(algorithm), key);
        hash.AppendData(source);
        return hash.GetHashAndReset();
    }

    /// <summary>Resolves the platform digest name after validating support.</summary>
    /// <param name="algorithm">The declared digest.</param>
    public static HashAlgorithmName HashName(string algorithm)
    {
        _ = HashSize(algorithm);
        return new HashAlgorithmName(algorithm.ToUpperInvariant());
    }

    public static void ZeroIfNotNull(byte[]? buffer)
    {
        if (buffer is not null)
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }

    public static byte[] Sha1(ReadOnlySpan<byte> source)
    {
        byte[] hash = new byte[Sha1HashBytes];
        HashSha1(source, hash);
        return hash;
    }

    public static void HashSha1(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#pragma warning disable CA5350, RS0030 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
#if NET6_0_OR_GREATER
        bool ok = SHA1.TryHashData(source, destination, out int bytesWritten);
#else
        using var sha = SHA1.Create();
        bool ok = sha.TryComputeHash(source, destination, out int bytesWritten);
#endif
#pragma warning restore CA5350, RS0030 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
        if (!ok || bytesWritten != Sha1HashBytes)
        {
            throw new CryptographicException("SHA-1 hash computation failed.");
        }
    }

    public static byte[] Sha512(ReadOnlySpan<byte> source)
    {
        byte[] hash = new byte[Sha512HashBytes];
        HashSha512(source, hash);
        return hash;
    }

    public static void HashSha512(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        bool ok = SHA512.TryHashData(source, destination, out int bytesWritten);
#else
        using var sha = SHA512.Create();
        bool ok = sha.TryComputeHash(source, destination, out int bytesWritten);
#endif
        if (!ok || bytesWritten != Sha512HashBytes)
        {
            throw new CryptographicException("SHA-512 hash computation failed.");
        }
    }

    public static void HashSha256(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#if NET6_0_OR_GREATER
        bool ok = SHA256.TryHashData(source, destination, out int bytesWritten);
#else
        using var sha = SHA256.Create();
        bool ok = sha.TryComputeHash(source, destination, out int bytesWritten);
#endif
        if (!ok || bytesWritten != Sha256HashBytes)
        {
            throw new CryptographicException("SHA-256 hash computation failed.");
        }
    }

    public static byte[] HmacSha512(byte[] key, byte[] source)
    {
        byte[] hash = new byte[Sha512HashBytes];
#if NET6_0_OR_GREATER
        bool ok = HMACSHA512.TryHashData(key, source, hash, out int bytesWritten);
#else
        using HMACSHA512 hmac = new(key);
        bool ok = hmac.TryComputeHash(source, hash, out int bytesWritten);
#endif
        if (!ok || bytesWritten != Sha512HashBytes)
        {
            throw new CryptographicException("HMAC-SHA512 computation failed.");
        }

        return hash;
    }

    public static byte[] AesCbcNoPadding(byte[] data, byte[] key, byte[] iv, bool encrypt)
    {
        Guard.NotNull(data, nameof(data));
        Guard.NotNull(key, nameof(key));
        Guard.NotNull(iv, nameof(iv));

#pragma warning disable CA1508 // InferSharp treats Aes.Create as unknown/null-capable.
        using Aes aes = Aes.Create() ?? throw new CryptographicException("AES provider creation failed.");
#pragma warning restore CA1508 // InferSharp treats Aes.Create as unknown/null-capable.

#if NET6_0_OR_GREATER
        aes.Key = key;
        return encrypt
            ? aes.EncryptCbc(data, iv, PaddingMode.None)
            : aes.DecryptCbc(data, iv, PaddingMode.None);
#else
#pragma warning disable RS0030 // AES-CBC is required by Office Crypto Standard and Agile encryption.
        aes.Mode = CipherMode.CBC;
#pragma warning restore RS0030 // AES-CBC is required by Office Crypto Standard and Agile encryption.
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        aes.IV = iv;

        using ICryptoTransform transform = CreateAesTransform(aes, encrypt);

        byte[]? result = transform.TransformFinalBlock(data, 0, data.Length);
        return result ?? throw new CryptographicException("AES transform returned no data.");
#endif
    }

    /// <summary>Transforms native Agile AES-CFB data with the specified byte feedback.</summary>
    /// <param name="data">The whole cipher input.</param>
    /// <param name="key">The provider key.</param>
    /// <param name="iv">The provider IV.</param>
    /// <param name="encrypt">Whether to encrypt.</param>
    public static byte[] AesCfbNoPadding(byte[] data, byte[] key, byte[] iv, bool encrypt)
    {
        using var aes = Aes.Create();
#pragma warning disable CA5358 // MS-OFFCRYPTO Agile descriptors can mandate byte-feedback CFB.
        aes.Mode = CipherMode.CFB;
#pragma warning restore CA5358 // MS-OFFCRYPTO Agile descriptors can mandate byte-feedback CFB.
        aes.FeedbackSize = 8;
        aes.Padding = PaddingMode.None;
        aes.Key = key;
        aes.IV = iv;
        using ICryptoTransform transform = CreateAesTransform(aes, encrypt);
        return transform.TransformFinalBlock(data, 0, data.Length);
    }

    public static ICryptoTransform CreateAesTransform(Aes aes, bool encrypt)
    {
        ICryptoTransform? transform = encrypt
            ? aes.CreateEncryptor()
            : aes.CreateDecryptor();

        return transform ?? throw new CryptographicException("AES transform creation failed.");
    }

    public static bool FixedTimeEquals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int length)
    {
        if (left.Length < length || right.Length < length)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(left[..length], right[..length]);
    }
}
