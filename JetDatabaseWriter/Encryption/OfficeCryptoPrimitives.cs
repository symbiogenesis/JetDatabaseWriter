namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;
using JetDatabaseWriter.Infrastructure;

#pragma warning disable CA5350 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
#pragma warning disable CA5358, CA5401 // AES-CBC mode and IV handling are mandated by Office Crypto specs.

internal static class OfficeCryptoPrimitives
{
    public const int Sha1HashBytes = 20;

    public const int Sha256HashBytes = 32;

    public const int Sha512HashBytes = 64;

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
#pragma warning disable RS0030 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
#if NET6_0_OR_GREATER
        bool ok = SHA1.TryHashData(source, destination, out int bytesWritten);
#else
        using var sha = SHA1.Create();
        bool ok = sha.TryComputeHash(source, destination, out int bytesWritten);
#endif
#pragma warning restore RS0030 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
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

        var aes = Aes.Create();
#pragma warning disable CA1508 // InferSharp treats Aes.Create as unknown/null-capable.
        if (aes is null)
        {
            throw new CryptographicException("AES provider creation failed.");
        }
#pragma warning restore CA1508

        using (aes)
        {
#if NET6_0_OR_GREATER
            aes.Key = key;
            return encrypt
                ? aes.EncryptCbc(data, iv, PaddingMode.None)
                : aes.DecryptCbc(data, iv, PaddingMode.None);
#else
#pragma warning disable RS0030 // AES-CBC is required by Office Crypto Standard and Agile encryption.
            aes.Mode = CipherMode.CBC;
#pragma warning restore RS0030
            aes.Padding = PaddingMode.None;
            aes.Key = key;
            aes.IV = iv;

            using ICryptoTransform transform = CreateAesTransform(aes, encrypt);

            byte[]? result = transform.TransformFinalBlock(data, 0, data.Length);
            return result ?? throw new CryptographicException("AES transform returned no data.");
#endif
        }
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
