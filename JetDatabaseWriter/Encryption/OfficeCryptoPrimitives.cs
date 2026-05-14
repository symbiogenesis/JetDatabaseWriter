namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;

#pragma warning disable CA5350 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.

internal static class OfficeCryptoPrimitives
{
    public const int Sha1HashBytes = 20;

    public const int Sha256HashBytes = 32;

    public const int Sha512HashBytes = 64;

    public static byte[] Sha1(ReadOnlySpan<byte> source)
    {
        byte[] hash = new byte[Sha1HashBytes];
        HashSha1(source, hash);
        return hash;
    }

    public static void HashSha1(ReadOnlySpan<byte> source, Span<byte> destination)
    {
#pragma warning disable SCS0006 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
#if NET6_0_OR_GREATER
        bool ok = SHA1.TryHashData(source, destination, out int bytesWritten);
#else
        using SHA1 sha = SHA1.Create();
#pragma warning restore SCS0006 // SHA-1 is mandated by the MS-OFFCRYPTO Standard encryption spec.
        bool ok = sha.TryComputeHash(source, destination, out int bytesWritten);
#endif
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
        using SHA512 sha = SHA512.Create();
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
        using SHA256 sha = SHA256.Create();
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

    public static bool FixedTimeEquals(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right, int length)
    {
        if (left.Length < length || right.Length < length)
        {
            return false;
        }

        return CryptographicOperations.FixedTimeEquals(left.Slice(0, length), right.Slice(0, length));
    }
}
