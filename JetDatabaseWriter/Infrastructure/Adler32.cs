namespace JetDatabaseWriter.Infrastructure;

using System;

/// <summary>
/// The Adler-32 checksum that ends a zlib stream (RFC 1950 §8.2). The
/// attachment encoder frames its deflate output as zlib by hand, because the
/// netstandard2.1 build has no <c>ZLibStream</c>.
/// </summary>
internal static class Adler32
{
    private const uint Modulus = 65521;

    /// <summary>
    /// The most bytes the two sums can take before they must be reduced: the
    /// largest n with 255n(n+1)/2 + (n+1)(Modulus-1) below 2^32 (zlib's NMAX),
    /// so the unsigned additions never overflow.
    /// </summary>
    private const int MaxRun = 5552;

    /// <summary>Computes the Adler-32 checksum of <paramref name="data"/>.</summary>
    /// <param name="data">The bytes to checksum.</param>
    /// <returns>The checksum, <c>(b &lt;&lt; 16) | a</c>.</returns>
    public static uint Compute(ReadOnlySpan<byte> data)
    {
        uint a = 1;
        uint b = 0;
        while (!data.IsEmpty)
        {
            int run = Math.Min(data.Length, MaxRun);
            foreach (byte value in data[..run])
            {
                a += value;
                b += a;
            }

            a %= Modulus;
            b %= Modulus;
            data = data[run..];
        }

        return (b << 16) | a;
    }
}
