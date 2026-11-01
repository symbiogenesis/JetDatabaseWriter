namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Security.Cryptography;

/// <summary>Independently encodes the version-three raw image commitment for journal fixtures.</summary>
internal static class JournalFixtureHash
{
    /// <summary>Hashes image geometry and ordered raw page hashes without using production digest state.</summary>
    /// <param name="image">The complete raw database image.</param>
    /// <param name="pageSize">The database page size.</param>
    /// <returns>The terminal record's image commitment.</returns>
    internal static byte[] Compute(ReadOnlySpan<byte> image, int pageSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> geometry = stackalloc byte[16];
        geometry.Clear();
        BinaryPrimitives.WriteInt64LittleEndian(geometry, image.Length);
        BinaryPrimitives.WriteInt32LittleEndian(geometry[8..], pageSize);
        hash.AppendData(geometry);
        int offset = 0;
        while (offset < image.Length)
        {
            int count = Math.Min(pageSize, image.Length - offset);
            hash.AppendData(SHA256.HashData(image.Slice(offset, count)));
            offset += count;
        }

        return hash.GetHashAndReset();
    }
}
