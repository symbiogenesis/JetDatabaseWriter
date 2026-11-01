namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Verification;

/// <summary>Retains raw page digests only while one store owns every physical write.</summary>
/// <remarks>SHA-256 and successful exact I/O remain effect contracts; no plaintext page images are retained.</remarks>
internal sealed class FilePageDigests
{
    private byte[] digests;

    private FilePageDigests(int pageSize, long length, byte[] digests)
    {
        this.PageSize = pageSize;
        this.Length = length;
        this.digests = digests;
    }

    /// <summary>Gets the byte length represented by the digest ledger.</summary>
    internal long Length { get; private set; }

    /// <summary>Gets the physical page size bound into this ledger's image identity.</summary>
    internal int PageSize { get; }

    /// <summary>Gets a value indicating whether every successful physical effect is represented.</summary>
    internal bool IsValid { get; private set; } = true;

    /// <summary>Gets the complete ordered digest manifest for the current image.</summary>
    internal ReadOnlySpan<byte> Manifest => this.digests.AsSpan(0, checked((int)PageDigestKernel.DigestBytes((ulong)this.Length, (uint)this.PageSize)));

    /// <summary>Acquires an initial ledger using one bounded raw-page buffer.</summary>
    /// <param name="database">The exclusively owned database stream.</param>
    /// <param name="pageSize">The validated raw page size.</param>
    /// <returns>The exact digest ledger for the acquired image.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The image exceeds the supported digest-ledger bounds.</exception>
    internal static FilePageDigests Load(FileStream database, int pageSize)
    {
        long length = database.Length;
        if (length < 0 || pageSize is not (2048 or 4096) || !PageDigestKernel.ValidShape((ulong)length, (uint)pageSize))
        {
            throw new ArgumentOutOfRangeException(nameof(database), "The raw image exceeds the bounded digest ledger.");
        }

        uint bytes = PageDigestKernel.DigestBytes((ulong)length, (uint)pageSize);
        byte[] digests = new byte[bytes];
        byte[] page = new byte[pageSize];
        int cursor = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        for (long offset = 0; offset < length; offset += pageSize)
        {
            int count = checked((int)Math.Min(pageSize, length - offset));
            database.Position = offset;
            StreamRead.ReadExactly(database, page.AsSpan(0, count));
            hash.AppendData(page, 0, count);
            hash.GetHashAndReset().CopyTo(digests, cursor);
            cursor += 32;
        }

        return new FilePageDigests(pageSize, length, digests);
    }

    /// <summary>Prefixes image length and page size so different image shapes cannot share a framing.</summary>
    /// <param name="hash">The commitment accumulator.</param>
    /// <param name="length">The exact raw image length.</param>
    /// <param name="pageSize">The physical page size.</param>
    internal static void AppendIdentity(IncrementalHash hash, long length, int pageSize)
    {
        byte[] identity = new byte[16];
        BinaryPrimitives.WriteInt64LittleEndian(identity, length);
        BinaryPrimitives.WriteInt32LittleEndian(identity.AsSpan(8), pageSize);
        hash.AppendData(identity);
    }

    private static byte[] Hash(ReadOnlySpan<byte> bytes)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(bytes);
        return hash.GetHashAndReset();
    }

    /// <summary>Invalidates reuse after an uncertain or untracked physical effect.</summary>
    internal void Invalidate() => this.IsValid = false;

    /// <summary>Tracks a successful aligned overwrite or contiguous complete-page append.</summary>
    /// <param name="oldLength">The physical length before the successful write.</param>
    /// <param name="offset">The page-aligned byte offset.</param>
    /// <param name="page">The complete raw page that was written.</param>
    internal void Written(long oldLength, long offset, ReadOnlySpan<byte> page)
    {
        if (!this.IsValid || oldLength != this.Length || oldLength < 0 || offset < 0 ||
            !PageDigestKernel.CanTrackWrite((ulong)oldLength, (ulong)offset, (uint)page.Length, (uint)this.PageSize))
        {
            this.Invalidate();
            return;
        }

        long length = Math.Max(oldLength, offset + page.Length);
        this.EnsureCapacity(length);
        this.Replace(offset, Hash(page));
        this.Length = length;
    }

    /// <summary>Tracks an aligned truncation; extension bytes require independent acquisition.</summary>
    /// <param name="oldLength">The physical length before the successful resize.</param>
    /// <param name="length">The resulting physical length.</param>
    internal void Resized(long oldLength, long length)
    {
        if (!this.IsValid || oldLength != this.Length || length < 0 || oldLength < 0 ||
            !PageDigestKernel.CanTrackResize((ulong)oldLength, (ulong)length, (uint)this.PageSize))
        {
            this.Invalidate();
            return;
        }

        this.Length = length;
    }

    /// <summary>Computes the length-bound ordered page-digest commitment without rereading payloads.</summary>
    /// <returns>The complete image commitment.</returns>
    /// <exception cref="InvalidOperationException">An uncertain effect invalidated the ledger.</exception>
    internal byte[] DatabaseHash()
    {
        if (!this.IsValid)
        {
            throw new InvalidOperationException("An invalidated raw digest ledger cannot authorize a commit.");
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendIdentity(hash, this.Length, this.PageSize);
        hash.AppendData(this.Manifest);
        return hash.GetHashAndReset();
    }

    private void EnsureCapacity(long length)
    {
        int count = checked((int)PageDigestKernel.DigestBytes((ulong)length, (uint)this.PageSize));
        if (count > this.digests.Length)
        {
            int capacity = Math.Max(count, Math.Min(33_554_432, Math.Max(32, this.digests.Length) * 2));
            Array.Resize(ref this.digests, capacity);
        }
    }

    private void Replace(long offset, byte[] digest)
    {
        if (!PageDigestKernel.ReplaceDigest(this.digests, checked((uint)(offset / this.PageSize)), digest))
        {
            this.Invalidate();
            throw new InvalidOperationException("The raw digest ledger rejected an admitted page position.");
        }
    }
}
