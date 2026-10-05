namespace JetDatabaseWriter.Encryption;

using System;

/// <summary>Thread-safe page encryption with page-zero bypass.</summary>
internal interface IPageCodec : IDisposable
{
    /// <summary>Gets whether pages are encrypted.</summary>
    public bool HasEncryption { get; }

    /// <summary>Decodes one page in an owned buffer.</summary>
    /// <param name="data">The buffer.</param>
    /// <param name="offset">The start offset.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize);

    /// <summary>Encodes one page in a scratch buffer.</summary>
    /// <param name="data">The buffer.</param>
    /// <param name="offset">The start offset.</param>
    /// <param name="pageNumber">The page number.</param>
    /// <param name="pageSize">The page size.</param>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize);
}