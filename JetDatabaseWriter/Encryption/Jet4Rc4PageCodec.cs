namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;

/// <summary>Native Jet RC4 with the database encoding key XOR the page number.</summary>
internal sealed class Jet4Rc4PageCodec : IPageCodec
{
    private uint databaseKey;

    /// <summary>Initializes a new instance of the <see cref="Jet4Rc4PageCodec"/> class.</summary>
    /// <param name="databaseKey">The database key.</param>
    internal Jet4Rc4PageCodec(uint databaseKey) => this.databaseKey = databaseKey;

    /// <inheritdoc/>
    public bool HasEncryption => true;

    /// <inheritdoc/>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize) => this.Encode(data, offset, pageNumber, pageSize);

    /// <inheritdoc/>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize)
    {
        if (pageNumber < 1)
        {
            return;
        }

        Span<byte> key = stackalloc byte[4];
        try
        {
            EncryptionManager.DeriveRc4PageKey(this.databaseKey, checked((uint)pageNumber), key);
            EncryptionManager.Rc4Transform(data, offset, pageSize, key);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    /// <inheritdoc/>
    public void Dispose() => this.databaseKey = 0;
}
