namespace JetDatabaseWriter.Encryption;

using System.Security.Cryptography;

/// <summary>Jet3's cyclic XOR page mask.</summary>
internal sealed class Jet3XorPageCodec : IPageCodec
{
    private readonly byte[] mask;

    /// <summary>Initializes a new instance of the <see cref="Jet3XorPageCodec"/> class.</summary>
    /// <param name="mask">The mask, copied into owned storage.</param>
    internal Jet3XorPageCodec(byte[] mask) => this.mask = (byte[])mask.Clone();

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

        long fileOffset = pageNumber * pageSize;
        for (int i = 0; i < pageSize; i++)
        {
            data[offset + i] ^= this.mask[(int)((fileOffset + i - pageSize) % this.mask.Length)];
        }
    }

    /// <inheritdoc/>
    public void Dispose() => CryptographicOperations.ZeroMemory(this.mask);
}
