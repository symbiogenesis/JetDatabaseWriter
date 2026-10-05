namespace JetDatabaseWriter.Encryption;

/// <summary>The plaintext page codec.</summary>
internal sealed class NoPageCodec : IPageCodec
{
    /// <inheritdoc/>
    public bool HasEncryption => false;

    /// <inheritdoc/>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize) { }

    /// <inheritdoc/>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize) { }

    /// <inheritdoc/>
    public void Dispose() { }
}