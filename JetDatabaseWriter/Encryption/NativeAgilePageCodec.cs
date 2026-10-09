namespace JetDatabaseWriter.Encryption;

using System.Security.Cryptography;

/// <summary>Native ACE Agile page encryption using the original unlocked provider key.</summary>
/// <param name="dataKey">The owned unlocked data key.</param>
/// <param name="keyDataSalt">The owned provider salt.</param>
/// <param name="encodingKey">The owned native encoding key.</param>
/// <param name="hashAlgorithm">The provider digest algorithm.</param>
/// <param name="cipherChaining">The provider chaining mode.</param>
internal sealed class NativeAgilePageCodec(byte[] dataKey, byte[] keyDataSalt, byte[] encodingKey, string hashAlgorithm = "SHA512", string cipherChaining = "ChainingModeCBC") : IPageCodec
{
    /// <inheritdoc/>
    public bool HasEncryption => true;

    /// <inheritdoc/>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize)
        => this.Transform(data, offset, pageNumber, pageSize, encrypt: false);

    /// <inheritdoc/>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize)
        => this.Transform(data, offset, pageNumber, pageSize, encrypt: true);

    /// <inheritdoc/>
    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(dataKey);
        CryptographicOperations.ZeroMemory(keyDataSalt);
        CryptographicOperations.ZeroMemory(encodingKey);
    }

    private void Transform(byte[] data, int offset, long pageNumber, int pageSize, bool encrypt)
    {
        if (pageNumber == 0)
        {
            return;
        }

        OfficeCryptoAgile.TransformFlatPage(data, offset, checked((int)pageNumber), pageSize, dataKey, keyDataSalt, encodingKey, encrypt, hashAlgorithm, cipherChaining);
    }
}
