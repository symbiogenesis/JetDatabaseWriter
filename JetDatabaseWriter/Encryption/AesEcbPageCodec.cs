namespace JetDatabaseWriter.Encryption;

using System;
using System.Security.Cryptography;

/// <summary>Thread-safe cached AES-ECB page transforms with owned key material.</summary>
internal sealed class AesEcbPageCodec : IPageCodec
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock aesGate = new();
#else
    private readonly object aesGate = new();
#endif
    private Aes? aes;
    private ICryptoTransform? aesEncryptor;
    private ICryptoTransform? aesDecryptor;
    private byte[]? aesPageKey;

    /// <summary>Initializes a new instance of the <see cref="AesEcbPageCodec"/> class.</summary>
    /// <param name="key">The owned AES page key.</param>
    internal AesEcbPageCodec(byte[] key) => this.aesPageKey = key;

    /// <inheritdoc/>
    public bool HasEncryption => true;

    /// <inheritdoc/>
    public void Decode(byte[] data, int offset, long pageNumber, int pageSize)
    {
        if (pageNumber > 0)
        {
            this.TransformAesInPlace(false, data, offset, pageSize);
        }
    }

    /// <inheritdoc/>
    public void Encode(byte[] data, int offset, long pageNumber, int pageSize)
    {
        if (pageNumber > 0)
        {
            this.TransformAesInPlace(true, data, offset, pageSize);
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (this.aesGate)
        {
            this.DisposeAesTransforms();
            this.DisposeAesPageKey();
        }
    }

    /// <summary>
    /// Runs the cached ECB transform over a buffer in place. ECB has no
    /// chaining state between 16-byte blocks, so passing the same array as
    /// input and output is safe, but an <see cref="ICryptoTransform"/> instance
    /// is not safe to use from two threads at once, and neither is the lazy
    /// build, so both run under <see cref="aesGate"/>.
    /// </summary>
    /// <param name="encrypt"><see langword="true"/> to encrypt; <see langword="false"/> to decrypt.</param>
    /// <param name="data">The buffer holding the blocks.</param>
    /// <param name="offset">The offset of the first block.</param>
    /// <param name="length">The byte count; a multiple of the AES block size.</param>
    /// <exception cref="CryptographicException">Thrown when the AES transform writes an unexpected byte count.</exception>
    private void TransformAesInPlace(bool encrypt, byte[] data, int offset, int length)
    {
        lock (this.aesGate)
        {
            this.EnsureAesTransforms();
            ICryptoTransform transform = encrypt ? this.aesEncryptor! : this.aesDecryptor!;
            int written = transform.TransformBlock(data, offset, length, data, offset);
            if (written != length)
            {
                throw new CryptographicException(
                    $"AES-ECB TransformBlock processed {written} bytes but {length} were expected.");
            }
        }
    }

    private void EnsureAesTransforms()
    {
        if (this.aes != null)
        {
            return;
        }

        byte[] key = this.aesPageKey ??
            throw new InvalidOperationException("AesPageKey must be set before requesting AES transforms.");

#pragma warning disable CA5358, RS0030 // ECB mode is required to match the ACCDB AES page encryption scheme
        this.aes = Aes.Create();
        this.aes.Key = key;
        this.aes.Mode = CipherMode.ECB;
        this.aes.Padding = PaddingMode.None;
#pragma warning restore CA5358, RS0030 // ECB mode is required to match the ACCDB AES page encryption scheme

        this.aesEncryptor = this.aes.CreateEncryptor();
        this.aesDecryptor = this.aes.CreateDecryptor();
    }

    private void DisposeAesTransforms()
    {
        this.aesEncryptor?.Dispose();
        this.aesDecryptor?.Dispose();
        this.aes?.Dispose();
        this.aesEncryptor = null;
        this.aesDecryptor = null;
        this.aes = null;
    }

    private void DisposeAesPageKey()
    {
        OfficeCryptoPrimitives.ZeroIfNotNull(this.aesPageKey);
        this.aesPageKey = null;
    }
}
