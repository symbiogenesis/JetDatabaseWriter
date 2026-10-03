namespace JetDatabaseWriter.Encryption.Models;

using System;
using System.Security.Cryptography;
using JetDatabaseWriter.Encryption;

/// <summary>
/// Owns the page-decryption keys an open database may need.
/// Built during reader/writer construction; consulted by every page read.
/// Caches an <see cref="Aes"/> instance and a pair of <see cref="ICryptoTransform"/>
/// objects derived from the AES page key so AES-encrypted databases pay
/// the key-schedule + transform-creation cost once per file open instead of once
/// per page. ECB mode has no chaining state, so the same transforms are reused
/// across every page. Pages are decrypted after the I/O gate is released, and
/// table-scan read-ahead and concurrent reads keep several page reads in
/// flight, so <see cref="AesDecryptInPlace"/> and <see cref="AesEncryptInPlace"/>
/// build and run the shared transforms under a private lock. The Jet3 XOR and
/// Jet4 RC4 paths keep no state between pages and need no lock.
/// </summary>
internal sealed class PageDecryptionKeys : IDisposable
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
    private uint rc4DbKey;
    private byte[]? jet3XorMask;

    /// <summary>Initializes a new instance of the <see cref="PageDecryptionKeys"/> class.</summary>
    /// <param name="jet3XorMask">The Jet3 XOR mask. Copied into owned storage when present.</param>
    /// <param name="rc4DbKey">The Jet4 RC4 database key.</param>
    /// <param name="aesPageKey">The AES-128 page decryption key. Ownership is transferred to this instance.</param>
    internal PageDecryptionKeys(byte[]? jet3XorMask, uint? rc4DbKey, byte[]? aesPageKey)
    {
        if (jet3XorMask is not null)
        {
            this.jet3XorMask = (byte[])jet3XorMask.Clone();
        }

        this.rc4DbKey = rc4DbKey.GetValueOrDefault();
        this.HasRc4DbKey = rc4DbKey.HasValue;
        this.aesPageKey = aesPageKey;
    }

    /// <summary>Gets a value indicating whether Jet3 page XOR encryption is active.</summary>
    internal bool HasJet3XorMask => this.jet3XorMask is not null;

    /// <summary>Gets a value indicating whether Jet4 RC4 page encryption is active.</summary>
    internal bool HasRc4DbKey { get; private set; }

    /// <summary>Gets a value indicating whether ACCDB CFB AES page encryption is active.</summary>
    internal bool HasAesPageKey => this.aesPageKey is not null;

    /// <summary>Gets a read-only view of the Jet3 XOR mask. Call only when <see cref="HasJet3XorMask"/> is true.</summary>
    internal ReadOnlySpan<byte> Jet3XorMask => this.jet3XorMask.AsSpan();

    /// <summary>Attempts to get the active Jet4 RC4 database key.</summary>
    /// <param name="dbKey">The Jet4 RC4 database key.</param>
    /// <returns><see langword="true"/> when RC4 page encryption is active.</returns>
    internal bool TryGetRc4DbKey(out uint dbKey)
    {
        dbKey = this.rc4DbKey;
        return this.HasRc4DbKey;
    }

    /// <summary>
    /// Decrypts whole AES-ECB blocks in place with the cached decryptor,
    /// building it on first use. Safe to call from several threads at once.
    /// </summary>
    /// <param name="data">The buffer holding the blocks.</param>
    /// <param name="offset">The offset of the first block.</param>
    /// <param name="length">The byte count; a multiple of the AES block size.</param>
    /// <exception cref="CryptographicException">Thrown when the AES transform writes an unexpected byte count.</exception>
    internal void AesDecryptInPlace(byte[] data, int offset, int length) =>
        this.TransformAesInPlace(encrypt: false, data, offset, length);

    /// <summary>
    /// Encrypts whole AES-ECB blocks in place with the cached encryptor,
    /// building it on first use. Safe to call from several threads at once.
    /// </summary>
    /// <param name="data">The buffer holding the blocks.</param>
    /// <param name="offset">The offset of the first block.</param>
    /// <param name="length">The byte count; a multiple of the AES block size.</param>
    /// <exception cref="CryptographicException">Thrown when the AES transform writes an unexpected byte count.</exception>
    internal void AesEncryptInPlace(byte[] data, int offset, int length) =>
        this.TransformAesInPlace(encrypt: true, data, offset, length);

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (this.aesGate)
        {
            this.DisposeAesTransforms();
            this.DisposeAesPageKey();
        }

        this.DisposeJet3XorMask();
        this.DisposeRc4DbKey();
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

    private void DisposeJet3XorMask()
    {
        OfficeCryptoPrimitives.ZeroIfNotNull(this.jet3XorMask);
        this.jet3XorMask = null;
    }

    private void DisposeRc4DbKey()
    {
        this.rc4DbKey = 0;
        this.HasRc4DbKey = false;
    }
}
