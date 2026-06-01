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
/// across every page (callers must serialize access — the existing per-reader
/// I/O gate already provides this).
/// </summary>
internal sealed class PageDecryptionKeys : IDisposable
{
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

    /// <summary>Returns the cached AES decryptor for the current AES page key, building it on first use.</summary>
    internal ICryptoTransform GetAesDecryptor()
    {
        this.EnsureAesTransforms();
        return this.aesDecryptor!;
    }

    /// <summary>Returns the cached AES encryptor for the current AES page key, building it on first use.</summary>
    internal ICryptoTransform GetAesEncryptor()
    {
        this.EnsureAesTransforms();
        return this.aesEncryptor!;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.DisposeAesTransforms();
        this.DisposeAesPageKey();
        this.DisposeJet3XorMask();
        this.DisposeRc4DbKey();
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
