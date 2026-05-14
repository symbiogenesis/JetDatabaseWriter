namespace JetDatabaseWriter.Encryption.Models;

using System;
using System.Security.Cryptography;

/// <summary>
/// Mutable holder for the three page-decryption keys an open database may need.
/// Populated during reader construction; consulted by every page read.
/// Caches an <see cref="Aes"/> instance and a pair of <see cref="ICryptoTransform"/>
/// objects derived from <see cref="AesPageKey"/> so AES-encrypted databases pay
/// the key-schedule + transform-creation cost once per file open instead of once
/// per page. ECB mode has no chaining state, so the same transforms are reused
/// across every page (callers must serialize access — the existing per-reader
/// I/O gate already provides this).
/// </summary>
public sealed class PageDecryptionKeys : IDisposable
{
    private byte[]? _aesPageKey;
    private Aes? _aes;
    private ICryptoTransform? _aesEncryptor;
    private ICryptoTransform? _aesDecryptor;

    /// <summary>Gets or sets the Jet3 XOR mask, non-null when Jet3 page encryption is active.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Mutable key holder by design.")]
    public byte[]? Jet3XorMask { get; set; }

    /// <summary>Gets or sets the Jet4 RC4 database key (header offset 0x3E), non-null when RC4 page encryption is active.</summary>
    public uint? Rc4DbKey { get; set; }

    /// <summary>Gets or sets the AES-128 page decryption key, non-null when ACCDB CFB AES page encryption is active.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1819:Properties should not return arrays", Justification = "Mutable key holder by design.")]
    public byte[]? AesPageKey
    {
        get => _aesPageKey;
        set
        {
            DisposeAesTransforms();
            _aesPageKey = value;
        }
    }

    /// <summary>Returns the cached AES decryptor for the current <see cref="AesPageKey"/>, building it on first use.</summary>
    internal ICryptoTransform GetAesDecryptor()
    {
        EnsureAesTransforms();
        return _aesDecryptor!;
    }

    /// <summary>Returns the cached AES encryptor for the current <see cref="AesPageKey"/>, building it on first use.</summary>
    internal ICryptoTransform GetAesEncryptor()
    {
        EnsureAesTransforms();
        return _aesEncryptor!;
    }

    /// <inheritdoc/>
    public void Dispose() => DisposeAesTransforms();

    private void EnsureAesTransforms()
    {
        if (_aes != null)
        {
            return;
        }

        if (_aesPageKey == null)
        {
            throw new InvalidOperationException("AesPageKey must be set before requesting AES transforms.");
        }

#pragma warning disable SCS0013, CA5358 // ECB mode is required to match the ACCDB AES page encryption scheme
        _aes = Aes.Create();
        _aes.Key = _aesPageKey;
        _aes.Mode = CipherMode.ECB;
        _aes.Padding = PaddingMode.None;
#pragma warning restore SCS0013, CA5358 // ECB mode is required to match the ACCDB AES page encryption scheme

        _aesEncryptor = _aes.CreateEncryptor();
        _aesDecryptor = _aes.CreateDecryptor();
    }

    private void DisposeAesTransforms()
    {
        _aesEncryptor?.Dispose();
        _aesDecryptor?.Dispose();
        _aes?.Dispose();
        _aesEncryptor = null;
        _aesDecryptor = null;
        _aes = null;
    }
}
