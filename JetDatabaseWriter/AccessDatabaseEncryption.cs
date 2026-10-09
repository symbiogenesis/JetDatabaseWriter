namespace JetDatabaseWriter;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Transactions;

/// <summary>Maintains native database encryption by replacing a fully staged file.</summary>
/// <remarks>Close readers and writers before maintenance. Data is processed one page at a time;
/// the original remains unchanged until replacement. The staging file is adjacent to the original,
/// and only the current operation's staging file is cleaned up. The file is flushed before replacement;
/// persistence of the directory entry after power loss depends on the host filesystem.</remarks>
public static class AccessDatabaseEncryption
{
    /// <summary>Detects native page encryption without modifying the database.</summary>
    /// <param name="path">The database file.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The native page encryption format.</returns>
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(string path, CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(path, cancellationToken);

    /// <summary>Detects native page encryption and restores the caller's stream position.</summary>
    /// <param name="stream">The readable seekable database stream, retained by the caller.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The native page encryption format.</returns>
    public static ValueTask<AccessEncryptionFormat> DetectEncryptionFormatAsync(Stream stream, CancellationToken cancellationToken = default)
        => EncryptionManager.DetectEncryptionFormatAsync(stream, cancellationToken);

    /// <summary>Encrypts an unencrypted database using its native provider.</summary>
    /// <param name="path">The database file.</param>
    /// <param name="newPassword">The nonempty password; its backing memory must remain unchanged until completion.</param>
    /// <param name="targetFormat">The native provider, or null for the database format's default.</param>
    /// <param name="options">Locking and resource limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous maintenance operation.</returns>
    /// <exception cref="ArgumentException">The requested target is unencrypted.</exception>
    public static ValueTask EncryptAsync(string path, ReadOnlyMemory<char> newPassword, AccessEncryptionFormat? targetFormat = null, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotEmpty(newPassword, nameof(newPassword));
        if (targetFormat == AccessEncryptionFormat.None)
        {
            throw new ArgumentException("Use DecryptAsync to remove encryption.", nameof(targetFormat));
        }

        return ConvertFileAsync(path, default, newPassword, targetFormat, encryptedSource: false, options, cancellationToken);
    }

    /// <summary>Removes native page encryption and password protection.</summary>
    /// <param name="path">The database file.</param>
    /// <param name="oldPassword">The current password; its backing memory must remain unchanged until completion.</param>
    /// <param name="options">Locking and resource limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous maintenance operation.</returns>
    public static ValueTask DecryptAsync(string path, ReadOnlyMemory<char> oldPassword, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
        => ConvertFileAsync(path, oldPassword, default, AccessEncryptionFormat.None, encryptedSource: true, options, cancellationToken);

    /// <summary>Changes the password using fresh native encryption.</summary>
    /// <param name="path">The database file.</param>
    /// <param name="oldPassword">The current password; its backing memory must remain unchanged until completion.</param>
    /// <param name="newPassword">The nonempty replacement password; its backing memory must remain unchanged until completion.</param>
    /// <param name="options">Locking and resource limits.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The asynchronous maintenance operation.</returns>
    public static ValueTask ChangePasswordAsync(string path, ReadOnlyMemory<char> oldPassword, ReadOnlyMemory<char> newPassword, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        Guard.NotEmpty(newPassword, nameof(newPassword));
        return ConvertFileAsync(path, oldPassword, newPassword, null, encryptedSource: true, options, cancellationToken);
    }

    /// <summary>Encrypts the bounded bootstrap database before its first destination write.</summary>
    /// <param name="plaintext">The newly generated bootstrap database.</param>
    /// <param name="format">The database format.</param>
    /// <param name="options">The creation options, including the password.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The encrypted bootstrap database.</returns>
    internal static async ValueTask<byte[]> PrepareCreatedDatabaseAsync(byte[] plaintext, DatabaseFormat format, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        if (options.Password.IsEmpty)
        {
            return plaintext;
        }

        cancellationToken.ThrowIfCancellationRequested();
        int pageSize = format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
        byte[] sourceHeader = plaintext.AsSpan(0, pageSize).ToArray();
        byte[] targetHeader = (byte[])sourceHeader.Clone();
        try
        {
            using IPageCodec sourceCodec = new NoPageCodec();
            using IPageCodec targetCodec = PrepareHeader(targetHeader, format, EncryptionConverter.ResolveBestTargetFormat(sourceHeader), options.Password, options, cancellationToken);
            await using var source = new MemoryStream(plaintext, writable: false);
            await using var target = new MemoryStream(plaintext.Length);
            await EncryptionConverter.TransformPagesAsync(source, target, targetHeader, sourceCodec, targetCodec, cancellationToken).ConfigureAwait(false);
            var database = DatabaseFile.ForWriter(target, targetHeader, options.Password, string.Empty, leaveOpen: true, out Pager pager, options.PageCacheSize, options, cancellationToken);
            try
            {
                if (database.Format.UsesHeaderMaskedSecuritySids)
                {
                    await NativeJetSecurity.RewriteAsync(database.Format, database.TableDefs, database.OwnedPages, pager, sourceHeader, targetHeader, cancellationToken: cancellationToken, bootstrap: true).ConfigureAwait(false);
                }

                await pager.FlushAsync(toDisk: false, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await database.DisposeAsync().ConfigureAwait(false);
            }

            return target.ToArray();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(plaintext);
            CryptographicOperations.ZeroMemory(sourceHeader);
            CryptographicOperations.ZeroMemory(targetHeader);
        }
    }

    private static async ValueTask ConvertFileAsync(string path, ReadOnlyMemory<char> oldPassword, ReadOnlyMemory<char> newPassword, AccessEncryptionFormat? targetFormat, bool encryptedSource, AccessWriterOptions? options, CancellationToken cancellationToken)
    {
        Guard.NotNullOrEmpty(path, nameof(path));
        cancellationToken.ThrowIfCancellationRequested();
        Guard.RequireExistingDatabaseFile(path, nameof(path));
        options ??= new AccessWriterOptions();
        options.Validate();
        using var fileLock = LockFileCoordinator.ForReencrypt(path, options);
        fileLock.Acquire();
        FileStream source = FileStreamFactory.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, FileOptions.Asynchronous | FileOptions.SequentialScan);
        try
        {
            PersistentRollbackJournal.RejectHotJournal(source);
            async ValueTask WriteAsync(Stream destination, CancellationToken token)
            {
                byte[]? sourceHeader = null;
                byte[]? targetHeader = null;
                try
                {
                    sourceHeader = await ReadHeaderAsync(source, token).ConfigureAwait(false);
                    DatabaseFormat format = JetFormat.DetectFormat(sourceHeader);
                    AccessEncryptionFormat detected = EncryptionConverter.Detect(sourceHeader);
                    bool protectedSource = detected != AccessEncryptionFormat.None || EncryptionManager.HasHeaderPassword(sourceHeader, format);
                    if (protectedSource != encryptedSource)
                    {
                        throw new InvalidOperationException(encryptedSource ? "The source database has no encryption or password protection." : "The source database is already encrypted or password protected.");
                    }

                    AccessEncryptionFormat target = targetFormat ?? EncryptionConverter.ResolveBestTargetFormat(sourceHeader);
                    using IPageCodec sourceCodec = PageCodecFactory.Open(sourceHeader, format, oldPassword, EncryptionManager.OldPasswordArgument, options.MaxEncryptionSpinCount, options.MaxEncryptionInfoBytes, cancellationToken: token);
                    targetHeader = (byte[])sourceHeader.Clone();
                    bool passwordOnly = encryptedSource && detected == AccessEncryptionFormat.None && targetFormat is null;
                    using IPageCodec targetCodec = passwordOnly
                        ? PreparePasswordOnlyHeader(targetHeader, format, newPassword)
                        : PrepareHeader(targetHeader, format, target, newPassword, options, token);
                    await EncryptionConverter.TransformPagesAsync(source, destination, targetHeader, sourceCodec, targetCodec, token).ConfigureAwait(false);
                    var database = DatabaseFile.ForWriter(destination, targetHeader, newPassword, string.Empty, leaveOpen: true, out Pager pager, options.PageCacheSize, options, token);
                    try
                    {
                        if (database.Format.UsesHeaderMaskedSecuritySids)
                        {
                            await NativeJetSecurity.RewriteAsync(database.Format, database.TableDefs, database.OwnedPages, pager, sourceHeader, targetHeader, cancellationToken: token).ConfigureAwait(false);
                        }

                        await pager.FlushAsync(toDisk: false, token).ConfigureAwait(false);
                    }
                    finally
                    {
                        await database.DisposeAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (sourceHeader is not null)
                    {
                        CryptographicOperations.ZeroMemory(sourceHeader);
                    }

                    if (targetHeader is not null)
                    {
                        CryptographicOperations.ZeroMemory(targetHeader);
                    }
                }
            }

            await EncryptionFileReplacement.ReplaceAsync(path, WriteAsync, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await source.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static NoPageCodec PreparePasswordOnlyHeader(byte[] header, DatabaseFormat format, ReadOnlyMemory<char> password)
    {
        if (format == DatabaseFormat.Jet3Mdb)
        {
            EncryptionManager.WriteNativeJet3EncryptionHeader(header, 0, password.Span);
            return new NoPageCodec();
        }

        if (format != DatabaseFormat.Jet4Mdb)
        {
            throw new NotSupportedException("Password-only maintenance requires a native Jet database password field.");
        }

        EncryptionManager.WriteNativeJet4EncryptionHeader(header, 0, password.Span);
        return new NoPageCodec();
    }

    /// <summary>Prepares a native page-zero header and owned output codec.</summary>
    /// <param name="header">The mutable page-zero header.</param>
    /// <param name="format">The database format.</param>
    /// <param name="target">The requested encryption provider.</param>
    /// <param name="password">The output password.</param>
    /// <param name="options">The cryptographic budgets.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The owned output codec.</returns>
    /// <exception cref="ArgumentException">An encrypted target has no password.</exception>
    /// <exception cref="NotSupportedException">The native provider cannot be created for this database format.</exception>
    internal static IPageCodec PrepareHeader(byte[] header, DatabaseFormat format, AccessEncryptionFormat target, ReadOnlyMemory<char> password, AccessWriterOptions options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (target == AccessEncryptionFormat.None)
        {
            if (format == DatabaseFormat.AceAccdb)
            {
                OfficeCryptoAgile.ClearFlatEncryptionHeader(header);
            }

            EncryptionManager.TransformHeaderMask(header, format);
            header.AsSpan(Constants.DatabaseHeader.EncodingKey, sizeof(uint)).Clear();
            EncryptionManager.WriteEmptyHeaderPassword(header, format);
            EncryptionManager.TransformHeaderMask(header, format);
            return new NoPageCodec();
        }

        if (password.IsEmpty)
        {
            throw new ArgumentException("Encryption requires a nonempty password.", nameof(password));
        }

        if (target == AccessEncryptionFormat.AccdbAgile && format == DatabaseFormat.AceAccdb)
        {
            return OfficeCryptoAgile.CreateFlatEncryptionHeader(header, password.Span, options.MaxEncryptionSpinCount, options.MaxEncryptionInfoBytes, cancellationToken);
        }

        if ((target == AccessEncryptionFormat.Jet4Rc4 && format == DatabaseFormat.Jet4Mdb)
            || (target == AccessEncryptionFormat.Jet3Rc4 && format == DatabaseFormat.Jet3Mdb))
        {
            byte[] random = new byte[sizeof(uint)];
            try
            {
                using var generator = RandomNumberGenerator.Create();
                uint key;
                do
                {
                    generator.GetBytes(random);
                    key = BinaryPrimitives.ReadUInt32LittleEndian(random);
                }
                while (key == 0);

                if (format == DatabaseFormat.Jet3Mdb)
                {
                    EncryptionManager.WriteNativeJet3EncryptionHeader(header, key, password.Span);
                }
                else
                {
                    EncryptionManager.WriteNativeJet4EncryptionHeader(header, key, password.Span);
                }

                return new Jet4Rc4PageCodec(key);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(random);
            }
        }

        throw new NotSupportedException($"The native provider {target} is not supported for {format} encryption maintenance.");
    }

    private static async ValueTask<byte[]> ReadHeaderAsync(Stream source, CancellationToken cancellationToken)
    {
        byte[] prefix = new byte[0x100];
        await source.ReadExactlyAsync(prefix.AsMemory(), cancellationToken).ConfigureAwait(false);
        DatabaseFormat format = JetFormat.DetectFormat(prefix);
        int pageSize = format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;
        byte[] header = new byte[pageSize];
        source.Position = 0;
        await source.ReadExactlyAsync(header.AsMemory(), cancellationToken).ConfigureAwait(false);
        return header;
    }
}
