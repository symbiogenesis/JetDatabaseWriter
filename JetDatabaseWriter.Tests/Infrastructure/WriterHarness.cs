namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Opens a database through the library's internal write layers — the
/// <see cref="DatabaseFile"/> and a <see cref="WriterServices"/> graph built
/// over it — for tests that drive a single writer service or patch pages
/// directly. Nothing goes through <see cref="AccessWriter"/>, so the facade
/// carries no test-only members. No lock-file slot is taken, no auto-commit
/// scope wraps service calls, and Agile-encrypted containers are not unwrapped.
/// </summary>
internal sealed class WriterHarness : IAsyncDisposable
{
    private WriterHarness(DatabaseFile database, WriterServices services)
    {
        this.Database = database;
        this.Services = services;
    }

    /// <summary>Gets the open database file.</summary>
    public DatabaseFile Database { get; }

    /// <summary>Gets the writer service graph built over <see cref="Database"/>.</summary>
    public WriterServices Services { get; }

    /// <summary>Opens <paramref name="path"/> for writing.</summary>
    /// <param name="path">The database file path.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    public static async ValueTask<WriterHarness> OpenAsync(string path, AccessWriterOptions? options = null, CancellationToken cancellationToken = default)
    {
        FileStream stream = DatabaseFile.OpenFileStream(path, FileAccess.ReadWrite, FileShare.Read, FileOptions.Asynchronous | FileOptions.RandomAccess);
        try
        {
            return await OpenAsync(stream, options, leaveOpen: false, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Opens the database held in <paramref name="stream"/> for writing.</summary>
    /// <param name="stream">A readable, writable, seekable stream holding the database bytes.</param>
    /// <param name="options">Optional writer options.</param>
    /// <param name="leaveOpen">Whether <paramref name="stream"/> stays open after the harness is disposed.</param>
    /// <param name="cancellationToken">A token used to cancel the open.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "The returned harness owns the database file, and the catch disposes it when construction fails.")]
    public static async ValueTask<WriterHarness> OpenAsync(Stream stream, AccessWriterOptions? options = null, bool leaveOpen = true, CancellationToken cancellationToken = default)
    {
        options ??= new AccessWriterOptions { UseLockFile = false };
        string path = stream is FileStream fileStream ? fileStream.Name : string.Empty;
        byte[] header = await DatabaseFile.ReadHeaderAsync(stream, cancellationToken).ConfigureAwait(false);
        var database = new DatabaseFile(stream, header, options.Password, path, leaveOpen, typeof(AccessWriter), writable: true);
        try
        {
            database.ByteRangeLock = options.CreateByteRangeLock(stream);
            var services = new WriterServices(database, options, database.ByteRangeLock);
            return new WriterHarness(database, services);
        }
        catch
        {
            await database.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await this.Services.Transactions.DisposeActiveTransactionAsync().ConfigureAwait(false);
        await this.Database.DisposeAsync().ConfigureAwait(false);
    }
}
