namespace JetDatabaseWriter.Tests.Infrastructure;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;

/// <summary>
/// Helpers for tests that create and reopen an in-memory ACCDB database.
/// </summary>
internal static class InMemoryAccessDatabase
{
    /// <summary>
    /// Creates a fresh ACE ACCDB database in a <see cref="MemoryStream" /> with lock files disabled.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A writable stream positioned at the start of the database.</returns>
    public static async ValueTask<MemoryStream> CreateFreshAceAccdbStreamAsync(CancellationToken cancellationToken = default)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken).ConfigureAwait(false))
        {
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Opens an in-memory database stream for writing with lock files disabled and leaves the stream open.
    /// </summary>
    /// <param name="stream">Database stream.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An open writer.</returns>
    public static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken);
    }

    /// <summary>
    /// Opens an in-memory database stream for reading with lock files disabled and leaves the stream open.
    /// </summary>
    /// <param name="stream">Database stream.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An open reader.</returns>
    public static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream, CancellationToken cancellationToken = default)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            cancellationToken);
    }
}
