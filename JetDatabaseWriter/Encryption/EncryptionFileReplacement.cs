namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>Stages one file-maintenance operation beside its destination.</summary>
internal static class EncryptionFileReplacement
{
    /// <summary>Writes and durably flushes a private staging file before replacing the original.</summary>
    /// <param name="path">The destination file.</param>
    /// <param name="write">The staging operation; retained source handles must permit delete sharing for replacement.</param>
    /// <param name="cancellationToken">Cancellation is honored through the final flush.</param>
    /// <returns>The asynchronous replacement operation.</returns>
    internal static async ValueTask ReplaceAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        string temporary = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        try
        {
            FileStream staging = FileStreamFactory.Open(temporary, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, FileOptions.Asynchronous);
            try
            {
                await write(staging, cancellationToken).ConfigureAwait(false);
                await staging.FlushAsync(cancellationToken).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();
#pragma warning disable CA1849 // FileStream has no asynchronous Flush(flushToDisk: true).
                staging.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }
            finally
            {
                await staging.DisposeAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            TryDelete(temporary);
            throw;
        }

        EncryptionManager.ReplaceFileWithTemp(temporary, path);
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // Only this operation's staging file is eligible for cleanup.
        }
    }
}
