namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Stages one file-maintenance operation beside its destination.</summary>
internal static class EncryptionFileReplacement
{
    /// <summary>Writes and durably flushes an exclusive staging file before replacing the original.</summary>
    /// <param name="path">The destination file.</param>
    /// <param name="write">The staging operation; retained source handles must permit delete sharing for replacement.</param>
    /// <param name="cancellationToken">Cancellation is honored through the final flush.</param>
    /// <returns>The asynchronous replacement operation.</returns>
    internal static async ValueTask ReplaceAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        path = Path.GetFullPath(path);
        string temporary = path + ".reenc-" + Guid.NewGuid().ToString("N") + ".tmp";
        bool stagingCreated = false;
        try
        {
            FileStream staging = EncryptionMaintenanceFile.Create(temporary);
            stagingCreated = true;
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
            if (stagingCreated)
            {
                TryDelete(temporary);
            }

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
