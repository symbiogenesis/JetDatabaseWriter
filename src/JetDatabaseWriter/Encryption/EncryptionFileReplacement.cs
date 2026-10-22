namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Stages one file-maintenance operation beside its destination.</summary>
internal static class EncryptionFileReplacement
{
    /// <summary>Flushes bounded immutable copies before publishing a persistent process-recovery decision.</summary>
    /// <param name="path">The destination file.</param>
    /// <param name="write">The staging operation.</param>
    /// <param name="cancellationToken">Cancellation is honored before preparation is published.</param>
    /// <param name="source">An optional exclusive source handle whose ownership transfers to this operation.</param>
    /// <returns>The asynchronous replacement operation.</returns>
    internal static async ValueTask ReplaceAsync(string path, Func<Stream, CancellationToken, ValueTask> write, CancellationToken cancellationToken, FileStream? source = null)
    {
        using var owner = EncryptionReplacementIntent.Begin(path, source, cancellationToken: cancellationToken);
        try
        {
            FileStream staging = owner.CreateReplacement();
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

            owner.Commit(cancellationToken);
        }
        catch (Exception error)
        {
            EncryptionReplacementCommit.AddRetainedPaths(error, owner);
            throw;
        }
    }
}
