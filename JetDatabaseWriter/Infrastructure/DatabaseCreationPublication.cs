namespace JetDatabaseWriter.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;

/// <summary>Publishes a complete newly created database without overwriting another file.</summary>
internal static class DatabaseCreationPublication
{
    /// <summary>Initializes and durably flushes a private adjacent staging file before publication.</summary>
    /// <param name="path">The full destination path.</param>
    /// <param name="initialize">Completes initialization while retaining the supplied stream.</param>
    /// <param name="observer">An optional operation-scoped publication boundary observer.</param>
    /// <param name="cancellationToken">Cancellation is honored before publication.</param>
    /// <returns>The asynchronous publication operation.</returns>
    internal static async ValueTask PublishAsync(string path, Func<FileStream, CancellationToken, ValueTask> initialize, Action<string, string>? observer, CancellationToken cancellationToken)
    {
        string temporary = path + ".create-" + Guid.NewGuid().ToString("N") + ".tmp";
        bool created = false;
        try
        {
            FileStream privateFile = EncryptionMaintenanceFile.Create(temporary);
            created = true;
            await privateFile.DisposeAsync().ConfigureAwait(false);

            // The netstandard Windows ACL creation API returns a stream named [Unknown].
            // Reopen the private file by name so locks and recovery sidecars stay operation-local.
            FileStream staging = FileStreamFactory.Open(temporary, FileMode.Open, FileAccess.ReadWrite, FileShare.None, FileOptions.Asynchronous);
            try
            {
                observer?.Invoke("Created", temporary);
                await initialize(staging, cancellationToken).ConfigureAwait(false);
                observer?.Invoke("Initialized", temporary);
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

            observer?.Invoke("Publishing", temporary);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporary, path);
            created = false;
        }
        finally
        {
            if (created)
            {
                try
                {
                    File.Delete(temporary);
                }
                catch (Exception error) when (error is IOException or UnauthorizedAccessException)
                {
                    // Only this operation's private staging path is eligible for cleanup.
                }
            }
        }
    }
}
