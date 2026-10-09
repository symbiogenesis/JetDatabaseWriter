namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;

/// <summary>Orders replacement contents, recoverable copies and namespace commits.</summary>
internal static class EncryptionReplacementCommit
{
    /// <summary>Identifies the retained original if a namespace commit fails.</summary>
    internal const string OriginalFileDataKey = "JetDatabaseWriter.OriginalFile";

    /// <summary>Identifies an incomplete owned backup whose cleanup was refused.</summary>
    internal const string IncompleteOriginalFileDataKey = "JetDatabaseWriter.IncompleteOriginalFile";

    /// <summary>Commits an already flushed adjacent staging file.</summary>
    /// <param name="temporary">The owned staging file.</param>
    /// <param name="path">The original destination.</param>
    /// <param name="boundary">An optional per-operation fault boundary observer.</param>
    /// <exception cref="IOException">The rename or flush failed; retained paths are supplied in the exception data.</exception>
    internal static void Commit(string temporary, string path, Action<string>? boundary = null)
    {
        string original = temporary + ".original";
        bool renamed = false;
        bool originalReady = false;
        try
        {
            CopyOriginal(path, original);
            originalReady = true;
            boundary?.Invoke("prepared");
            File.Replace(temporary, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
            renamed = true;
            boundary?.Invoke("renamed");
            FlushFile(path);
            boundary?.Invoke("committed");
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or PlatformNotSupportedException)
        {
            var failure = new IOException(
                renamed
                    ? $"Replacement of '{path}' was renamed, but its durability commit failed. The original is retained in '{original}'."
                    : $"Could not commit replacement of '{path}'. Retained copies are named in the exception data; check the destination before retrying.",
                error);
            if (!renamed && File.Exists(temporary))
            {
                failure.Data[EncryptionManager.ReplacementFileDataKey] = temporary;
            }

            if (originalReady && File.Exists(original))
            {
                failure.Data[OriginalFileDataKey] = original;
            }

            if (error.Data[IncompleteOriginalFileDataKey] is string incomplete)
            {
                failure.Data[IncompleteOriginalFileDataKey] = incomplete;
            }

            throw failure;
        }

        // Cleanup is after the commit decision. A cleanup failure cannot undo it.
        try
        {
            File.Delete(original);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // The replacement is committed; failed cleanup may leave an extra original backup.
        }
    }

    private static void CopyOriginal(string path, string original)
    {
        bool backupCreated = false;
        try
        {
            using FileStream backup = EncryptionMaintenanceFile.Create(original);
            backupCreated = true;
            using var source = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            source.CopyTo(backup);
            backup.Flush(flushToDisk: true);
        }
        catch (Exception error)
        {
            if (backupCreated)
            {
                try
                {
                    File.Delete(original);
                }
                catch (Exception cleanupError) when (cleanupError is IOException or UnauthorizedAccessException)
                {
                    error.Data[IncompleteOriginalFileDataKey] = original;
                }
            }

            throw;
        }
    }

    private static void FlushFile(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        stream.Flush(flushToDisk: true);
    }
}
