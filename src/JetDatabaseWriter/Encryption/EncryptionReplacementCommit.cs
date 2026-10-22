namespace JetDatabaseWriter.Encryption;

using System;
using System.IO;
using System.Threading;

/// <summary>Orders replacement copies and a persistent process-recovery decision.</summary>
internal static class EncryptionReplacementCommit
{
    /// <summary>Identifies the immutable retained original when preparation has completed.</summary>
    internal const string OriginalFileDataKey = "JetDatabaseWriter.OriginalFile";

    /// <summary>Commits an already flushed staging file through a bounded replacement intent.</summary>
    /// <param name="temporary">The caller-owned staging file.</param>
    /// <param name="path">The original destination.</param>
    /// <param name="boundary">An optional per-operation fault boundary observer.</param>
    /// <exception cref="IOException">Preparation, publication or the decision failed.</exception>
    internal static void Commit(string temporary, string path, Action<string>? boundary = null)
    {
        EncryptionReplacementIntent? owner = null;
        try
        {
            owner = EncryptionReplacementIntent.Begin(path, boundary: boundary);
            owner.CopyReplacement(temporary);
            owner.Commit(CancellationToken.None);
        }
        catch (Exception error)
        {
            if (owner is not null)
            {
                AddRetainedPaths(error, owner);
            }

            if (owner?.IsPrepared != true)
            {
                error.Data[EncryptionManager.ReplacementFileDataKey] = temporary;
            }

            throw;
        }
        finally
        {
            owner?.Dispose();
        }

        try
        {
            File.Delete(temporary);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // The decision is complete; this caller-owned input is no longer needed.
        }
    }

    /// <summary>Reports only complete retained generation copies.</summary>
    /// <param name="error">The original error.</param>
    /// <param name="owner">The operation owning the copies.</param>
    internal static void AddRetainedPaths(Exception error, EncryptionReplacementIntent owner)
    {
        if (owner.IsPrepared)
        {
            error.Data[EncryptionManager.ReplacementFileDataKey] = owner.NewPath;
            error.Data[OriginalFileDataKey] = owner.OldPath;
        }
    }
}
