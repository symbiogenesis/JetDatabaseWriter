namespace JetDatabaseWriter.Encryption;

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;

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
            FlushDirectory(path);
            boundary?.Invoke("prepared");
            File.Replace(temporary, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
            renamed = true;
            boundary?.Invoke("renamed");
            FlushFile(path);
            FlushDirectory(path);
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
            FlushDirectory(path);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException)
        {
            // The replacement is committed; failed cleanup may leave an extra private original.
        }
    }

    private static void CopyOriginal(string path, string original)
    {
        bool backupCreated = false;
        try
        {
            using FileStream backup = EncryptionPrivateFile.Create(original);
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

    private static void FlushDirectory(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            // Windows has no supported directory fsync equivalent. File.Replace
            // retains the destination ACL; flushing it requests content persistence.
            return;
        }

        if (!RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && !RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            throw new PlatformNotSupportedException("Durable replacement requires Windows, Linux or macOS.");
        }

        string directory = Path.GetDirectoryName(Path.GetFullPath(path)) ?? throw new IOException("The destination has no parent directory.");

        // O_RDONLY | O_CLOEXEC, plus macOS O_DIRECTORY. Linux O_DIRECTORY varies by architecture.
        int flags = RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? 0x1100000 : 0x80000;
        int descriptor = NativeOpen(directory, flags);
        if (descriptor < 0)
        {
            throw NativeFailure("open directory", directory);
        }

        try
        {
            int result;
            do
            {
                result = NativeFsync(descriptor);
            }
            while (result != 0 && Marshal.GetLastWin32Error() == 4); // EINTR

            if (result != 0)
            {
                throw NativeFailure("flush directory", directory);
            }
        }
        finally
        {
            _ = NativeClose(descriptor);
        }
    }

    private static IOException NativeFailure(string operation, string path)
        => new($"Could not {operation} '{path}'.", new Win32Exception(Marshal.GetLastWin32Error()));

#pragma warning disable SYSLIB1054, CA2101 // DllImport supports netstandard2.1; Unix paths use the UTF-8 narrow-string marshaller.
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "open", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern int NativeOpen(string path, int flags);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "fsync", SetLastError = true)]
    private static extern int NativeFsync(int descriptor);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "close", SetLastError = true)]
    private static extern int NativeClose(int descriptor);
#pragma warning restore SYSLIB1054, CA2101
}
