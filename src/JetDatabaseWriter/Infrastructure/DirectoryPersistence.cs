namespace JetDatabaseWriter.Infrastructure;

using System;
using System.IO;
#if NET6_0_OR_GREATER
using System.Runtime.InteropServices;
#endif
using Microsoft.Win32.SafeHandles;

/// <summary>Fences adjacent namespace changes through the reviewed managed Windows handle path.</summary>
/// <remarks>A successful OS flush remains subject to filesystem and device qualification.</remarks>
internal static class DirectoryPersistence
{
    // SafeFileHandle.Open forwards this Windows backup-semantics option. The
    // source-audited .NET 10 path accepts writable directory handles; this is
    // deliberately narrower than a portable directory-flush API contract.
#if NET6_0_OR_GREATER
    private const FileOptions BackupSemantics = (FileOptions)0x02000000;
#endif

    /// <summary>Gets a value indicating whether this asset supplies the Windows namespace-fence adapter.</summary>
    internal static bool IsSupported =>
#if NET6_0_OR_GREATER
        OperatingSystem.IsWindows();
#else
        false;
#endif

    /// <summary>Flushes the parent directory where supported, refusing failed acknowledgements.</summary>
    /// <param name="path">A child path in the directory whose namespace changed.</param>
    /// <returns>True after an acknowledged directory flush; false on unsupported assets or platforms.</returns>
    /// <exception cref="ArgumentNullException">The child path is null.</exception>
    /// <exception cref="IOException">The directory cannot be acquired or its flush fails.</exception>
    internal static bool FlushParent(string path)
    {
        Guard.NotNull(path, nameof(path));
#if NET6_0_OR_GREATER
        if (!IsSupported)
        {
            return false;
        }

        string directory = Path.GetDirectoryName(Path.GetFullPath(path))
            ?? throw new IOException("A directory fence requires a child path with a parent directory.");
        using SafeFileHandle handle = File.OpenHandle(directory, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, BackupSemantics);
        FlushHandle(handle);
        return true;
#else
        return false;
#endif
    }

    /// <summary>Requires an actual successful flush, including errors suppressed by the BCL.</summary>
    /// <param name="handle">The directory handle retained through acknowledgement.</param>
    /// <exception cref="IOException">The flush was refused, including suppressed access denial.</exception>
    /// <exception cref="PlatformNotSupportedException">The library asset lacks the reviewed adapter.</exception>
    internal static void FlushHandle(SafeFileHandle handle)
    {
#if NET6_0_OR_GREATER
        if (!IsSupported)
        {
            throw new PlatformNotSupportedException("This platform has no reviewed directory persistence adapter.");
        }

        // FlushToDisk suppresses ERROR_ACCESS_DENIED. Capture the last-error
        // value immediately, before disposal or any other managed/native call.
        Marshal.SetLastPInvokeError(0);
        RandomAccess.FlushToDisk(handle);
        int error = Marshal.GetLastPInvokeError();
        if (error != 0)
        {
            throw new IOException("The directory persistence fence was not acknowledged.", unchecked((int)0x80070000) | (error & 0xFFFF));
        }
#else
        throw new PlatformNotSupportedException("This library asset has no reviewed directory persistence adapter.");
#endif
    }
}
