namespace JetDatabaseWriter.Encryption;

using System.IO;
using System.Runtime.InteropServices;
using System.Security.AccessControl;

/// <summary>Creates exclusive maintenance files using managed filesystem APIs.</summary>
/// <remarks>Unix callers must protect the containing directory; POSIX modes do not suppress inherited macOS ACLs.</remarks>
internal static class EncryptionMaintenanceFile
{
    /// <summary>Creates a new maintenance file without replacing an existing path.</summary>
    /// <param name="path">The unique maintenance file name in a protected directory.</param>
    /// <returns>The exclusive read/write stream.</returns>
    /// <exception cref="IOException">Creation failed or the path already exists.</exception>
    internal static FileStream Create(string path)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var security = new FileSecurity();
            security.SetSecurityDescriptorSddlForm("D:P(A;;FA;;;OW)", AccessControlSections.Access);
            return new FileInfo(path).Create(FileMode.CreateNew, FileSystemRights.FullControl, FileShare.None, 4096, FileOptions.None, security);
        }

#if NET7_0_OR_GREATER
        return new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.ReadWrite,
            Share = FileShare.None,
            UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite,
        });
#else
        // netstandard2.1 has no atomic Unix creation-mode API. Confidentiality
        // therefore depends on the caller protecting the containing directory.
        return new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
#endif
    }
}
