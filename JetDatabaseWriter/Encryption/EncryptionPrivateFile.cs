namespace JetDatabaseWriter.Encryption;

using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

/// <summary>Creates private maintenance files without exposing payloads through inherited access rules.</summary>
internal static class EncryptionPrivateFile
{
    /// <summary>Creates a new file accessible only to its owner.</summary>
    /// <param name="path">The unique private file name.</param>
    /// <returns>The exclusive read/write stream owning its native handle.</returns>
    /// <exception cref="IOException">Private creation failed.</exception>
    /// <exception cref="PlatformNotSupportedException">The platform has no private creation implementation.</exception>
    internal static FileStream Create(string path)
    {
        SafeFileHandle handle;
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
#pragma warning disable CA2000 // Ownership transfers to FileStream below; its constructor failure disposes this handle.
            handle = CreateWindows(path);
#pragma warning restore CA2000
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            // O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC: private from creation, never inherited by child processes.
            int flags = RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? 0x1000A02 : 0x800C2;
            int descriptor = RuntimeInformation.IsOSPlatform(OSPlatform.OSX)
                ? CreateMacOS(path, flags)
                : NativeOpen(path, flags, 0x180);
            if (descriptor < 0)
            {
                throw Failure(path);
            }

#pragma warning disable CA2000 // Ownership transfers to FileStream below; its constructor failure disposes this handle.
            handle = new SafeFileHandle(new IntPtr(descriptor), ownsHandle: true);
#pragma warning restore CA2000
        }
        else
        {
            throw new PlatformNotSupportedException("Private encryption maintenance files require Windows, Linux or macOS.");
        }

        try
        {
            return new FileStream(handle, FileAccess.ReadWrite, 4096, isAsync: false);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    private static int CreateMacOS(string path, int flags)
    {
        IntPtr acl = NativeAclInit(0);
        if (acl == IntPtr.Zero)
        {
            throw Failure(path);
        }

        try
        {
            if (NativeAclGetFlags(acl, out IntPtr aclFlags) != 0 ||
                NativeAclAddFlag(aclFlags, 1 << 17) != 0 ||
                NativeAclSetFlags(acl, aclFlags) != 0)
            {
                throw Failure(path);
            }

            IntPtr security = NativeFilesecInit();
            if (security == IntPtr.Zero)
            {
                throw Failure(path);
            }

            try
            {
                ushort mode = 0x180;

                // FILESEC_MODE and FILESEC_ACL: specify both at creation, with
                // ACL_FLAG_NO_INHERIT, so a parent allow ACE cannot expose bytes.
                if (NativeFilesecSetMode(security, 4, ref mode) != 0 ||
                    NativeFilesecSetAcl(security, 5, ref acl) != 0)
                {
                    throw Failure(path);
                }

                int descriptor = NativeOpenPrivateMac(path, flags, security);
                if (descriptor < 0)
                {
                    throw Failure(path);
                }

                return descriptor;
            }
            finally
            {
                NativeFilesecFree(security);
            }
        }
        finally
        {
            _ = NativeAclFree(acl);
        }
    }

    private static SafeFileHandle CreateWindows(string path)
    {
        string fullPath = Path.GetFullPath(path);
        string nativePath = fullPath;
        if (!fullPath.StartsWith(@"\\?\", StringComparison.Ordinal))
        {
            nativePath = fullPath.StartsWith(@"\\", StringComparison.Ordinal)
                ? @"\\?\UNC\" + fullPath[2..]
                : @"\\?\" + fullPath;
        }

        // A protected DACL with OWNER RIGHTS prevents broader parent-directory
        // inheritance before the first plaintext byte reaches the file.
        if (!NativeDescriptor("D:P(A;;FA;;;OW)", 1, out IntPtr descriptor, out _))
        {
            throw Failure(path);
        }

        try
        {
            var attributes = new SecurityAttributes(Marshal.SizeOf<SecurityAttributes>(), descriptor);
            SafeFileHandle handle = NativeCreateFile(nativePath, 0xC0000000, 0, ref attributes, 1, 0x80, IntPtr.Zero);
            if (handle.IsInvalid)
            {
                IOException error = Failure(path);
                handle.Dispose();
                throw error;
            }

            return handle;
        }
        finally
        {
            _ = NativeLocalFree(descriptor);
        }
    }

    private static IOException Failure(string path)
        => new($"Could not create private maintenance file '{path}'.", new Win32Exception(Marshal.GetLastWin32Error()));

#pragma warning disable SYSLIB1054, CA2101 // DllImport supports netstandard2.1; Unix paths use the UTF-8 narrow-string marshaller.
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "open", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern int NativeOpen(string path, int flags, uint mode);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "openx_np", SetLastError = true, CharSet = CharSet.Ansi)]
    private static extern int NativeOpenPrivateMac(string path, int flags, IntPtr security);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "filesec_init", SetLastError = true)]
    private static extern IntPtr NativeFilesecInit();

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "filesec_free")]
    private static extern void NativeFilesecFree(IntPtr security);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "filesec_set_property", SetLastError = true)]
    private static extern int NativeFilesecSetMode(IntPtr security, int property, ref ushort mode);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "filesec_set_property", SetLastError = true)]
    private static extern int NativeFilesecSetAcl(IntPtr security, int property, ref IntPtr acl);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "acl_init", SetLastError = true)]
    private static extern IntPtr NativeAclInit(int count);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "acl_free")]
    private static extern int NativeAclFree(IntPtr acl);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "acl_get_flagset_np", SetLastError = true)]
    private static extern int NativeAclGetFlags(IntPtr acl, out IntPtr flags);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "acl_add_flag_np", SetLastError = true)]
    private static extern int NativeAclAddFlag(IntPtr flags, int flag);

    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    [DllImport("libc", EntryPoint = "acl_set_flagset_np", SetLastError = true)]
    private static extern int NativeAclSetFlags(IntPtr acl, IntPtr flags);

    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("advapi32.dll", EntryPoint = "ConvertStringSecurityDescriptorToSecurityDescriptorW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool NativeDescriptor(string text, uint revision, out IntPtr descriptor, out uint size);

    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("kernel32.dll", EntryPoint = "CreateFileW", SetLastError = true, CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern SafeFileHandle NativeCreateFile(string path, uint access, uint share, ref SecurityAttributes attributes, uint disposition, uint flags, IntPtr template);

    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [DllImport("kernel32.dll", EntryPoint = "LocalFree", ExactSpelling = true)]
    private static extern IntPtr NativeLocalFree(IntPtr memory);
#pragma warning restore SYSLIB1054, CA2101

    [StructLayout(LayoutKind.Sequential)]
    private readonly struct SecurityAttributes(int length, IntPtr descriptor)
    {
        private readonly int length = length;

        private readonly IntPtr descriptor = descriptor;

        private readonly int inheritHandle = 0;
    }
}
