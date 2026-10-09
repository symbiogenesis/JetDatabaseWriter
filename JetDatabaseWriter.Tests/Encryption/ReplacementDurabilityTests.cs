namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Security.AccessControl;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Exercises recoverable copies at namespace commit boundaries.</summary>
public sealed class ReplacementDurabilityTests
{
    /// <summary>New maintenance files do not inherit a broadly readable Windows directory ACL.</summary>
    [Fact]
    public void MaintenanceFile_OnWindows_HasProtectedOwnerOnlyAcl()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementDurabilityTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "private.tmp");
        try
        {
            using (FileStream staging = EncryptionMaintenanceFile.Create(path))
            {
                staging.WriteByte(7);
            }

            FileSecurity security = new FileInfo(path).GetAccessControl();
            Assert.True(security.AreAccessRulesProtected);
            AuthorizationRuleCollection rules = security.GetAccessRules(includeExplicit: true, includeInherited: true, typeof(System.Security.Principal.SecurityIdentifier));
            FileSystemAccessRule rule = Assert.IsType<FileSystemAccessRule>(Assert.Single(rules));
            Assert.False(rule.IsInherited);
            Assert.Equal("S-1-3-4", rule.IdentityReference.Value);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Modern Unix staging files request owner-only POSIX permissions.</summary>
    [Fact]
    public void MaintenanceFile_OnUnix_HasOwnerOnlyMode()
    {
        if (OperatingSystem.IsWindows() || LibraryTarget.IsNetStandard)
        {
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementDurabilityTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "private.tmp");
        try
        {
            using (FileStream staging = EncryptionMaintenanceFile.Create(path))
            {
                staging.WriteByte(7);
            }

            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A maintenance file name collision preserves the existing bytes.</summary>
    [Fact]
    public async Task MaintenanceFile_ExistingPath_PreservesContents()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementDurabilityTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "existing.tmp");
        byte[] existing = [1, 2, 3];
        try
        {
            await File.WriteAllBytesAsync(path, existing, TestContext.Current.CancellationToken);
            Assert.Throws<IOException>(() => EncryptionMaintenanceFile.Create(path));
            Assert.Equal(existing, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>A refused backup removes only an incomplete file this operation created.</summary>
    /// <param name="existingBackup">Whether another operation already owns the backup name.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Commit_CopyRefused_CleansOnlyOwnedIncompleteBackup(bool existingBackup)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementDurabilityTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "missing.accdb");
        string temporary = path + ".reenc-owned.tmp";
        string backup = temporary + ".original";
        byte[] replacement = [4, 5, 6];
        byte[] unrelated = [7, 8, 9];
        try
        {
            await File.WriteAllBytesAsync(temporary, replacement, TestContext.Current.CancellationToken);
            if (existingBackup)
            {
                await File.WriteAllBytesAsync(backup, unrelated, TestContext.Current.CancellationToken);
            }

            IOException error = Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(temporary, path));
            Assert.Equal(temporary, error.Data[EncryptionManager.ReplacementFileDataKey]);
            Assert.Null(error.Data[EncryptionReplacementCommit.OriginalFileDataKey]);
            Assert.Equal(replacement, await File.ReadAllBytesAsync(temporary, TestContext.Current.CancellationToken));
            if (existingBackup)
            {
                Assert.Equal(unrelated, await File.ReadAllBytesAsync(backup, TestContext.Current.CancellationToken));
            }
            else
            {
                Assert.False(File.Exists(backup));
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Each failed commit retains the original and recoverable replacement contents.</summary>
    /// <param name="boundary">The commit boundary to interrupt.</param>
    [Theory]
    [InlineData("prepared")]
    [InlineData("renamed")]
    [InlineData("committed")]
    public async Task Commit_FailedBoundary_RetainsOriginalAndReplacement(string boundary)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementDurabilityTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.accdb");
        string temporary = path + ".reenc-owned.tmp";
        byte[] oldBytes = [1, 2, 3];
        byte[] newBytes = [4, 5, 6, 7];
        try
        {
            await File.WriteAllBytesAsync(path, oldBytes, TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(temporary, newBytes, TestContext.Current.CancellationToken);
            await using (var staged = new FileStream(temporary, FileMode.Open, FileAccess.ReadWrite))
            {
#pragma warning disable CA1849 // No asynchronous Flush(flushToDisk: true) API exists.
                staged.Flush(flushToDisk: true);
#pragma warning restore CA1849
            }

            IOException error = Assert.Throws<IOException>(() => EncryptionReplacementCommit.Commit(temporary, path, observed =>
            {
                if (observed == boundary)
                {
                    throw new IOException("Injected namespace commit interruption.");
                }
            }));
            string retainedOriginal = Assert.IsType<string>(error.Data[EncryptionReplacementCommit.OriginalFileDataKey]);
            Assert.Equal(oldBytes, await File.ReadAllBytesAsync(retainedOriginal, TestContext.Current.CancellationToken));
            if (OperatingSystem.IsWindows())
            {
                Assert.True(new FileInfo(retainedOriginal).GetAccessControl().AreAccessRulesProtected);
            }
            else if (!LibraryTarget.IsNetStandard)
            {
                Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(retainedOriginal));
            }

            if (boundary == "prepared")
            {
                Assert.Equal(oldBytes, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
                Assert.Equal(temporary, error.Data[EncryptionManager.ReplacementFileDataKey]);
                Assert.Equal(newBytes, await File.ReadAllBytesAsync(temporary, TestContext.Current.CancellationToken));
            }
            else
            {
                Assert.False(File.Exists(temporary));
                Assert.Equal(newBytes, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
