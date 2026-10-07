namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using Xunit;

/// <summary>Linked sources inherit permissions without inheriting unrelated credentials.</summary>
public sealed class LinkedSourceSecurityTests
{
    [Fact]
    public void LinkedOptions_DoNotForwardHostPassword()
    {
        var options = new AccessReaderOptions("host secret") { UseLockFile = false };
        AccessReaderOptions linked = LinkedTableManager.CreateLinkedSourceOpenOptions(options, Path.Combine(Path.GetTempPath(), "host.accdb"));
        Assert.True(linked.Password.IsEmpty);
    }

    [Theory]
    [InlineData("\\\\.\\pipe\\unopened-source")]
    [InlineData("\\\\?\\C:\\unopened.accdb")]
    [InlineData("C:unopened.accdb")]
    [InlineData("//./pipe/unopened-source")]
    public async Task LinkedSource_DeviceOrDriveRelativePath_IsRefusedBeforeAuthorization(string sourcePath)
    {
        int calls = 0;
        var options = new AccessReaderOptions
        {
            LinkedSourcePathValidator = (_, _) =>
            {
                calls++;
                return true;
            },
        };
        var policy = new LinkedSourcePolicy(options, Path.Combine(Path.GetTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Hostile", Kind = LinkedTableKind.Access, SourcePath = sourcePath, SourceObjectName = "Data" };
        _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task LinkedSource_DeniedPath_DoesNotRequestCredentials()
    {
        int passwordRequests = 0;
        var options = new AccessReaderOptions
        {
            LinkedSourcePathValidator = static (_, _) => false,
            LinkedSourcePasswordResolver = (_, _) =>
            {
                passwordRequests++;
                return "source secret".AsMemory();
            },
        };
        var policy = new LinkedSourcePolicy(options, Path.Combine(Path.GetTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Denied", Kind = LinkedTableKind.Access, SourcePath = Path.Combine(Path.GetTempPath(), "denied.accdb"), SourceObjectName = "Data" };
        _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
        Assert.Equal(0, passwordRequests);
    }

    [Theory]
    [InlineData("COM¹.accdb")]
    [InlineData("LPT².accdb")]
    [InlineData("COM³.accdb")]
    public async Task LinkedSource_SuperscriptDeviceNames_RespectWindowsNamespaces(string fileName)
    {
        var policy = new LinkedSourcePolicy(new AccessReaderOptions(), Path.Combine(Path.GetTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Device", Kind = LinkedTableKind.Access, SourcePath = Path.Combine(Path.GetTempPath(), fileName), SourceObjectName = "Data" };
        Type expected = Path.DirectorySeparatorChar == '\\' ? typeof(UnauthorizedAccessException) : typeof(FileNotFoundException);
        _ = await Assert.ThrowsAsync(expected, async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task LinkedSource_AllowlistNestedUnderSymlink_IsRefused()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"linked-security-{Guid.NewGuid():N}");
        string target = Path.Combine(directory, "target");
        string alias = Path.Combine(directory, "alias");
        string nested = Path.Combine(target, "nested");
        Directory.CreateDirectory(nested);
        try
        {
            Directory.CreateSymbolicLink(alias, target);
            string sourcePath = Path.Combine(alias, "nested", "source.accdb");
            await File.WriteAllTextAsync(Path.Combine(nested, "source.accdb"), "must not be read", TestContext.Current.CancellationToken);
            var options = new AccessReaderOptions { UseLockFile = false, LinkedSourcePathAllowlist = [Path.Combine(alias, "nested")] };
            var policy = new LinkedSourcePolicy(options, Path.Combine(directory, "host.accdb"));
            var link = new LinkedTableInfo { Name = "Alias", Kind = LinkedTableKind.Access, SourcePath = sourcePath, SourceObjectName = "Data" };
            _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
        }
        finally
        {
            if (Directory.Exists(alias))
            {
                Directory.Delete(alias);
            }

            Directory.Delete(directory, recursive: true);
        }
    }
}
