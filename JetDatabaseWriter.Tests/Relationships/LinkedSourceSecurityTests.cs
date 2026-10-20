namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Relationships;
using Xunit;

/// <summary>Linked sources inherit capabilities without inheriting unrelated credentials.</summary>
public sealed class LinkedSourceSecurityTests
{
    [Fact]
    public void LinkedOptions_DoNotForwardHostPassword()
    {
        var options = new AccessReaderOptions("host secret")
        {
            UseLockFile = false,
            LinkedSourceResolver = static (_, _, _) => throw new UnauthorizedAccessException(),
        };
        AccessReaderOptions linked = LinkedTableManager.CreateLinkedSourceOpenOptions(options);
        Assert.True(linked.Password.IsEmpty);
        Assert.Same(options.LinkedSourceResolver, linked.LinkedSourceResolver);
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
            LinkedSourceResolver = (_, _, _) =>
            {
                calls++;
                throw new InvalidOperationException("Unsafe hints must not reach the resolver.");
            },
        };
        var policy = new LinkedSourcePolicy(options, Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Hostile", Kind = LinkedTableKind.Access, SourcePath = sourcePath, SourceObjectName = "Data" };
        _ = await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task LinkedSource_DeniedCapability_DoesNotRequestCredentials()
    {
        int passwordRequests = 0;
        var denied = new UnauthorizedAccessException("Denied capability.");
        var options = new AccessReaderOptions
        {
            LinkedSourceResolver = (_, _, _) => throw denied,
            LinkedSourcePasswordResolver = (_, _) =>
            {
                passwordRequests++;
                return "source secret".AsMemory();
            },
        };
        var policy = new LinkedSourcePolicy(options, Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Denied", Kind = LinkedTableKind.Access, SourcePath = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "denied.accdb"), SourceObjectName = "Data" };
        Assert.Same(denied, await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken)));
        Assert.Equal(0, passwordRequests);
    }

    [Theory]
    [InlineData("COM¹.accdb")]
    [InlineData("LPT².accdb")]
    [InlineData("COM³.accdb")]
    public async Task LinkedSource_SuperscriptDeviceNames_CannotOpenWithoutCapability(string fileName)
    {
        var policy = new LinkedSourcePolicy(new AccessReaderOptions(), Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "host.accdb"));
        var link = new LinkedTableInfo { Name = "Device", Kind = LinkedTableKind.Access, SourcePath = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), fileName), SourceObjectName = "Data" };
        _ = await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await LinkedTableManager.OpenLinkedSourceAsync(policy, link, TestContext.Current.CancellationToken));
    }
}
