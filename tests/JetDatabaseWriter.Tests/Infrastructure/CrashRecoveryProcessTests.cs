namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.IO;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks that crash child diagnostics remain useful without disclosing secrets.</summary>
public sealed class CrashRecoveryProcessTests
{
    [Fact]
    public void AssertChild_NormalFailureAfterReadyAndPhase_CannotPassCrashEvidence()
    {
        const string childVariable = "JDW_CRASH_HELPER_NORMAL_FAILURE";
        const string phase = "before-kill-failure";
        if (Environment.GetEnvironmentVariable(childVariable) is { } childPath)
        {
            CrashRecoveryProcess.MarkReady(childPath);
            Assert.True(TableDefReader.VerifyCacheHits);
            File.WriteAllText(childPath + ".crashed", phase);
            Assert.Fail("Injected ordinary failure after recording the requested phase.");
            return;
        }

        string directory = Path.Combine(Path.GetTempPath(), $"CrashFailure_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "subject.accdb");
        try
        {
            Xunit.Sdk.XunitException failure = Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => CrashRecoveryProcess.AssertChild(
                "JetDatabaseWriter.Tests.Infrastructure.CrashRecoveryProcessTests.AssertChild_NormalFailureAfterReadyAndPhase_CannotPassCrashEvidence",
                path,
                phase,
                "ordinary child failure after valid markers",
                new Dictionary<string, string> { [childVariable] = path }));
            Assert.Contains("forced termination", failure.Message, StringComparison.Ordinal);
            Assert.Contains("ready=True, phase=before-kill-failure, exit=1", failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public void CrashChild_UnknownMethod_ExitsWithoutDiscoveringTests()
    {
        string[] arguments = CrashRecoveryProcess.ChildArguments("Unrecognized.CrashChild", typeof(object).Assembly.Location);
        PowerShellRunResult result = PowerShellProcessRunner.Run("dotnet", arguments, TimeSpan.FromSeconds(30));
        Assert.Equal(1, result.ExitCode);
        Assert.False(result.TimedOut);
        Assert.Contains("Unknown crash child method", result.StandardError, StringComparison.Ordinal);
        Assert.DoesNotContain("Discovering", result.StandardOutput, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CrashChild_MissingPath_ExitsWithoutRunningParentScenario()
    {
        const string method = "JetDatabaseWriter.Tests.Writer.TransactionCrashRecoveryTests.InterruptedWrites_RecoverExactOriginalBytes";
        string[] arguments = CrashRecoveryProcess.ChildArguments(method, typeof(object).Assembly.Location);
        PowerShellRunResult result = PowerShellProcessRunner.Run("dotnet", arguments, TimeSpan.FromSeconds(30));
        Assert.Equal(1, result.ExitCode);
        Assert.False(result.TimedOut);
        Assert.Contains("Crash child path is missing", result.StandardError, StringComparison.Ordinal);
        Assert.DoesNotContain("Discovering", result.StandardOutput, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MarkReady_RecordsExactLoadedSharedFrameworkProductVersion()
    {
        string path = Path.Combine(Path.GetTempPath(), $"CrashRuntime_{Guid.NewGuid():N}.accdb");
        try
        {
            CrashRecoveryProcess.MarkReady(path);
            string? versionDirectory = Path.GetDirectoryName(typeof(object).Assembly.Location);
            Assert.NotNull(versionDirectory);
            Assert.Equal(Path.GetFileName(versionDirectory), File.ReadAllText(path + ".ready"));
        }
        finally
        {
            File.Delete(path + ".ready");
        }
    }

    [Theory]
    [InlineData("10.0.13")]
    [InlineData("11.0.0-preview.7.26351.1")]
    [InlineData("11.0.0-rc.2.26500.3")]
    public void ChildArguments_PinExactSharedProductVersionIncludingPrerelease(string productVersion)
    {
        string coreLibrary = Path.Combine(Path.GetTempPath(), "dotnet", "shared", "Microsoft.NETCore.App", productVersion, "System.Private.CoreLib.dll");
        string[] arguments = CrashRecoveryProcess.ChildArguments("Example.Test", coreLibrary);
        Assert.Equal(["exec", "--fx-version", productVersion, "--roll-forward", "Disable", typeof(CrashRecoveryProcess).Assembly.Location, "--jdw-crash-child", "Example.Test"], arguments);
        CrashRecoveryProcess.AssertMatchingRuntime(productVersion, productVersion, "matching runtime");
    }

    [Fact]
    public void ChildArguments_UnrecognizedCoreLibLayout_RefusesToGuessRuntime()
    {
        string coreLibrary = Path.Combine(Path.GetTempPath(), "private-runtime", "System.Private.CoreLib.dll");
        Assert.Throws<InvalidOperationException>(() => CrashRecoveryProcess.ChildArguments("Example.Test", coreLibrary));
    }

    [Theory]
    [InlineData("11.0.0-rc.2.26500.3", "11.0.0")]
    [InlineData("11.0.0-rc.2.26500.3", "11.0.0-preview.7.26351.1")]
    [InlineData("11.0.0-rc.2.26500.3", "10.0.13")]
    public void RuntimeMismatch_CannotPassCrashEvidence(string parentRuntime, string childRuntime)
    {
        Xunit.Sdk.XunitException failure = Assert.ThrowsAny<Xunit.Sdk.XunitException>(() => CrashRecoveryProcess.AssertMatchingRuntime(parentRuntime, childRuntime, "runtime evidence diagnostic"));
        Assert.Contains("Child runtime differs", failure.Message, StringComparison.Ordinal);
        Assert.Contains("runtime evidence diagnostic", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_ReportsScenarioPhaseAndBothStreams_RedactsDatabaseAndPassword()
    {
        string path = Path.Combine(Path.GetTempPath(), "private-database.accdb");
        const string password = "private-password";
        var result = new PowerShellRunResult(-1, $"opened {path}", $"failed password {password}", TimedOut: true, OutputComplete: false, ProcessExited: true);

        string diagnostic = CrashRecoveryProcess.Describe("encrypted=True, boundary=Publishing", path, password, ready: true, phase: "Publishing", result, "11.0.0-rc.2.26500.3", "10.0.13");

        Assert.Contains("encrypted=True, boundary=Publishing", diagnostic, StringComparison.Ordinal);
        Assert.Contains("ready=True, phase=Publishing, exit=-1", diagnostic, StringComparison.Ordinal);
        Assert.Contains("parent runtime=11.0.0-rc.2.26500.3, child runtime=10.0.13", diagnostic, StringComparison.Ordinal);
        Assert.Contains("Standard output:", diagnostic, StringComparison.Ordinal);
        Assert.Contains("opened [database]", diagnostic, StringComparison.Ordinal);
        Assert.Contains("Standard error:", diagnostic, StringComparison.Ordinal);
        Assert.Contains("failed password [password]", diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain(path, diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain(password, diagnostic, StringComparison.Ordinal);
    }
}
