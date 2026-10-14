namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks that crash child diagnostics remain useful without disclosing secrets.</summary>
public sealed class CrashRecoveryProcessTests
{
    [Fact]
    public void Describe_ReportsScenarioPhaseAndBothStreams_RedactsDatabaseAndPassword()
    {
        string path = Path.Combine(Path.GetTempPath(), "private-database.accdb");
        const string password = "private-password";
        var result = new PowerShellRunResult(-1, $"opened {path}", $"failed password {password}", TimedOut: true, OutputComplete: false, ProcessExited: true);

        string diagnostic = CrashRecoveryProcess.Describe("encrypted=True, boundary=Publishing", path, password, ready: true, phase: "Publishing", result);

        Assert.Contains("encrypted=True, boundary=Publishing", diagnostic, StringComparison.Ordinal);
        Assert.Contains("ready=True, phase=Publishing, exit=-1", diagnostic, StringComparison.Ordinal);
        Assert.Contains("Standard output:", diagnostic, StringComparison.Ordinal);
        Assert.Contains("opened [database]", diagnostic, StringComparison.Ordinal);
        Assert.Contains("Standard error:", diagnostic, StringComparison.Ordinal);
        Assert.Contains("failed password [password]", diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain(path, diagnostic, StringComparison.Ordinal);
        Assert.DoesNotContain(password, diagnostic, StringComparison.Ordinal);
    }
}
