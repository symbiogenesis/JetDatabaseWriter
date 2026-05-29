namespace JetDatabaseWriter.Tests.Infrastructure;

using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;
using Xunit;

public sealed class DaoPowerShellHostResolverTests
{
    [Fact]
    public void GetCandidateHostPaths_Wow64ProcessPrefersSysnativeForNativeHost()
    {
        IReadOnlyList<string> candidates = DaoPowerShellHostResolver.GetCandidateHostPaths(
            @"C:\Windows",
            is64BitOperatingSystem: true,
            is64BitProcess: false,
            preferWow64Host: false);

        Assert.Equal(
            [
                @"C:\Windows\Sysnative\WindowsPowerShell\v1.0\powershell.exe",
                @"C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe",
            ],
            candidates);
    }

    [Fact]
    public void GetCandidateHostPaths_64BitProcessCanPreferWow64First()
    {
        IReadOnlyList<string> candidates = DaoPowerShellHostResolver.GetCandidateHostPaths(
            @"C:\Windows",
            is64BitOperatingSystem: true,
            is64BitProcess: true,
            preferWow64Host: true);

        Assert.Equal(
            [
                @"C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe",
                @"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
            ],
            candidates);
    }

    [Fact]
    public void GetCandidateHostPaths_32BitWindowsOnlyReturnsSystem32()
    {
        IReadOnlyList<string> candidates = DaoPowerShellHostResolver.GetCandidateHostPaths(
            @"C:\Windows",
            is64BitOperatingSystem: false,
            is64BitProcess: false,
            preferWow64Host: true);

        Assert.Equal(
            [@"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"],
            candidates);
    }

    [Fact]
    public void GetCandidateHostPaths_64BitProcessPrefersNativeFirst()
    {
        // ARM64 native Office (Program Files) or x64 Office on x64 OS.
        IReadOnlyList<string> candidates = DaoPowerShellHostResolver.GetCandidateHostPaths(
            @"C:\Windows",
            is64BitOperatingSystem: true,
            is64BitProcess: true,
            preferWow64Host: false);

        Assert.Equal(
            [
                @"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
                @"C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe",
            ],
            candidates);
    }
}
