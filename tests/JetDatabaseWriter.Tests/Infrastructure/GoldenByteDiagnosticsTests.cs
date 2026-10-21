namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Security.Cryptography;
using Xunit;

public sealed class GoldenByteDiagnosticsTests
{
    [Fact]
    public void Capture_MatchingSpecimen_ExportsExactBytesWithoutChangingBaseline()
    {
        string directory = NewDirectory();
        byte[] expected = [1, 2, 3, 4];
        string baselinePath = Path.Combine(directory, "approved.bin");
        File.WriteAllBytes(baselinePath, expected);

        (string hash, bool baselineMatches, string details) = GoldenByteDiagnostics.Capture(
            "Jet4Mdb.bootstrap", expected, Hash(expected), 2, baselinePath, directory);

        Assert.Equal(Hash(expected), hash);
        Assert.True(baselineMatches);
        Assert.Equal(expected, File.ReadAllBytes(Path.Combine(directory, "Jet4Mdb.bootstrap.actual.bin")));
        Assert.Equal(expected, File.ReadAllBytes(baselinePath));
        Assert.Contains("Differing bytes: 0", details, StringComparison.Ordinal);
        Assert.Equal("Offset,Page,PageOffset,Expected,Actual" + Environment.NewLine, File.ReadAllText(Path.Combine(directory, "Jet4Mdb.bootstrap.diff.csv")));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Capture_LengthAndValueChanges_ReportAbsoluteAndPageOffsets(bool appended)
    {
        string directory = NewDirectory();
        byte[] expected = appended ? [1, 2, 3, 4] : [1, 2, 3, 4, 5];
        byte[] actual = appended ? [1, 9, 3, 4, 5] : [1, 9, 3, 4];
        string baselinePath = Path.Combine(directory, "approved.bin");
        File.WriteAllBytes(baselinePath, expected);

        (_, bool baselineMatches, string details) = GoldenByteDiagnostics.Capture(
            "AceAccdb.table", actual, Hash(expected), 2, baselinePath, directory);

        Assert.True(baselineMatches);
        string differences = File.ReadAllText(Path.Combine(directory, "AceAccdb.table.diff.csv"));
        Assert.Contains("1,0,1,02,09", differences, StringComparison.Ordinal);
        Assert.Contains(appended ? "4,2,0,EOF,05" : "4,2,0,05,EOF", differences, StringComparison.Ordinal);
        Assert.Contains("Differing bytes: 2", details, StringComparison.Ordinal);
        Assert.Contains("Differing pages: 0,2", details, StringComparison.Ordinal);
    }

    [Fact]
    public void Capture_IncorrectApprovedBaseline_CannotValidateMatchingActualHash()
    {
        string directory = NewDirectory();
        byte[] actual = [1, 2, 3, 4];
        string baselinePath = Path.Combine(directory, "approved.bin");
        File.WriteAllBytes(baselinePath, [1, 9, 3, 4]);

        (_, bool baselineMatches, string details) = GoldenByteDiagnostics.Capture(
            "AceAccdb.Files.flat", actual, Hash(actual), 2, baselinePath, directory);

        Assert.False(baselineMatches);
        Assert.Contains("Approved baseline hash mismatch", details, StringComparison.Ordinal);
        Assert.Equal(actual, File.ReadAllBytes(Path.Combine(directory, "AceAccdb.Files.flat.actual.bin")));
    }

    [Fact]
    public void Capture_MissingBaselineAndNoOutput_OnlyCreatesEvidenceOnFailure()
    {
        string directory = NewDirectory();
        string baselinePath = Path.Combine(directory, "missing.bin");
        byte[] expected = [1, 2, 3, 4];
        (_, bool baselineMatches, string matching) = GoldenByteDiagnostics.Capture(
            "Jet3Mdb.table", expected, Hash(expected), 2, baselinePath, outputDirectory: null);
        Assert.True(baselineMatches);
        Assert.Equal(string.Empty, matching);

        (_, baselineMatches, string mismatch) = GoldenByteDiagnostics.Capture(
            "Jet3Mdb.table", [1, 9, 3, 4], Hash(expected), 2, baselinePath, outputDirectory: null);
        Assert.True(baselineMatches);
        Assert.Contains("Approved baseline bytes unavailable", mismatch, StringComparison.Ordinal);
        Assert.Contains(Path.GetFullPath(Path.GetTempPath()), mismatch, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("relative-output")]
    [InlineData("%TEMP%/golden")]
    public void Capture_InvalidOutputPath_IsRejectedBeforeWriting(string outputDirectory)
        => Assert.Throws<ArgumentException>(() => GoldenByteDiagnostics.Capture(
            "specimen", [1], Hash([1]), 2, Path.Combine(NewDirectory(), "missing.bin"), outputDirectory));

    [Fact]
    public void Capture_UnsafeSpecimenIdentity_IsRejectedBeforeWriting()
        => Assert.Throws<ArgumentException>(() => GoldenByteDiagnostics.Capture(
            "../other-session", [1], Hash([1]), 2, Path.Combine(NewDirectory(), "missing.bin"), NewDirectory()));

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));

    private static string NewDirectory() => Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "jdw-golden-tests-" + Guid.NewGuid().ToString("N"))).FullName;
}
