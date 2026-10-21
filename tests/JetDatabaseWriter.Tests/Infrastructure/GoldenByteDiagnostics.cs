namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

/// <summary>Preserves normalized golden bytes and a complete byte/page comparison for review.</summary>
internal static class GoldenByteDiagnostics
{
    /// <summary>Exports a specimen on failure, or on every check when an explicit output directory is supplied.</summary>
    /// <param name="specimenId">The stable filename-safe specimen identity.</param>
    /// <param name="actual">The normalized bytes under test.</param>
    /// <param name="expectedSha256">The approved SHA-256 hash.</param>
    /// <param name="pageSize">The size of one database page.</param>
    /// <param name="baselinePath">The approved binary fixture, when available.</param>
    /// <param name="outputDirectory">An absolute output directory, or null to retain only failures in system temp.</param>
    /// <returns>The actual hash, baseline integrity verdict and diagnostic evidence description.</returns>
    /// <exception cref="ArgumentException">The specimen identity or output directory is unsafe.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The page size is not positive.</exception>
    internal static (string ActualHash, bool BaselineMatches, string Details) Capture(
        string specimenId,
        byte[] actual,
        string expectedSha256,
        int pageSize,
        string baselinePath,
        string? outputDirectory)
    {
        ArgumentNullException.ThrowIfNull(actual);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        ArgumentException.ThrowIfNullOrWhiteSpace(specimenId);
        foreach (char character in specimenId)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character is not '.' and not '_' and not '-')
            {
                throw new ArgumentException("The specimen identity must be filename-safe.", nameof(specimenId));
            }
        }

        if (outputDirectory is not null && (!Path.IsPathFullyQualified(outputDirectory) || outputDirectory.Contains('%', StringComparison.Ordinal)))
        {
            throw new ArgumentException("Golden output must be an absolute path without unresolved environment tokens.", nameof(outputDirectory));
        }

        string actualHash = Hash(actual);
        byte[]? baseline = File.Exists(baselinePath) ? File.ReadAllBytes(baselinePath) : null;
        string? baselineHash = baseline is null ? null : Hash(baseline);
        bool baselineMatches = baseline is null || string.Equals(baselineHash, expectedSha256, StringComparison.Ordinal);
        if (outputDirectory is null && baselineMatches && string.Equals(actualHash, expectedSha256, StringComparison.Ordinal))
        {
            return (actualHash, baselineMatches, string.Empty);
        }

        outputDirectory ??= Path.Combine(Path.GetTempPath(), "jdw-golden-" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
        Directory.CreateDirectory(outputDirectory);
        string prefix = Path.Combine(outputDirectory, specimenId);
        File.WriteAllBytes(prefix + ".actual.bin", actual);
        var report = new StringBuilder();
        report.AppendLine(CultureInfo.InvariantCulture, $"Specimen: {specimenId}")
            .AppendLine(CultureInfo.InvariantCulture, $"Page size: {pageSize}")
            .AppendLine(CultureInfo.InvariantCulture, $"Approved SHA-256: {expectedSha256}")
            .AppendLine(CultureInfo.InvariantCulture, $"Actual SHA-256: {actualHash}; length: {actual.Length}");
        if (baseline is null)
        {
            report.AppendLine(CultureInfo.InvariantCulture, $"Approved baseline bytes unavailable: {baselinePath}. Hash acceptance remains unchanged.");
        }
        else
        {
            report.AppendLine(CultureInfo.InvariantCulture, $"Baseline SHA-256: {baselineHash}; length: {baseline.Length}");
            if (!baselineMatches)
            {
                report.AppendLine("Approved baseline hash mismatch: the fixture does not match the pinned hash.");
            }

            var differences = new StringBuilder("Offset,Page,PageOffset,Expected,Actual" + Environment.NewLine);
            var pages = new SortedSet<int>();
            int count = 0;
            for (int offset = 0; offset < Math.Max(baseline.Length, actual.Length); offset++)
            {
                if (offset < baseline.Length && offset < actual.Length && baseline[offset] == actual[offset])
                {
                    continue;
                }

                string expectedByte = offset < baseline.Length ? baseline[offset].ToString("X2", CultureInfo.InvariantCulture) : "EOF";
                string actualByte = offset < actual.Length ? actual[offset].ToString("X2", CultureInfo.InvariantCulture) : "EOF";
                differences.Append(CultureInfo.InvariantCulture, $"{offset},{offset / pageSize},{offset % pageSize},{expectedByte},{actualByte}").AppendLine();
                pages.Add(offset / pageSize);
                count++;
            }

            File.WriteAllText(prefix + ".diff.csv", differences.ToString());
            report.AppendLine(CultureInfo.InvariantCulture, $"Differing bytes: {count}")
                .AppendLine(CultureInfo.InvariantCulture, $"Differing pages: {string.Join(",", pages)}")
                .AppendLine("Page numbers index the normalized specimen; TDEF chains use chain order.");
        }

        report.AppendLine(CultureInfo.InvariantCulture, $"Evidence: {prefix}.actual.bin and {prefix}.report.txt");
        File.WriteAllText(prefix + ".report.txt", report.ToString());
        return (actualHash, baselineMatches, report.ToString());
    }

    private static string Hash(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes));
}
