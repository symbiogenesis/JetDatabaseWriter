namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Globalization;
using System.IO;
using Xunit;
using Xunit.Sdk;

/// <summary>Retains complete fault-test images only when their full byte comparison fails.</summary>
internal static class StatementFaultDiagnostics
{
    internal static void AssertEqual(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> actual, StatementFaultContext context, string? outputDirectory = null, string phase = "")
    {
        if (expected.SequenceEqual(actual))
        {
            return;
        }

        try
        {
            Assert.Equal(expected, actual);
        }
        catch (XunitException failure)
        {
            string description = context + ", phase=" + phase;
            outputDirectory ??= Environment.GetEnvironmentVariable("JDW_FAULT_DIAGNOSTICS_DIRECTORY");
            string evidence;
            try
            {
                outputDirectory ??= Path.Combine(Path.GetTempPath(), "jdw-statement-fault-" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
                if (!Path.IsPathFullyQualified(outputDirectory))
                {
                    throw new ArgumentException("Statement fault output must be an absolute path.", nameof(outputDirectory));
                }

                Directory.CreateDirectory(outputDirectory);
                string prefix = Path.Combine(outputDirectory, "fault-" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture));
                File.WriteAllBytes(prefix + ".expected.bin", expected);
                File.WriteAllBytes(prefix + ".actual.bin", actual);
                File.WriteAllText(prefix + ".txt", description + Environment.NewLine + failure.Message);
                evidence = "Complete expected and actual images: " + prefix;
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException or ArgumentException)
            {
                evidence = "Unable to retain complete fault images: " + error.Message;
            }

            throw new XunitException(description + Environment.NewLine + evidence, failure);
        }
    }
}
