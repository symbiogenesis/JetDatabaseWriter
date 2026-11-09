namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using JetDatabaseWriter.Enums;
using Xunit;
using Xunit.Sdk;

/// <summary>Checks precise fault identities and complete failure images without success-path I/O.</summary>
public sealed class StatementFaultDiagnosticsTests
{
    [Fact]
    public void EqualImages_DoNotCreateDiagnosticDirectory()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-fault-test-" + Guid.NewGuid().ToString("N"));
        StatementFaultDiagnostics.AssertEqual([1, 2], [1, 2], new StatementFaultContext(DatabaseFormat.Jet3Mdb, "Insert", 2, "write", 17), directory);
        Assert.False(Directory.Exists(directory));
    }

    [Fact]
    public void MismatchedImages_RetainEveryByteAndOriginalAssertionWithFaultContext()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-fault-test-" + Guid.NewGuid().ToString("N"));
        var context = new StatementFaultContext(DatabaseFormat.Jet3Mdb, "Insert", 2, "partial-write", 17);
        try
        {
            XunitException failure = Assert.Throws<XunitException>(() => StatementFaultDiagnostics.AssertEqual([1, 2, 3], [1, 9], context, directory, "rollback"));
            Assert.Contains("format=Jet3Mdb, statement=Insert, shard=2, mode=partial-write, point=17", failure.Message, StringComparison.Ordinal);
            Assert.Contains("phase=rollback", failure.Message, StringComparison.Ordinal);
            Assert.IsAssignableFrom<XunitException>(failure.InnerException);
            Assert.Equal(new byte[] { 1, 2, 3 }, File.ReadAllBytes(Assert.Single(Directory.GetFiles(directory, "*.expected.bin"))));
            Assert.Equal(new byte[] { 1, 9 }, File.ReadAllBytes(Assert.Single(Directory.GetFiles(directory, "*.actual.bin"))));
            Assert.Contains(context.ToString(), File.ReadAllText(Assert.Single(Directory.GetFiles(directory, "*.txt"))), StringComparison.Ordinal);

            Assert.Throws<XunitException>(() => StatementFaultDiagnostics.AssertEqual([1], [2], context, directory));
            Assert.Equal(2, Directory.GetFiles(directory, "*.actual.bin").Length);
        }
        finally
        {
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public void DiagnosticWriteFailure_PreservesTheOriginalByteAssertion()
    {
        string file = Path.Combine(Path.GetTempPath(), "jdw-fault-test-" + Guid.NewGuid().ToString("N"));
        File.WriteAllText(file, "Blocks creation of the requested output directory.");
        try
        {
            var context = new StatementFaultContext(DatabaseFormat.AceAccdb, "Delete", 1, "read", 2);
            XunitException failure = Assert.Throws<XunitException>(() => StatementFaultDiagnostics.AssertEqual([1], [2], context, file));
            Assert.Contains("Unable to retain complete fault images", failure.Message, StringComparison.Ordinal);
            Assert.IsAssignableFrom<XunitException>(failure.InnerException);
            Assert.Contains(context.ToString(), failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(file);
        }
    }
}
