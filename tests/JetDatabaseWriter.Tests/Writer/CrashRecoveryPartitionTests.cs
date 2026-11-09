namespace JetDatabaseWriter.Tests.Writer;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>Guards independent crash specimens without removing repeated process interruptions.</summary>
public sealed class CrashRecoveryPartitionTests
{
    [Fact]
    public void TransactionSpecimens_CoverEveryFormatAndEncryptionStateExactlyOnce()
    {
        var actual = new HashSet<(DatabaseFormat Format, bool Encrypted)>();
        foreach (TheoryDataRow<DatabaseFormat, bool> row in TransactionCrashRecoveryTests.Specimens)
        {
            Assert.True(actual.Add(row.Data), $"Duplicate transaction crash specimen: {row.Data}.");
        }

        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            Assert.True(actual.Remove((format, false)), $"Missing unencrypted transaction crash specimen: {format}.");
            Assert.True(actual.Remove((format, true)), $"Missing encrypted transaction crash specimen: {format}.");
        }

        Assert.Empty(actual);
    }

    [Fact]
    public void ReplaySpecimens_CoverEveryNativeEncryptionProviderExactlyOnce()
    {
        var actual = new HashSet<(DatabaseFormat Format, string Fixture)>();
        foreach (TheoryDataRow<DatabaseFormat, string> row in RecoveryReplayCrashTests.Specimens)
        {
            Assert.True(actual.Add(row.Data), $"Duplicate replay crash specimen: {row.Data}.");
        }

        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb })
        {
            Assert.True(actual.Remove((format, string.Empty)), $"Missing unencrypted replay crash specimen: {format}.");
        }

        Assert.True(actual.Remove((DatabaseFormat.Jet3Mdb, "UpstreamJet3Rc4.mdb")));
        Assert.True(actual.Remove((DatabaseFormat.Jet4Mdb, "NativeJet4Rc4.mdb")));
        Assert.True(actual.Remove((DatabaseFormat.AceAccdb, "NativeAceStandard.accdb")));
        Assert.True(actual.Remove((DatabaseFormat.AceAccdb, "NativeAceAgile.accdb")));
        Assert.Empty(actual);
    }
}
