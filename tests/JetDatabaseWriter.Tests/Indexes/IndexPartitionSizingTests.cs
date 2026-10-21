namespace JetDatabaseWriter.Tests.Indexes;

using System;
using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Helpers;
using JetDatabaseWriter.Indexes.Models;
using Xunit;

/// <summary>Checks partition boundaries against actual page emission, including prefix collapse.</summary>
public sealed class IndexPartitionSizingTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    public void IntermediatePartitions_MatchEmittedCapacity(DatabaseFormat format, bool retainPrefix)
    {
        var profile = JetFormat.ForNewDatabase(format);
        var entries = new List<DecodedIntermediateEntry>();
        for (int i = 0; i < 600; i++)
        {
            byte[] key = new byte[300];
            key[0] = retainPrefix ? (byte)1 : checked((byte)(i / 100));
            key[^2] = checked((byte)(i >> 8));
            key[^1] = unchecked((byte)i);
            entries.Add(new(new(key, i + 1, 0), i + 1000));
        }

        List<List<DecodedIntermediateEntry>>? actual = IndexHelpers.TryGreedySplitIntermediateInN(profile.IndexPage, profile.PageSize, 7, entries);
        Assert.NotNull(actual);
        Assert.Equal(entries, actual.SelectMany(group => group));
        foreach (List<DecodedIntermediateEntry> group in actual)
        {
            byte[]? emitted = IndexPageCodec.TryBuildIntermediatePage(profile.IndexPage, profile.PageSize, 7, group, 0, 0, 0);
            Assert.NotNull(emitted);
            int next = entries.IndexOf(group[^1]) + 1;
            if (next < entries.Count)
            {
                Assert.Null(IndexPageCodec.TryBuildIntermediatePage(profile.IndexPage, profile.PageSize, 7, [.. group, entries[next]], 0, 0, 0));
            }
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IntermediatePartitions_OversizedSingletonRefuses(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        List<DecodedIntermediateEntry> entries = [new(new(new byte[profile.PageSize], 1, 0), 1), new(new([1], 2, 0), 2)];
        Assert.Null(IndexHelpers.TryGreedySplitIntermediateInN(profile.IndexPage, profile.PageSize, 7, entries));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public void IntermediatePartitions_InvalidPointerAfterOverflowThrows(DatabaseFormat format)
    {
        var profile = JetFormat.ForNewDatabase(format);
        List<DecodedIntermediateEntry> entries = [new(new(new byte[profile.PageSize], 1, 0), 1), new(new([1], 2, 0), 0x100000000L)];
        Assert.Throws<ArgumentOutOfRangeException>(() => IndexHelpers.TryGreedySplitIntermediateInN(profile.IndexPage, profile.PageSize, 7, entries));
    }
}
