namespace JetDatabaseWriter.Tests.Writer;

using System.Collections.Generic;
using Xunit;

/// <summary>Guards complete, nonoverlapping fault positions within every format partition.</summary>
public sealed class StatementAtomicityPartitionTests
{
    [Fact]
    public void FaultCases_CoverEveryMutationAndShardExactlyOnce()
    {
        var actual = new HashSet<(string Statement, int Shard)>();
        foreach (TheoryDataRow<string, int> row in StatementAtomicityTestBase.Cases)
        {
            Assert.True(actual.Add(row.Data), $"Duplicate fault partition: {row.Data}.");
        }

        foreach (string statement in new[] { "Insert", "Update", "Delete", "CascadeDelete", "AddColumn", "CreateTable" })
        {
            for (int shard = 0; shard < 8; shard++)
            {
                Assert.True(actual.Remove((statement, shard)), $"Missing fault partition: {statement}, {shard}.");
            }
        }

        Assert.Empty(actual);
    }
}
