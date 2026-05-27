namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Relationships;
using Xunit;

public sealed class RelationshipRuntimePolicyTests
{
    [Fact]
    public void CascadeDepthPolicy_AllowsConfiguredLimit()
    {
        RelationshipCascadePolicy.ThrowIfDepthExceeded(AccessWriter.CascadeMaxDepth);
    }

    [Fact]
    public void CascadeDepthPolicy_RejectsBeyondConfiguredLimit()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => RelationshipCascadePolicy.ThrowIfDepthExceeded(AccessWriter.CascadeMaxDepth + 1));

        Assert.Contains("cascade depth", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void KeyBuilder_ProjectsNonNullKeysAndUsesCanonicalNormalization()
    {
        var sourceRows = new List<object?[]>
        {
            new object?[] { "alpha", 5 },
            new object?[] { "ALPHA", 5 },
            new object?[] { DBNull.Value, 5 },
            new object?[] { "bravo", null },
        };

        int[] relationshipColumns = [0, 1];

        var projectedRows = RelationshipKeyBuilder.ProjectNonNullKeys(sourceRows, relationshipColumns);
        var keys = RelationshipKeyBuilder.BuildSetFromProjectedKeys(projectedRows);
        string? expectedKey = RelationshipKeyBuilder.Build(
            ["ALPHA", 5],
            RelationshipKeyBuilder.CreateIdentityOrdinals(2));

        Assert.Equal(2, projectedRows.Count);
        string actualKey = Assert.Single(keys);
        Assert.Equal(expectedKey, actualKey);
    }
}
