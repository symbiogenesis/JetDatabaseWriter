namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Relationships;
using Xunit;

public sealed class RelationshipRuntimePolicyTests
{
    [Fact]
    public void CascadeDepthPolicy_AllowsConfiguredLimit() => RelationshipCascadePolicy.ThrowIfDepthExceeded(RelationshipCascadePolicy.MaxDepth);

    [Fact]
    public void CascadeDepthPolicy_RejectsBeyondConfiguredLimit()
    {
        JetConstraintException exception = Assert.Throws<JetConstraintException>(
            () => RelationshipCascadePolicy.ThrowIfDepthExceeded(RelationshipCascadePolicy.MaxDepth + 1));

        Assert.Equal(JetErrorCode.CascadeDepthExceeded, exception.ErrorCode);
        Assert.NotNull(exception.ErrorInfo);
        Assert.Null(exception.ErrorInfo.TableName);
        Assert.Null(exception.ErrorInfo.RelationshipName);
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

        List<object?[]> projectedRows = RelationshipKeyBuilder.ProjectNonNullKeys(sourceRows, relationshipColumns);
        HashSet<string> keys = RelationshipKeyBuilder.BuildSetFromProjectedKeys(projectedRows);
        string? expectedKey = RelationshipKeyBuilder.Build(
            ["ALPHA", 5],
            RelationshipKeyBuilder.CreateIdentityOrdinals(2));

        Assert.Equal(2, projectedRows.Count);
        string actualKey = Assert.Single(keys);
        Assert.Equal(expectedKey, actualKey);
    }
}
