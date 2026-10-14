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
    public void KeyBuilder_ProjectsNullComponentsAndUsesCanonicalNormalization()
    {
        var sourceRows = new List<object?[]>
        {
            new object?[] { "alpha", 5 },
            new object?[] { "ALPHA", 5 },
            new object?[] { DBNull.Value, 5 },
            new object?[] { "bravo", null },
            new object?[] { null, DBNull.Value },
        };

        int[] relationshipColumns = [0, 1];

        List<object?[]> projectedRows = RelationshipKeyBuilder.ProjectKeys(sourceRows, relationshipColumns);
        HashSet<string> keys = RelationshipKeyBuilder.BuildSetFromProjectedKeys(projectedRows);
        string? expectedKey = RelationshipKeyBuilder.BuildKey(
            ["ALPHA", 5],
            RelationshipKeyBuilder.CreateIdentityOrdinals(2));

        Assert.Equal(5, projectedRows.Count);
        Assert.Equal(4, keys.Count);
        Assert.Contains(expectedKey!, keys);
        Assert.Contains(RelationshipKeyBuilder.BuildKey([null, 5], relationshipColumns)!, keys);
        Assert.Contains(RelationshipKeyBuilder.BuildKey(["bravo", null], relationshipColumns)!, keys);
        Assert.Contains(RelationshipKeyBuilder.BuildKey([null, null], relationshipColumns)!, keys);
    }
}
