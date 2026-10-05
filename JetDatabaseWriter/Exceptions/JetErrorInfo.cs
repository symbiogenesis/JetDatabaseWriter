namespace JetDatabaseWriter.Exceptions;

/// <summary>Context for a database failure; no row values are captured.</summary>
public sealed record JetErrorInfo
{
    /// <summary>Gets the TableName associated with the failure, when known.</summary>
    public string? TableName { get; init; }

    /// <summary>Gets the ColumnName associated with the failure, when known.</summary>
    public string? ColumnName { get; init; }

    /// <summary>Gets the IndexName associated with the failure, when known.</summary>
    public string? IndexName { get; init; }

    /// <summary>Gets the RelationshipName associated with the failure, when known.</summary>
    public string? RelationshipName { get; init; }

    /// <summary>Gets the ObjectName associated with the failure, when known.</summary>
    public string? ObjectName { get; init; }

    /// <summary>Gets the Reason associated with the failure, when known.</summary>
    public string? Reason { get; init; }

    /// <summary>Gets the page number associated with the failure, when known.</summary>
    public long? PageNumber { get; init; }
}
