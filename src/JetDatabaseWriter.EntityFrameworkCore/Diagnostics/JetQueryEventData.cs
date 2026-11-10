namespace JetDatabaseWriter.EntityFrameworkCore.Diagnostics;

using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

/// <summary>
/// Structured query execution data. Query expressions, parameters and row values are never included.
/// </summary>
public sealed class JetQueryEventData : DbContextEventData
{
    internal JetQueryEventData(
        EventDefinitionBase definition,
        Func<EventDefinitionBase, EventData, string> messageGenerator,
        DbContext? context,
        string executionPath,
        string reason,
        string? tableName = null,
        long rowsRead = 0,
        long rowsReturned = 0,
        TimeSpan duration = default,
        bool completed = false)
        : base(definition, messageGenerator, context)
    {
        this.ExecutionPath = executionPath;
        this.Reason = reason;
        this.TableName = tableName;
        this.RowsRead = rowsRead;
        this.RowsReturned = rowsReturned;
        this.Duration = duration;
        this.Completed = completed;
    }

    /// <summary>Gets the selected execution strategy or table access path.</summary>
    public string ExecutionPath { get; }

    /// <summary>Gets the admission or fallback reason without query values.</summary>
    public string Reason { get; }

    /// <summary>Gets the physical table name for table access events.</summary>
    public string? TableName { get; }

    /// <summary>Gets the number of decoded candidate rows visited by this enumeration.</summary>
    public long RowsRead { get; }

    /// <summary>Gets the number of matching buffers returned by this enumeration.</summary>
    public long RowsReturned { get; }

    /// <summary>Gets elapsed enumeration time, including time spent awaiting the consumer.</summary>
    public TimeSpan Duration { get; }

    /// <summary>Gets a value indicating whether enumeration reached its end or requested row limit.</summary>
    public bool Completed { get; }
}
