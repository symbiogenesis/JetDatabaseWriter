namespace JetDatabaseWriter.EntityFrameworkCore.Diagnostics;

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;

/// <summary>
/// Events emitted under the EF Core query category by JetDatabaseWriter.
/// </summary>
public static class JetEventId
{
    /// <summary>Identifies the selected buffer or entity execution strategy.</summary>
    public static readonly EventId QueryExecution = new(CoreEventId.ProviderBaseId + 1, "JetDatabaseWriter.Query.QueryExecution");

    /// <summary>Identifies the actual index or scan path and any required CLR sorting.</summary>
    public static readonly EventId TableRead = new(CoreEventId.ProviderBaseId + 2, "JetDatabaseWriter.Query.TableRead");

    /// <summary>Reports row counts and elapsed time when a table enumeration ends or is disposed.</summary>
    public static readonly EventId TableReadCompleted = new(CoreEventId.ProviderBaseId + 3, "JetDatabaseWriter.Query.TableReadCompleted");
}
