namespace JetDatabaseWriter.EntityFrameworkCore.Diagnostics;

using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Logging;

/// <summary>
/// Provider-specific logging definitions for the <see cref="JetDatabaseWriter"/> Entity Framework Core provider.
/// </summary>
/// <param name="options">The EF Core logging and warning options.</param>
public sealed class JetLoggingDefinitions(ILoggingOptions options) : LoggingDefinitions
{
    internal EventDefinition<string, string> QueryExecution { get; } = new(
        options,
        JetEventId.QueryExecution,
        LogLevel.Debug,
        nameof(JetEventId.QueryExecution),
        static level => LoggerMessage.Define<string, string>(level, JetEventId.QueryExecution, "Jet query uses {ExecutionPath}: {Reason}."));

    internal EventDefinition<string, string, string> TableRead { get; } = new(
        options,
        JetEventId.TableRead,
        LogLevel.Debug,
        nameof(JetEventId.TableRead),
        static level => LoggerMessage.Define<string, string, string>(level, JetEventId.TableRead, "Jet table {TableName} uses {ExecutionPath}: {Reason}."));

    internal EventDefinition<string, long, long, double, bool> TableReadCompleted { get; } = new(
        options,
        JetEventId.TableReadCompleted,
        LogLevel.Debug,
        nameof(JetEventId.TableReadCompleted),
        static level => LoggerMessage.Define<string, long, long, double, bool>(level, JetEventId.TableReadCompleted, "Jet table {TableName} read {RowsRead} candidate rows and returned {RowsReturned} buffers in {ElapsedMilliseconds} ms; completed: {Completed}."));
}
