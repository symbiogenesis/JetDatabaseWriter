namespace JetDatabaseWriter.EntityFrameworkCore.Diagnostics;

using System;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

internal static class JetQueryDiagnostics
{
    internal static void QueryExecution(IDiagnosticsLogger<DbLoggerCategory.Query> logger, DbContext context, string path, string reason)
    {
        EventDefinition<string, string> definition = ((JetLoggingDefinitions)logger.Definitions).QueryExecution;
        if (logger.ShouldLog(definition))
        {
            definition.Log(logger, path, reason);
        }

        if (logger.NeedsEventData(definition, out bool diagnosticSourceEnabled, out bool simpleLogEnabled))
        {
            JetQueryEventData data = new(definition, QueryExecutionMessage, context, path, reason);
            logger.DispatchEventData(definition, data, diagnosticSourceEnabled, simpleLogEnabled);
        }
    }

    internal static void TableRead(IDiagnosticsLogger<DbLoggerCategory.Query>? logger, DbContext? context, string table, string path, string reason)
    {
        if (logger is null)
        {
            return;
        }

        EventDefinition<string, string, string> definition = ((JetLoggingDefinitions)logger.Definitions).TableRead;
        if (logger.ShouldLog(definition))
        {
            definition.Log(logger, table, path, reason);
        }

        if (logger.NeedsEventData(definition, out bool diagnosticSourceEnabled, out bool simpleLogEnabled))
        {
            JetQueryEventData data = new(definition, TableReadMessage, context, path, reason, table);
            logger.DispatchEventData(definition, data, diagnosticSourceEnabled, simpleLogEnabled);
        }
    }

    internal static bool MeasuresTableRead(IDiagnosticsLogger<DbLoggerCategory.Query>? logger)
    {
        if (logger is null)
        {
            return false;
        }

        EventDefinitionBase definition = ((JetLoggingDefinitions)logger.Definitions).TableReadCompleted;
        return logger.ShouldLog(definition) || logger.NeedsEventData(definition, out _, out _);
    }

    internal static void TableReadCompleted(
        IDiagnosticsLogger<DbLoggerCategory.Query> logger,
        DbContext? context,
        string table,
        string path,
        string reason,
        long rowsRead,
        long rowsReturned,
        TimeSpan duration,
        bool completed)
    {
        EventDefinition<string, long, long, double, bool> definition = ((JetLoggingDefinitions)logger.Definitions).TableReadCompleted;
        if (logger.ShouldLog(definition))
        {
            definition.Log(logger, table, rowsRead, rowsReturned, duration.TotalMilliseconds, completed);
        }

        if (logger.NeedsEventData(definition, out bool diagnosticSourceEnabled, out bool simpleLogEnabled))
        {
            JetQueryEventData data = new(definition, TableReadCompletedMessage, context, path, reason, table, rowsRead, rowsReturned, duration, completed);
            logger.DispatchEventData(definition, data, diagnosticSourceEnabled, simpleLogEnabled);
        }
    }

    private static string QueryExecutionMessage(EventDefinitionBase definition, EventData data)
    {
        var query = (JetQueryEventData)data;
        return ((EventDefinition<string, string>)definition).GenerateMessage(query.ExecutionPath, query.Reason);
    }

    private static string TableReadMessage(EventDefinitionBase definition, EventData data)
    {
        var query = (JetQueryEventData)data;
        return ((EventDefinition<string, string, string>)definition).GenerateMessage(query.TableName!, query.ExecutionPath, query.Reason);
    }

    private static string TableReadCompletedMessage(EventDefinitionBase definition, EventData data)
    {
        var query = (JetQueryEventData)data;
        return ((EventDefinition<string, long, long, double, bool>)definition).GenerateMessage(query.TableName!, query.RowsRead, query.RowsReturned, query.Duration.TotalMilliseconds, query.Completed);
    }
}
