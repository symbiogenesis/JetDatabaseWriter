namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Diagnostics;
using System.Threading;
using JetDatabaseWriter.Exceptions;

/// <summary>Emits best-effort diagnostic text without changing database operation outcomes.</summary>
internal static class DiagnosticTrace
{
    private static readonly AsyncLocal<bool> Delivering = new();

    /// <summary>Refuses callback reentry before acquiring a database gate or starting disposal.</summary>
    /// <exception cref="JetOperationException">A diagnostic callback is active on this flow.</exception>
    internal static void ThrowIfReentrant()
    {
        if (Delivering.Value)
        {
            throw new JetOperationException(JetErrorCode.ReentrantDiagnosticCall, "Database operations and disposal cannot be entered from a diagnostic callback.");
        }
    }

    /// <summary>Writes a diagnostic without allowing listener failures to change the operation's outcome.</summary>
    /// <param name="message">Diagnostic text that excludes cell values, paths, identities and exception messages.</param>
    internal static void WriteLine(string message) => Deliver(message, warning: false);

    /// <summary>Emits a warning outside page and I/O locks.</summary>
    /// <param name="message">The diagnostic text.</param>
    internal static void Warning(string message) => Deliver(message, warning: true);

    private static void Deliver(string message, bool warning)
    {
        if (Delivering.Value)
        {
            return;
        }

        Delivering.Value = true;
        try
        {
            JetFailureMetrics.Diagnostic();
            if (warning)
            {
                Trace.TraceWarning(message);
            }
            else
            {
                Trace.WriteLine(message);
            }
        }
#pragma warning disable CA1031, RCS1075 // Caller-provided diagnostic listeners must not change database operation outcomes.
        catch (Exception)
#pragma warning restore CA1031, RCS1075
        {
            JetFailureMetrics.ListenerFailure();
        }
        finally
        {
            Delivering.Value = false;
        }
    }
}
