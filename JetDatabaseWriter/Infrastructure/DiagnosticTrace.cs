namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Diagnostics;

/// <summary>Emits best-effort diagnostic text without changing database operation outcomes.</summary>
internal static class DiagnosticTrace
{
    /// <summary>Writes a diagnostic without allowing listener failures to change the operation's outcome.</summary>
    /// <param name="message">Diagnostic text that excludes cell values, paths, identities and exception messages.</param>
    internal static void WriteLine(string message)
    {
        try
        {
            Trace.WriteLine(message);
        }
#pragma warning disable CA1031, RCS1075 // Caller-provided diagnostic listeners must not change database operation outcomes.
        catch (Exception)
#pragma warning restore CA1031, RCS1075
        {
            // Listener failures are deliberately ignored.
        }
    }
}
