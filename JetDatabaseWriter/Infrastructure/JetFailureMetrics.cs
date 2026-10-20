namespace JetDatabaseWriter.Infrastructure;

using System.Threading;
using JetDatabaseWriter.Exceptions;

/// <summary>Updates counters without invoking diagnostic callbacks or creating an EventSource.</summary>
internal static class JetFailureMetrics
{
    private static long failures;
    private static long corruptions;
    private static long diagnostics;
    private static long listenerFailures;

    internal static long Failures => Interlocked.Read(ref failures);

    internal static long Corruptions => Interlocked.Read(ref corruptions);

    internal static long Diagnostics => Interlocked.Read(ref diagnostics);

    internal static long ListenerFailures => Interlocked.Read(ref listenerFailures);

    internal static JetErrorInfo Context(JetErrorCode code, JetErrorInfo? context, bool corruption = false)
    {
        if (code != JetErrorCode.None)
        {
            _ = Interlocked.Increment(ref failures);
            if (corruption)
            {
                _ = Interlocked.Increment(ref corruptions);
            }
        }

        return context ?? new JetErrorInfo();
    }

    internal static void Diagnostic() => Interlocked.Increment(ref diagnostics);

    internal static void ListenerFailure() => Interlocked.Increment(ref listenerFailures);
}
