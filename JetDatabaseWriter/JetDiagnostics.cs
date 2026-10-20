namespace JetDatabaseWriter;

using System.Diagnostics.Tracing;
using JetDatabaseWriter.Infrastructure;

/// <summary>Optional process-wide diagnostic counters, containing no database values or paths.</summary>
public static class JetDiagnostics
{
    /// <summary>
    /// Registers the JetDatabaseWriter EventSource. Call before opening databases, then
    /// enable EventCounters with an EventListener or a diagnostic collector.
    /// Recording failures never invokes a listener or initializes this provider.
    /// </summary>
    /// <returns>The process-wide provider, owned by the library.</returns>
    public static EventSource EnableCounters() => FailureEventSource.Instance;

    [EventSource(Name = "JetDatabaseWriter")]
    private sealed class FailureEventSource : EventSource
    {
        internal static readonly FailureEventSource Instance = new();

        private readonly PollingCounter failures;
        private readonly PollingCounter corruptions;
        private readonly PollingCounter diagnostics;
        private readonly PollingCounter listenerFailures;

        private FailureEventSource()
        {
            this.failures = new PollingCounter("failures", this, static () => JetFailureMetrics.Failures);
            this.corruptions = new PollingCounter("corruptions", this, static () => JetFailureMetrics.Corruptions);
            this.diagnostics = new PollingCounter("diagnostics", this, static () => JetFailureMetrics.Diagnostics);
            this.listenerFailures = new PollingCounter("listener-failures", this, static () => JetFailureMetrics.ListenerFailures);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.failures.Dispose();
                this.corruptions.Dispose();
                this.diagnostics.Dispose();
                this.listenerFailures.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
