namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics;
using System.Threading;

/// <summary>Routes trace messages to a listener owned by the current test without changing global listeners per test.</summary>
internal sealed class TestTraceScope : IDisposable
{
    private static readonly AsyncLocal<TraceListener?> CurrentListener = new();

    private readonly TraceListener? previous;

    /// <summary>Initializes static members of the <see cref="TestTraceScope"/> class.</summary>
    /// <remarks>The routing listener belongs to the test process; scopes leave their supplied listeners with the caller.</remarks>
    static TestTraceScope() => Trace.Listeners.Add(new RoutingTraceListener());

    internal TestTraceScope(TraceListener listener)
    {
        this.previous = CurrentListener.Value;
        CurrentListener.Value = listener;
    }

    public void Dispose() => CurrentListener.Value = this.previous;

    private sealed class RoutingTraceListener : TraceListener
    {
        public override void Write(string? message) => CurrentListener.Value?.Write(message);

        public override void WriteLine(string? message) => CurrentListener.Value?.WriteLine(message);
    }
}
