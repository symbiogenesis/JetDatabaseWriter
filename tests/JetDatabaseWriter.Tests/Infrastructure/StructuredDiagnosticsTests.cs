namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>Diagnostic callbacks cannot recursively invoke themselves.</summary>
public sealed class StructuredDiagnosticsTests
{
    [Theory]
    [InlineData("read")]
    [InlineData("create")]
    [InlineData("detect")]
    public async Task DiagnosticListener_CannotOpenOrCreateDatabases(string operation)
    {
        await using var stream = new MemoryStream();
        Task? pending = null;
        using var listener = new CallbackListener(() => pending = operation switch
        {
            "read" => AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken).AsTask(),
            "create" => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken).AsTask(),
            _ => AccessDatabaseEncryption.DetectEncryptionFormatAsync(stream, TestContext.Current.CancellationToken).AsTask(),
        });
        using (var scope = new TestTraceScope(listener))
        {
            DiagnosticTrace.WriteLine("outer diagnostic");
        }

        Assert.NotNull(pending);
        JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(() => pending);
        Assert.Equal(JetErrorCode.ReentrantDiagnosticCall, failure.ErrorCode);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.Position);
    }

    [Fact(DisableParallelization = true)]
    public void UnsupportedType_DoesNotCountAsCorruption()
    {
        long before = JetFailureMetrics.Corruptions;
        _ = new JetNotSupportedException(JetErrorCode.UnknownColumnType, "unsupported type");
        Assert.Equal(before, JetFailureMetrics.Corruptions);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void DiagnosticListener_CannotReenterOperationOrDisposal(bool dispose)
    {
        var gate = new AsyncReentrantOperationGate(typeof(AccessReader));
        using AsyncReentrantOperationGate.Lease active = gate.Enter();
        Exception? failure = null;
        using var listener = new CallbackListener(() => failure = Record.Exception(() =>
        {
            if (dispose)
            {
                _ = gate.TryBeginDispose(out _);
            }
            else
            {
                using AsyncReentrantOperationGate.Lease nested = gate.Enter();
            }
        }));
        using var scope = new TestTraceScope(listener);
        DiagnosticTrace.WriteLine("outer diagnostic");
        Assert.Equal(JetErrorCode.ReentrantDiagnosticCall, Assert.IsType<JetOperationException>(failure).ErrorCode);
    }

    [Fact]
    public async Task OptionalCounters_ExposeOnlyAggregateMetrics()
    {
        using var listener = new CounterListener();
        EventSource source = JetDiagnostics.EnableCounters();
        listener.EnableEvents(source, EventLevel.LogAlways, EventKeywords.All, new Dictionary<string, string?> { ["EventCounterIntervalSec"] = "0.1" });
        long before = JetFailureMetrics.Corruptions;
        _ = new JetCorruptDataException(JetErrorCode.CorruptPage, "corrupt", new JetErrorInfo { PageNumber = 12 });
        Assert.True(JetFailureMetrics.Corruptions > before);
        await listener.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal("JetDatabaseWriter", source.Name);
    }

    [Fact]
    public void ListenerReentrancy_IsSuppressed()
    {
        using var listener = new ReentrantListener();
        using var scope = new TestTraceScope(listener);
        DiagnosticTrace.WriteLine("outer diagnostic");
        Assert.Equal(1, listener.Calls);
    }

    private sealed class ReentrantListener : TraceListener
    {
        internal int Calls { get; private set; }

        public override void Write(string? message)
        {
            this.Calls++;
            if (this.Calls < 3)
            {
                DiagnosticTrace.WriteLine("nested diagnostic");
            }
        }

        public override void WriteLine(string? message) => this.Write(message);
    }

    private sealed class CallbackListener(Action callback) : TraceListener
    {
        public override void Write(string? message) => callback();

        public override void WriteLine(string? message) => this.Write(message);
    }

    private sealed class CounterListener : EventListener
    {
        private readonly HashSet<string> names = [];

        internal TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            if (eventData.EventName == "EventCounters" && eventData.Payload?[0] is IDictionary<string, object> payload
                && payload["Name"] is string name)
            {
                _ = this.names.Add(name);
                if (this.names.SetEquals(["failures", "corruptions", "diagnostics", "listener-failures"]))
                {
                    this.Completed.TrySetResult();
                }
            }
        }
    }
}
