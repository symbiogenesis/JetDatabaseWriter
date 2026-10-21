namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Xunit;

/// <summary>Diagnostic capture follows each test's execution context, including asynchronous continuations.</summary>
public sealed class TestTraceScopeTests
{
    [Fact]
    public async Task ConcurrentScopes_CaptureOnlyTheirOwnMessages()
    {
        var firstReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<string> first = CaptureAsync("first", firstReady, secondReady.Task);
        Task<string> second = CaptureAsync("second", secondReady, firstReady.Task);

        Assert.Equal($"first{Environment.NewLine}first{Environment.NewLine}", await first);
        Assert.Equal($"second{Environment.NewLine}second{Environment.NewLine}", await second);
    }

    [Fact]
    public void NestedScope_DisposalRestoresOuterListener()
    {
        using var outerOutput = new StringWriter(CultureInfo.InvariantCulture);
        using var outerListener = new TextWriterTraceListener(outerOutput);
        using var outerScope = new TestTraceScope(outerListener);
        using var innerOutput = new StringWriter(CultureInfo.InvariantCulture);
        using var innerListener = new TextWriterTraceListener(innerOutput);
        using (var innerScope = new TestTraceScope(innerListener))
        {
            Trace.WriteLine("inner");
        }

        Trace.WriteLine("outer");
        Assert.Equal($"inner{Environment.NewLine}", innerOutput.ToString());
        Assert.Equal($"outer{Environment.NewLine}", outerOutput.ToString());
    }

    [Fact]
    public async Task ThrowingScope_DoesNotAffectOtherExecutionContexts()
    {
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(output);
        using var scope = new TestTraceScope(listener);
        await Task.Run(
            () =>
            {
                using var throwing = new ThrowingTraceListener();
                using var throwingScope = new TestTraceScope(throwing);
                Assert.Throws<IOException>(() => Trace.WriteLine("failure"));
            },
            TestContext.Current.CancellationToken);

        Trace.WriteLine("captured");
        Assert.Equal($"captured{Environment.NewLine}", output.ToString());
    }

    private static async Task<string> CaptureAsync(string message, TaskCompletionSource ready, Task otherReady)
    {
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        using var listener = new TextWriterTraceListener(output);
        using var scope = new TestTraceScope(listener);
        ready.SetResult();
        await otherReady.WaitAsync(TestContext.Current.CancellationToken);
        Trace.WriteLine(message);
        await Task.Yield();
        Trace.WriteLine(message);
        return output.ToString();
    }

    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Write(string? message) => throw new IOException("diagnostic failure");

        public override void WriteLine(string? message) => throw new IOException("diagnostic failure");
    }
}
