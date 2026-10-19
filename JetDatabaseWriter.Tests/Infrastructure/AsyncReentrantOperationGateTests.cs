namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;
using Xunit;

/// <summary>Checks operation draining and disposal races.</summary>
public sealed class AsyncReentrantOperationGateTests
{
    /// <summary>Disposal drains the active root, including calls nested after disposal begins.</summary>
    [Fact]
    public async Task Disposal_WaitsForRoot_AndAllowsItsNestedCalls()
    {
        var gate = new AsyncReentrantOperationGate(typeof(AccessReader));
        AsyncReentrantOperationGate.Lease root = gate.Enter();
        Assert.True(gate.TryBeginDispose(out Task drained));
        Assert.False(drained.IsCompleted);

        using (gate.Enter())
        {
            Assert.False(drained.IsCompleted);
        }

        Assert.False(drained.IsCompleted);
        root.Dispose();
        await drained.WaitAsync(TestContext.Current.CancellationToken);
        Assert.Throws<ObjectDisposedException>(() => gate.Enter());
        Assert.False(gate.TryBeginDispose(out Task completed));
        Assert.Same(gate.DisposeCompleted, completed);
        Assert.False(completed.IsCompleted);
        gate.CompleteDispose();
        await completed.WaitAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>An idle gate drains immediately and shares disposal failures with subsequent callers.</summary>
    [Fact]
    public async Task IdleDisposal_CompletesDrain_AndSharesFailure()
    {
        var gate = new AsyncReentrantOperationGate(typeof(AccessReader));
        Assert.True(gate.TryBeginDispose(out Task drained));
        Assert.True(drained.IsCompletedSuccessfully);
        var failure = new InvalidOperationException("Teardown failed.");
        gate.CompleteDispose(failure);
        Assert.False(gate.TryBeginDispose(out Task completed));
        InvalidOperationException observed = await Assert.ThrowsAsync<InvalidOperationException>(() => completed);
        Assert.Same(failure, observed);
        Assert.Throws<ObjectDisposedException>(() => gate.Enter());
    }

    /// <summary>Concurrent root completion cannot lose the disposal drain notification.</summary>
    [Fact]
    public async Task Disposal_RacingFinalOperation_AlwaysDrains()
    {
        for (int iteration = 0; iteration < 128; iteration++)
        {
            var gate = new AsyncReentrantOperationGate(typeof(AccessReader));
            AsyncReentrantOperationGate.Lease root = gate.Enter();
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var dispose = Task.Run(
                async () =>
                {
                    await start.Task;
                    Assert.True(gate.TryBeginDispose(out Task drained));
                    await drained.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
                    gate.CompleteDispose();
                },
                TestContext.Current.CancellationToken);
            var release = Task.Run(
                async () =>
                {
                    await start.Task;
                    root.Dispose();
                },
                TestContext.Current.CancellationToken);
            start.SetResult();
            await Task.WhenAll(dispose, release);
            await gate.DisposeCompleted.WaitAsync(TestContext.Current.CancellationToken);
        }
    }
}
