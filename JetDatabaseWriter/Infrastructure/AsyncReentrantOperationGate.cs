namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Coordinates top-level operations against asynchronous disposal.
/// Nested calls on the same async flow are treated as part of the active root operation,
/// while new top-level operations are rejected once disposal begins.
/// </summary>
internal sealed class AsyncReentrantOperationGate
{
    private const int StateOpen = 0;
    private const int StateDisposing = 1;
    private const int StateDisposed = 2;

    private readonly AsyncLocal<int> operationDepth = new();
#if NET8_0_OR_GREATER
    private readonly Lock stateLock = new();
#else
    private readonly object stateLock = new();
#endif

    private readonly TaskCompletionSource<object?> disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<object?>? operationsDrained;
    private int activeOperations;
    private int state;

    public Task DisposeCompleted => this.disposeCompleted.Task;

    public Lease Enter(object owner)
    {
        int depth = this.operationDepth.Value;
        if (depth > 0)
        {
            this.operationDepth.Value = depth + 1;
            return new Lease(this, isRoot: false);
        }

        if (Volatile.Read(ref this.state) != StateOpen)
        {
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        _ = Interlocked.Increment(ref this.activeOperations);

        if (Volatile.Read(ref this.state) != StateOpen)
        {
            this.ReleaseActiveOperation();
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        this.operationDepth.Value = 1;
        return new Lease(this, isRoot: true);
    }

    public bool TryBeginDispose(out Task waitForOperations)
    {
        if (Interlocked.CompareExchange(ref this.state, StateDisposing, StateOpen) != StateOpen)
        {
            waitForOperations = this.disposeCompleted.Task;
            return false;
        }

        lock (this.stateLock)
        {
            this.operationsDrained = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (Volatile.Read(ref this.activeOperations) == 0)
            {
                this.operationsDrained.TrySetResult(null);
            }

            waitForOperations = this.operationsDrained.Task;
            return true;
        }
    }

    public void CompleteDispose(Exception? error = null)
    {
        if (error == null)
        {
            this.disposeCompleted.TrySetResult(null);
        }
        else
        {
            this.disposeCompleted.TrySetException(error);
        }

        Volatile.Write(ref this.state, StateDisposed);
    }

    private void ReleaseOperation(bool isRoot)
    {
        int depth = this.operationDepth.Value;
        this.operationDepth.Value = depth > 0 ? depth - 1 : 0;

        if (!isRoot)
        {
            return;
        }

        this.ReleaseActiveOperation();
    }

    private void ReleaseActiveOperation()
    {
        if (Interlocked.Decrement(ref this.activeOperations) != 0)
        {
            return;
        }

        TaskCompletionSource<object?>? drained;
        lock (this.stateLock)
        {
            drained = this.operationsDrained;
        }

        drained?.TrySetResult(null);
    }

    internal readonly struct Lease(AsyncReentrantOperationGate owner, bool isRoot) : IDisposable
    {
        public void Dispose() => owner.ReleaseOperation(isRoot);
    }
}
