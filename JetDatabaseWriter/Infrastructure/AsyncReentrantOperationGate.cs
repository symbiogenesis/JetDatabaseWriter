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

    public Task DisposeCompleted => disposeCompleted.Task;

    public Lease Enter(object owner)
    {
        int depth = operationDepth.Value;
        if (depth > 0)
        {
            operationDepth.Value = depth + 1;
            return new Lease(this, isRoot: false);
        }

        if (Volatile.Read(ref state) != StateOpen)
        {
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        _ = Interlocked.Increment(ref activeOperations);

        if (Volatile.Read(ref state) != StateOpen)
        {
            ReleaseActiveOperation();
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        operationDepth.Value = 1;
        return new Lease(this, isRoot: true);
    }

    public bool TryBeginDispose(out Task waitForOperations)
    {
        if (Interlocked.CompareExchange(ref state, StateDisposing, StateOpen) != StateOpen)
        {
            waitForOperations = disposeCompleted.Task;
            return false;
        }

        lock (stateLock)
        {
            operationsDrained = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (Volatile.Read(ref activeOperations) == 0)
            {
                operationsDrained.TrySetResult(null);
            }

            waitForOperations = operationsDrained.Task;
            return true;
        }
    }

    public void CompleteDispose(Exception? error = null)
    {
        if (error == null)
        {
            disposeCompleted.TrySetResult(null);
        }
        else
        {
            disposeCompleted.TrySetException(error);
        }

        Volatile.Write(ref state, StateDisposed);
    }

    private void ReleaseOperation(bool isRoot)
    {
        int depth = operationDepth.Value;
        operationDepth.Value = depth > 0 ? depth - 1 : 0;

        if (!isRoot)
        {
            return;
        }

        ReleaseActiveOperation();
    }

    private void ReleaseActiveOperation()
    {
        if (Interlocked.Decrement(ref activeOperations) != 0)
        {
            return;
        }

        TaskCompletionSource<object?>? drained;
        lock (stateLock)
        {
            drained = operationsDrained;
        }

        drained?.TrySetResult(null);
    }

    internal readonly struct Lease(AsyncReentrantOperationGate owner, bool isRoot) : IDisposable
    {
        private readonly AsyncReentrantOperationGate _owner = owner;
        private readonly bool _isRoot = isRoot;

        public void Dispose() => _owner.ReleaseOperation(_isRoot);
    }
}
