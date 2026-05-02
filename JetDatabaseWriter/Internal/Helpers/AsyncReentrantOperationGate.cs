namespace JetDatabaseWriter.Internal.Helpers;

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

    private readonly AsyncLocal<int> _operationDepth = new();
    private readonly object _stateLock = new();
    private readonly TaskCompletionSource<object?> _disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<object?>? _operationsDrained;
    private int _activeOperations;
    private int _state;

    public Task DisposeCompleted => _disposeCompleted.Task;

    public Lease Enter(object owner)
    {
        int depth = _operationDepth.Value;
        if (depth > 0)
        {
            _operationDepth.Value = depth + 1;
            return new Lease(this, isRoot: false);
        }

        if (Volatile.Read(ref _state) != StateOpen)
        {
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        _ = Interlocked.Increment(ref _activeOperations);

        if (Volatile.Read(ref _state) != StateOpen)
        {
            ReleaseActiveOperation();
            throw new ObjectDisposedException(owner?.GetType().FullName);
        }

        _operationDepth.Value = 1;
        return new Lease(this, isRoot: true);
    }

    public bool TryBeginDispose(out Task waitForOperations)
    {
        if (Interlocked.CompareExchange(ref _state, StateDisposing, StateOpen) != StateOpen)
        {
            waitForOperations = _disposeCompleted.Task;
            return false;
        }

        lock (_stateLock)
        {
            _operationsDrained = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (Volatile.Read(ref _activeOperations) == 0)
            {
                _operationsDrained.TrySetResult(null);
            }

            waitForOperations = _operationsDrained.Task;
            return true;
        }
    }

    public void CompleteDispose(Exception? error = null)
    {
        if (error == null)
        {
            _disposeCompleted.TrySetResult(null);
        }
        else
        {
            _disposeCompleted.TrySetException(error);
        }

        Volatile.Write(ref _state, StateDisposed);
    }

    private void ReleaseOperation(bool isRoot)
    {
        int depth = _operationDepth.Value;
        _operationDepth.Value = depth > 0 ? depth - 1 : 0;

        if (!isRoot)
        {
            return;
        }

        ReleaseActiveOperation();
    }

    private void ReleaseActiveOperation()
    {
        if (Interlocked.Decrement(ref _activeOperations) != 0)
        {
            return;
        }

        TaskCompletionSource<object?>? drained;
        lock (_stateLock)
        {
            drained = _operationsDrained;
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