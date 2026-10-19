namespace JetDatabaseWriter.Infrastructure;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Coordinates top-level operations against asynchronous disposal.
/// Nested calls on the same async flow are treated as part of the active root operation,
/// while new top-level operations are rejected once disposal begins.
/// </summary>
/// <param name="ownerType">The public type named by the <see cref="ObjectDisposedException"/> raised after disposal.</param>
/// <param name="attachmentReads">The optional attachment budget context shared by linked readers.</param>
internal sealed class AsyncReentrantOperationGate(Type ownerType, AttachmentReadContext? attachmentReads = null)
{
    private const int StateOpen = 0;
    private const int StateDisposing = 1;
    private const int StateDisposed = 2;

    private readonly AsyncLocal<int> operationDepth = new();
    private readonly TaskCompletionSource<object?> disposeCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private TaskCompletionSource<object?>? operationsDrained;
    private int activeOperations;
    private int state;

    public Task DisposeCompleted => this.disposeCompleted.Task;

    public Lease Enter()
    {
        int depth = this.operationDepth.Value;
        if (depth > 0)
        {
            this.operationDepth.Value = depth + 1;
            return new Lease(this, isRoot: false, depth + 1, attachmentReads?.Enter() ?? default);
        }

        if (!this.TryEnterRootOperation())
        {
            Guard.ThrowIfDisposed(disposed: true, ownerType);
        }

        this.operationDepth.Value = 1;
        return new Lease(this, isRoot: true, 1, attachmentReads?.Enter() ?? default);
    }

    public bool TryBeginDispose(out Task waitForOperations)
    {
        if (Interlocked.CompareExchange(ref this.state, StateDisposing, StateOpen) != StateOpen)
        {
            waitForOperations = this.disposeCompleted.Task;
            return false;
        }

        var drained = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        // The full fence pairs publication with the final operation's decrement,
        // so one of them observes and completes the drain.
        _ = Interlocked.Exchange(ref this.operationsDrained, drained);
        if (Volatile.Read(ref this.activeOperations) == 0)
        {
            drained.TrySetResult(null);
        }

        waitForOperations = drained.Task;
        return true;
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

    private bool TryEnterRootOperation()
    {
        if (Volatile.Read(ref this.state) != StateOpen)
        {
            return false;
        }

        _ = Interlocked.Increment(ref this.activeOperations);

        if (Volatile.Read(ref this.state) == StateOpen)
        {
            return true;
        }

        this.ReleaseActiveOperation();
        return false;
    }

    private void ReleaseActiveOperation()
    {
        if (Interlocked.Decrement(ref this.activeOperations) != 0)
        {
            return;
        }

        Volatile.Read(ref this.operationsDrained)?.TrySetResult(null);
    }

    internal readonly struct Lease(AsyncReentrantOperationGate owner, bool isRoot, int depth, AttachmentReadContext.Lease attachmentRead) : IDisposable
    {
        /// <summary>Restores this operation's flow context after an iterator yield.</summary>
        internal void Resume()
        {
            owner.operationDepth.Value = depth;
            attachmentRead.Resume();
        }

        public void Dispose()
        {
            attachmentRead.Dispose();
            owner.ReleaseOperation(isRoot);
        }
    }
}
