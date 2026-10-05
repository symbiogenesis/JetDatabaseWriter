namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading;

/// <summary>
/// A <see cref="MemoryStream"/> that, once armed, injects a fault at a chosen
/// point of a writer's page I/O, for tests that check how a writer recovers
/// when an operation is stopped partway: <see cref="FailOnWrite"/> throws
/// <see cref="IOException"/> from a chosen write, which changes nothing, and
/// <see cref="CancelAfterWrites"/> and <see cref="CancelAfterReads"/> cancel a
/// token once a chosen write or read has completed. Each arms one fault, and
/// every other write and read succeeds.
/// </summary>
internal sealed class WriteFaultStream : MemoryStream
{
    private int writesUntilFault;
    private int readsUntilFault;
    private bool partialWriteFault;
    private int flushesUntilFault;
    private bool persistentWriteFault;
    private int writesUntilCancel;
    private int readsUntilCancel;
    private CancellationTokenSource? cancellation;

    /// <summary>Gets the number of writes that have reached the stream.</summary>
    public int WriteCount { get; private set; }

    /// <summary>Gets the number of reads that have reached the stream.</summary>
    public int ReadCount { get; private set; }

    /// <summary>Gets a value indicating whether the armed fault has been raised.</summary>
    public bool Faulted { get; private set; }

    /// <summary>Arms the stream so its <paramref name="nthWrite"/>-th write from now throws.</summary>
    /// <param name="nthWrite">Which write from now fails, starting at 1.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="nthWrite"/> is not positive.</exception>
    public void FailOnWrite(int nthWrite)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nthWrite);
        this.writesUntilFault = nthWrite;
        this.Faulted = false;
    }

    /// <summary>Arms a one-shot read failure.</summary>
    /// <param name="nthRead">The read to fail, starting at one.</param>
    public void FailOnRead(int nthRead)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nthRead);
        this.readsUntilFault = nthRead;
        this.Faulted = false;
    }

    /// <summary>Arms a write failure after modifying half of its buffer.</summary>
    /// <param name="nthWrite">The write to fail, starting at one.</param>
    public void FailDuringWrite(int nthWrite)
    {
        this.FailOnWrite(nthWrite);
        this.partialWriteFault = true;
    }

    /// <summary>Arms a one-shot flush failure.</summary>
    /// <param name="nthFlush">The flush to fail, starting at one.</param>
    public void FailOnFlush(int nthFlush)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nthFlush);
        this.flushesUntilFault = nthFlush;
        this.Faulted = false;
    }

    /// <summary>Arms a write failure that also prevents undo writes.</summary>
    /// <param name="nthWrite">The first write to fail, starting at one.</param>
    public void FailWritesFrom(int nthWrite)
    {
        this.FailOnWrite(nthWrite);
        this.persistentWriteFault = true;
    }

    /// <inheritdoc/>
    public override System.Threading.Tasks.Task FlushAsync(CancellationToken cancellationToken)
    {
        if (this.flushesUntilFault > 0 && --this.flushesUntilFault == 0)
        {
            this.Faulted = true;
            throw new IOException("Injected flush fault.");
        }

        return base.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// Arms the stream so it cancels <paramref name="source"/> once its
    /// <paramref name="nthWrite"/>-th write from now has reached the stream.
    /// </summary>
    /// <param name="nthWrite">After which write from now the token is cancelled, starting at 1.</param>
    /// <param name="source">The token source to cancel.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="nthWrite"/> is not positive.</exception>
    public void CancelAfterWrites(int nthWrite, CancellationTokenSource source)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nthWrite);
        ArgumentNullException.ThrowIfNull(source);
        this.writesUntilCancel = nthWrite;
        this.readsUntilCancel = 0;
        this.cancellation = source;
        this.Faulted = false;
    }

    /// <summary>
    /// Arms the stream so it cancels <paramref name="source"/> once its
    /// <paramref name="nthRead"/>-th read from now has completed.
    /// </summary>
    /// <param name="nthRead">After which read from now the token is cancelled, starting at 1.</param>
    /// <param name="source">The token source to cancel.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="nthRead"/> is not positive.</exception>
    public void CancelAfterReads(int nthRead, CancellationTokenSource source)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(nthRead);
        ArgumentNullException.ThrowIfNull(source);
        this.readsUntilCancel = nthRead;
        this.writesUntilCancel = 0;
        this.cancellation = source;
        this.Faulted = false;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A derived <see cref="MemoryStream"/> routes the span and async write
    /// overloads through this one, so every page write is counted once.
    /// </remarks>
    public override void Write(byte[] buffer, int offset, int count)
    {
        try
        {
            this.CountWrite();
        }
        catch (IOException) when (this.partialWriteFault)
        {
            this.partialWriteFault = false;
            base.Write(buffer, offset, count / 2);
            throw;
        }

        base.Write(buffer, offset, count);
        this.CancelIfDue(ref this.writesUntilCancel);
    }

    /// <inheritdoc/>
    public override void WriteByte(byte value)
    {
        this.CountWrite();
        base.WriteByte(value);
        this.CancelIfDue(ref this.writesUntilCancel);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A derived <see cref="MemoryStream"/> routes the span and async read
    /// overloads through this one, so every page read is counted once.
    /// </remarks>
    public override int Read(byte[] buffer, int offset, int count)
    {
        if (this.readsUntilFault > 0 && --this.readsUntilFault == 0)
        {
            this.Faulted = true;
            throw new IOException("Injected read fault.");
        }

        int read = base.Read(buffer, offset, count);
        this.ReadCount++;
        this.CancelIfDue(ref this.readsUntilCancel);
        return read;
    }

    /// <inheritdoc/>
    public override int ReadByte()
    {
        int value = base.ReadByte();
        this.ReadCount++;
        this.CancelIfDue(ref this.readsUntilCancel);
        return value;
    }

    private void CountWrite()
    {
        this.WriteCount++;
        if ((this.persistentWriteFault && this.Faulted) || (this.writesUntilFault > 0 && --this.writesUntilFault == 0))
        {
            this.Faulted = true;
            throw new IOException("Injected write fault.");
        }
    }

    private void CancelIfDue(ref int remaining)
    {
        if (remaining > 0 && --remaining == 0)
        {
            this.Faulted = true;
            this.cancellation?.Cancel();
        }
    }
}
