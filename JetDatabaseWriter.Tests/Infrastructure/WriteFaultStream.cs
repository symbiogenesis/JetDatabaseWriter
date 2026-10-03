namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;

/// <summary>
/// A <see cref="MemoryStream"/> that throws <see cref="IOException"/> once,
/// on a chosen write after it is armed, for tests that check how a writer
/// recovers when a page write fails partway through an operation. The failed
/// write changes nothing; every later write succeeds.
/// </summary>
internal sealed class WriteFaultStream : MemoryStream
{
    private int writesUntilFault;

    /// <summary>Gets the number of writes that have reached the stream.</summary>
    public int WriteCount { get; private set; }

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

    /// <inheritdoc/>
    /// <remarks>
    /// A derived <see cref="MemoryStream"/> routes the span and async write
    /// overloads through this one, so every page write is counted once.
    /// </remarks>
    public override void Write(byte[] buffer, int offset, int count)
    {
        this.CountWrite();
        base.Write(buffer, offset, count);
    }

    /// <inheritdoc/>
    public override void WriteByte(byte value)
    {
        this.CountWrite();
        base.WriteByte(value);
    }

    private void CountWrite()
    {
        this.WriteCount++;
        if (this.writesUntilFault > 0 && --this.writesUntilFault == 0)
        {
            this.Faulted = true;
            throw new IOException("Injected write fault.");
        }
    }
}
