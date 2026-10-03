namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A pass-through stream that counts the bytes read from and written to it
/// and records where each read started, so a test can check how much of a
/// database an operation read and which pages it touched. The inner stream
/// belongs to the test: disposing this stream only records the dispose.
/// </summary>
/// <param name="inner">The stream that holds the bytes.</param>
internal sealed class CountingStream(Stream inner) : Stream
{
    private readonly Lock gate = new();
    private readonly List<(long Offset, int Length)> reads = [];
    private long bytesRead;
    private long bytesWritten;

    /// <summary>Gets the number of bytes read since the stream was created or last <see cref="Reset"/>.</summary>
    public long BytesRead
    {
        get
        {
            lock (this.gate)
            {
                return this.bytesRead;
            }
        }
    }

    /// <summary>Gets the number of bytes written since the stream was created or last <see cref="Reset"/>; a <see cref="SetLength"/> call counts as one.</summary>
    public long BytesWritten
    {
        get
        {
            lock (this.gate)
            {
                return this.bytesWritten;
            }
        }
    }

    /// <summary>Gets a value indicating whether the stream has been disposed.</summary>
    public bool IsDisposed { get; private set; }

    /// <inheritdoc/>
    public override bool CanRead => !this.IsDisposed && inner.CanRead;

    /// <inheritdoc/>
    public override bool CanSeek => !this.IsDisposed && inner.CanSeek;

    /// <inheritdoc/>
    public override bool CanWrite => !this.IsDisposed && inner.CanWrite;

    /// <inheritdoc/>
    public override long Length => inner.Length;

    /// <inheritdoc/>
    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    /// <summary>
    /// Returns the page numbers that the reads since the stream was created or
    /// last <see cref="Reset"/> touched, counting every page a read overlaps.
    /// </summary>
    /// <param name="pageSize">The database page size in bytes.</param>
    /// <returns>The page numbers read.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="pageSize"/> is not positive.</exception>
    public HashSet<long> PagesRead(int pageSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);

        var pages = new HashSet<long>();
        lock (this.gate)
        {
            foreach ((long offset, int length) in this.reads)
            {
                for (long page = offset / pageSize; page <= (offset + length - 1) / pageSize; page++)
                {
                    _ = pages.Add(page);
                }
            }
        }

        return pages;
    }

    /// <summary>Clears the byte counts and the recorded reads.</summary>
    public void Reset()
    {
        lock (this.gate)
        {
            this.bytesRead = 0;
            this.bytesWritten = 0;
            this.reads.Clear();
        }
    }

    /// <inheritdoc/>
    public override void Flush() => inner.Flush();

    /// <inheritdoc/>
    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        long position = inner.Position;
        int n = inner.Read(buffer, offset, count);
        this.RecordRead(position, n);
        return n;
    }

    /// <inheritdoc/>
    public override int Read(Span<byte> buffer)
    {
        long position = inner.Position;
        int n = inner.Read(buffer);
        this.RecordRead(position, n);
        return n;
    }

    /// <inheritdoc/>
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        long position = inner.Position;
        int n = await inner.ReadAsync(buffer, cancellationToken);
        this.RecordRead(position, n);
        return n;
    }

    /// <inheritdoc/>
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        this.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc/>
    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    /// <inheritdoc/>
    public override void SetLength(long value)
    {
        this.RecordWrite(1);
        inner.SetLength(value);
    }

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count)
    {
        this.RecordWrite(count);
        inner.Write(buffer, offset, count);
    }

    /// <inheritdoc/>
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        this.RecordWrite(buffer.Length);
        inner.Write(buffer);
    }

    /// <inheritdoc/>
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        this.RecordWrite(buffer.Length);
        return inner.WriteAsync(buffer, cancellationToken);
    }

    /// <inheritdoc/>
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        this.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    /// <inheritdoc/>
    protected override void Dispose(bool disposing)
    {
        this.IsDisposed = true;
        base.Dispose(disposing);
    }

    private void RecordRead(long offset, int length)
    {
        if (length <= 0)
        {
            return;
        }

        lock (this.gate)
        {
            this.bytesRead += length;
            this.reads.Add((offset, length));
        }
    }

    private void RecordWrite(long length)
    {
        lock (this.gate)
        {
            this.bytesWritten += length;
        }
    }
}
