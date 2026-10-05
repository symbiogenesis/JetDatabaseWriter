namespace JetDatabaseWriter.TestSupport;

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
/// <param name="pageSize">The page size used for the type histogram.</param>
internal class PageTraceStream(Stream inner, int pageSize = 4096) : Stream
{
#if NET9_0_OR_GREATER
    private readonly System.Threading.Lock gate = new();
#else
    private readonly object gate = new();
#endif
    private readonly List<(long Offset, int Length)> reads = [];
    private readonly List<(long Offset, int Length)> writes = [];
    private readonly Dictionary<byte, long> pageTypes = [];
    private long flushes;
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

    /// <summary>Gets the number of flush calls.</summary>
    public long Flushes
    {
        get
        {
            lock (this.gate)
            {
                return this.flushes;
            }
        }
    }

    /// <summary>Gets a snapshot of full-page read counts by page type.</summary>
    public IReadOnlyDictionary<byte, long> PageTypeHistogram
    {
        get
        {
            lock (this.gate)
            {
                return new Dictionary<byte, long>(this.pageTypes);
            }
        }
    }

    /// <summary>Gets read counts by physical page number.</summary>
    /// <param name="size">The page size.</param>
    /// <returns>The counts.</returns>
    public IReadOnlyDictionary<long, long> ReadCounts(int size) => this.CountPages(this.reads, size);

    /// <summary>Gets write counts by physical page number.</summary>
    /// <param name="size">The page size.</param>
    /// <returns>The counts.</returns>
    public IReadOnlyDictionary<long, long> WriteCounts(int size) => this.CountPages(this.writes, size);

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
            this.writes.Clear();
            this.pageTypes.Clear();
            this.flushes = 0;
        }
    }

    /// <inheritdoc/>
    public override void Flush()
    {
        this.RecordFlush();
        inner.Flush();
    }

    /// <inheritdoc/>
    public override Task FlushAsync(CancellationToken cancellationToken)
    {
        this.RecordFlush();
        return inner.FlushAsync(cancellationToken);
    }

    /// <inheritdoc/>
    public override int Read(byte[] buffer, int offset, int count)
    {
        long position = inner.Position;
        int n = inner.Read(buffer, offset, count);
        this.RecordRead(position, n);
        this.RecordPageType(position, buffer.AsSpan(offset, n), n);
        return n;
    }

    /// <inheritdoc/>
    public override int Read(Span<byte> buffer)
    {
        long position = inner.Position;
        int n = inner.Read(buffer);
        this.RecordRead(position, n);
        this.RecordPageType(position, buffer, n);
        return n;
    }

    /// <inheritdoc/>
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        long position = inner.Position;
        int n = await inner.ReadAsync(buffer, cancellationToken);
        this.RecordRead(position, n);
        this.RecordPageType(position, buffer.Span, n);
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
        this.RecordWrite(0, 1);
        inner.SetLength(value);
    }

    /// <inheritdoc/>
    public override void Write(byte[] buffer, int offset, int count)
    {
        this.RecordWrite(inner.Position, count);
        inner.Write(buffer, offset, count);
    }

    /// <inheritdoc/>
    public override void Write(ReadOnlySpan<byte> buffer)
    {
        this.RecordWrite(inner.Position, buffer.Length);
        inner.Write(buffer);
    }

    /// <inheritdoc/>
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        this.RecordWrite(inner.Position, buffer.Length);
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

    private void RecordWrite(long offset, int length)
    {
        lock (this.gate)
        {
            this.bytesWritten += length;
            this.writes.Add((offset, length));
        }
    }

    private Dictionary<long, long> CountPages(List<(long Offset, int Length)> operations, int size)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(size);
        var counts = new Dictionary<long, long>();
        lock (this.gate)
        {
            foreach ((long offset, int length) in operations)
            {
                for (long page = offset / size; page <= (offset + length - 1) / size; page++)
                {
                    counts.TryGetValue(page, out long count);
                    counts[page] = count + 1;
                }
            }
        }

        return counts;
    }

    private void RecordFlush()
    {
        lock (this.gate)
        {
            this.flushes++;
        }
    }

    private void RecordPageType(long position, ReadOnlySpan<byte> buffer, int length)
    {
        if (length < pageSize || position % pageSize != 0)
        {
            return;
        }

        lock (this.gate)
        {
            for (int offset = 0; offset + pageSize <= length; offset += pageSize)
            {
                this.pageTypes.TryGetValue(buffer[offset], out long count);
                this.pageTypes[buffer[offset]] = count + 1;
            }
        }
    }
}
