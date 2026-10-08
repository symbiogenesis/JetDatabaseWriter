namespace JetDatabaseWriter.Relationships;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Owns a forward-only text source and bounds bytes delivered to its decoder.</summary>
/// <param name="source">The opened source stream.</param>
/// <param name="maxBytes">The maximum source bytes, or null for unlimited consumption.</param>
/// <param name="tableName">The linked table used in size-limit errors.</param>
internal sealed class LinkedTextSourceStream(Stream source, long? maxBytes, string tableName) : Stream
{
    private long consumed;
    private bool exceeded;

    public override bool CanRead => source.CanRead;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => source.Length;

    public override long Position
    {
        get => this.consumed;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => this.Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        int read = source.Read(buffer[..this.GetReadLength(buffer.Length)]);
        this.RecordRead(read);
        return read;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        int read = await source.ReadAsync(buffer[..this.GetReadLength(buffer.Length)], cancellationToken).ConfigureAwait(false);
        this.RecordRead(read);
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        this.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() => source.Flush();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            source.Dispose();
        }

        base.Dispose(disposing);
    }

    private int GetReadLength(int requested)
    {
        if (this.exceeded)
        {
            this.ThrowSizeLimitExceeded();
        }

        if (!maxBytes.HasValue)
        {
            return requested;
        }

        long remaining = maxBytes.Value - this.consumed;

        // Read at most one excess byte to distinguish EOF from a growing source.
        return remaining < requested ? checked((int)(remaining + 1)) : requested;
    }

    private void RecordRead(int read)
    {
        if (maxBytes.HasValue && read > maxBytes.Value - this.consumed)
        {
            this.exceeded = true;
            this.ThrowSizeLimitExceeded();
        }

        this.consumed += read;
    }

    private void ThrowSizeLimitExceeded() => throw new InvalidDataException(
        $"Linked text table '{tableName}' source file exceeds AccessReaderOptions.{nameof(AccessReaderOptions.LinkedTextMaxSourceFileBytes)} ({maxBytes}).");
}
