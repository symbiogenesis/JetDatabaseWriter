namespace JetDatabaseWriter.Internal.Helpers;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// A <see cref="Stream"/> wrapper that delegates all I/O to an inner stream
/// but suppresses <see cref="Dispose(bool)"/> and <see cref="DisposeAsync"/>.
/// Used to implement the <c>leaveOpen</c> pattern so the caller retains
/// ownership of the underlying stream.
/// </summary>
internal sealed class NonClosingStreamWrapper(Stream inner) : Stream
{
    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => inner.CanSeek;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => inner.Length;

    public override long Position
    {
        get => inner.Position;
        set => inner.Position = value;
    }

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.ReadAsync(buffer, cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        inner.ReadAsync(buffer, offset, count, cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

    public override void SetLength(long value) => inner.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.WriteAsync(buffer, cancellationToken);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        inner.WriteAsync(buffer, offset, count, cancellationToken);

#pragma warning disable CA2215 // Intentionally not calling base — the purpose of this wrapper is to suppress disposal

    public override ValueTask DisposeAsync()
    {
        // Intentionally do not dispose the inner stream.
        return default;
    }

    protected override void Dispose(bool disposing)
    {
        // Intentionally do not dispose the inner stream.
    }

#pragma warning restore CA2215
}
