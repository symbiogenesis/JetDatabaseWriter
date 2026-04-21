namespace JetDatabaseReader;

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
internal sealed class NonClosingStreamWrapper : Stream
{
    private readonly Stream _inner;

    public NonClosingStreamWrapper(Stream inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public override bool CanRead => _inner.CanRead;

    public override bool CanSeek => _inner.CanSeek;

    public override bool CanWrite => _inner.CanWrite;

    public override long Length => _inner.Length;

    public override long Position
    {
        get => _inner.Position;
        set => _inner.Position = value;
    }

    public override void Flush() => _inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _inner.ReadAsync(buffer, cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        _inner.ReadAsync(buffer, offset, count, cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

    public override void SetLength(long value) => _inner.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        _inner.WriteAsync(buffer, cancellationToken);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        _inner.WriteAsync(buffer, offset, count, cancellationToken);

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
