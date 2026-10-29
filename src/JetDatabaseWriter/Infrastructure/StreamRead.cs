namespace JetDatabaseWriter.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Verification;

internal static class StreamRead
{
    public static void ReadExactly(Stream stream, Span<byte> buffer)
    {
        Guard.NotNull(stream, nameof(stream));
        int totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int bytesRead = stream.Read(buffer[totalRead..]);
            if (bytesRead == 0)
            {
                throw new EndOfStreamException();
            }

            totalRead = AdvanceRead(buffer.Length, totalRead, bytesRead);
        }
    }

    public static async ValueTask ReadExactlyAsync(
        Stream stream,
        Memory<byte> buffer,
        CancellationToken cancellationToken = default) => _ = await ReadAtLeastAsync(
            stream,
            buffer,
            buffer.Length,
            throwOnEndOfStream: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);

    public static async ValueTask<int> ReadAtLeastAsync(
        Stream stream,
        Memory<byte> buffer,
        int minimumBytes,
        bool throwOnEndOfStream = true,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        if (!BinaryBuffer.IsRange(buffer.Length, 0, minimumBytes))
        {
            throw new ArgumentOutOfRangeException(nameof(minimumBytes));
        }

        int totalRead = 0;
        while (!RecoveryKernel.ReadMinimumSatisfied(checked((uint)buffer.Length), checked((uint)minimumBytes), checked((uint)totalRead)))
        {
            cancellationToken.ThrowIfCancellationRequested();
            int bytesRead = await stream.ReadAsync(buffer[totalRead..], cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                if (throwOnEndOfStream)
                {
                    throw new EndOfStreamException();
                }

                return totalRead;
            }

            totalRead = AdvanceRead(buffer.Length, totalRead, bytesRead);
        }

        return totalRead;
    }

    internal static int AdvanceRead(int length, int offset, int count)
    {
        if (count < 0)
        {
            throw new IOException("The stream returned an invalid read count.");
        }

        uint next = RecoveryKernel.BufferReadAdvance(checked((uint)length), checked((uint)offset), checked((uint)count));
        if (next == uint.MaxValue)
        {
            throw new IOException("The stream returned an invalid read count.");
        }

        return checked((int)next);
    }
}
