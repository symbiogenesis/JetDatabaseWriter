#if !NET7_0_OR_GREATER
namespace JetDatabaseWriter.Infrastructure;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

internal static class StreamReadExtensions
{
    public static async ValueTask ReadExactlyAsync(
        this Stream stream,
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        _ = await stream.ReadAtLeastAsync(
            buffer,
            buffer.Length,
            throwOnEndOfStream: true,
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public static async ValueTask<int> ReadAtLeastAsync(
        this Stream stream,
        Memory<byte> buffer,
        int minimumBytes,
        bool throwOnEndOfStream = true,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(stream, nameof(stream));
        if ((uint)minimumBytes > (uint)buffer.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(minimumBytes));
        }

        int totalRead = 0;
        while (totalRead < minimumBytes)
        {
            int bytesRead = await stream.ReadAsync(buffer.Slice(totalRead), cancellationToken).ConfigureAwait(false);
            if (bytesRead == 0)
            {
                if (throwOnEndOfStream)
                {
                    throw new EndOfStreamException();
                }

                return totalRead;
            }

            totalRead += bytesRead;
        }

        return totalRead;
    }
}
#endif
