namespace JetDatabaseWriter.Infrastructure;

using System;
using JetDatabaseWriter.Verification;

internal static class BinaryBuffer
{
    internal static bool IsRange(long length, long start, long count)
        => length >= 0 && start >= 0 && count >= 0
            && RecoveryKernel.IsByteRange(checked((ulong)length), checked((ulong)start), checked((ulong)count));

    internal static byte[] CopySlice(byte[] buffer, int start, int length)
    {
        if (length <= 0)
        {
            return [];
        }

        Guard.NotNull(buffer, nameof(buffer));
        if (!IsRange(buffer.Length, start, length))
        {
            throw new ArgumentOutOfRangeException(nameof(start), "The requested slice exceeds the buffer.");
        }

        return buffer.AsSpan(start, length).ToArray();
    }
}
