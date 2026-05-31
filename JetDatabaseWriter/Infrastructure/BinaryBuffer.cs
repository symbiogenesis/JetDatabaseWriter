namespace JetDatabaseWriter.Infrastructure;

using System;

internal static class BinaryBuffer
{
    internal static byte[] CopySlice(byte[] buffer, int start, int length) => length <= 0 ? [] : buffer.AsSpan(start, length).ToArray();

    internal static byte[] CopyTail(byte[] buffer, int start) => buffer.AsSpan(start).ToArray();
}
