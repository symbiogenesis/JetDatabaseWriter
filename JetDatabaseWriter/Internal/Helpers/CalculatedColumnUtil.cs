namespace JetDatabaseWriter.Internal.Helpers;

using System;
using System.Buffers.Binary;
using static JetDatabaseWriter.Constants;

/// <summary>
/// Wrap / unwrap helpers for the 23-byte on-disk envelope every Access 2010+
/// calculated-column value carries. Translated from Jackcess
/// <c>CalculatedColumnUtil</c> (<c>wrapCalculatedValue</c> / <c>unwrapCalculatedValue</c>);
/// see <c>docs/design/calculated-columns-format-notes.md</c> for the layout.
/// </summary>
/// <remarks>
/// Per Jackcess: bytes 0..15 of the wrapper are reserved (Access uses them for
/// version + CRC + scratch; we emit zeros and ignore them on read), bytes 16..19
/// hold the little-endian uint32 payload length, and bytes 20.. carry the actual
/// value encoded per the column's result type. The reserved region is part of
/// the persisted format and the 23-byte total is invariant across all result
/// types; only the trailing payload length differs.
/// </remarks>
internal static class CalculatedColumnUtil
{
    /// <summary>
    /// Returns a fresh byte array containing <paramref name="payload"/> wrapped
    /// in the 23-byte calculated-value envelope. <paramref name="payload"/>
    /// must already be encoded per the column's result type (the caller is
    /// responsible for that encoding \u2014 e.g. UCS-2 LE for <c>T_TEXT</c>,
    /// little-endian int32 for <c>T_LONG</c>).
    /// </summary>
    public static byte[] Wrap(byte[] payload)
    {
        Guard.NotNull(payload, nameof(payload));
        var wrapped = new byte[CalculatedColumn.ExtraDataLen + payload.Length];
        BinaryPrimitives.WriteInt32LittleEndian(wrapped.AsSpan(CalculatedColumn.DataLenOffset, 4), payload.Length);
        Buffer.BlockCopy(payload, 0, wrapped, CalculatedColumn.DataOffset, payload.Length);
        return wrapped;
    }

    /// <summary>
    /// Strips the 23-byte calculated-value envelope and returns the inner
    /// payload bytes. Returns <paramref name="data"/> unchanged when shorter
    /// than the wrapper header (defensive parity with Jackcess).
    /// </summary>
    public static byte[] Unwrap(byte[] data)
    {
        Guard.NotNull(data, nameof(data));
        if (data.Length < CalculatedColumn.DataOffset)
        {
            return data;
        }

        int dataLen = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(CalculatedColumn.DataLenOffset, 4));
        int available = data.Length - CalculatedColumn.DataOffset;
        int copyLen = Math.Max(0, Math.Min(available, dataLen));
        var unwrapped = new byte[copyLen];
        Buffer.BlockCopy(data, CalculatedColumn.DataOffset, unwrapped, 0, copyLen);
        return unwrapped;
    }
}
