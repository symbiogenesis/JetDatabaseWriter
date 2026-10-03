namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Collections.Generic;
using Xunit;

/// <summary>
/// Reads and builds Jet3 (Access 97) row trailers independently of the library,
/// so tests can check the bytes the writer stores and feed the reader rows
/// laid out as Access lays them out. A Jet3 row stores the EOD and every
/// variable-column offset in one byte each; a row longer than 256 bytes adds
/// a jump table of <c>(rowLength - 1) / 256</c> one-byte entries between the
/// offset table and <c>var_len</c>. Each entry is the index of the first
/// offset (the EOD counting as index <c>var_len</c>) at or past its 256-byte
/// boundary, and an entry whose boundary no offset reaches is a dummy. The
/// reader is a port of Jackcess <c>TableImpl.readJumpTableVarColOffsets</c>,
/// which mdbtools' <c>mdb_crack_row3</c> matches.
/// </summary>
internal static class Jet3RowTrailerReference
{
    /// <summary>
    /// Decodes the variable-column offsets of a Jet3 row the way Jackcess does.
    /// </summary>
    /// <param name="row">The whole row, from <c>num_cols</c> to the end of the null mask.</param>
    /// <param name="nullMaskSize">The null-mask size in bytes.</param>
    /// <returns>The offsets of the <c>var_len</c> variable columns, then the EOD.</returns>
    public static int[] DecodeVarOffsets(ReadOnlySpan<byte> row, int nullMaskSize)
    {
        int rowEnd = row.Length - 1;
        int numVarCols = row[rowEnd - nullMaskSize];
        int[] offsets = new int[numVarCols + 1];
        int numJumps = (row.Length - 1) / 256;
        int colOffset = rowEnd - nullMaskSize - numJumps - 1;

        // If the last jump is a dummy value, ignore it.
        if ((colOffset - numVarCols) / 256 < numJumps)
        {
            numJumps--;
        }

        int jumpsUsed = 0;
        for (int i = 0; i < numVarCols + 1; i++)
        {
            while (jumpsUsed < numJumps && i == row[rowEnd - nullMaskSize - jumpsUsed - 1])
            {
                jumpsUsed++;
            }

            offsets[i] = row[colOffset - i] + (jumpsUsed * 256);
        }

        return offsets;
    }

    /// <summary>
    /// Returns a Jet3 row's jump-table bytes in stored order: the entry for the
    /// highest 256-byte boundary first, the entry for offset 256 last, next to
    /// <c>var_len</c>.
    /// </summary>
    /// <param name="row">The whole row.</param>
    /// <param name="nullMaskSize">The null-mask size in bytes.</param>
    /// <returns>The jump-table bytes.</returns>
    public static byte[] JumpBytes(ReadOnlySpan<byte> row, int nullMaskSize)
    {
        int count = (row.Length - 1) / 256;
        int varLenPos = row.Length - nullMaskSize - 1;
        return row.Slice(varLenPos - count, count).ToArray();
    }

    /// <summary>
    /// Returns the expected jump-table bytes, in stored order, for a row whose
    /// variable-column offsets (EOD last) are <paramref name="offsets"/> and
    /// whose jump table has <paramref name="count"/> entries: per boundary, the
    /// first offset index at or past it, or <c>0xFF</c> when none is.
    /// </summary>
    /// <param name="offsets">The variable-column offsets, then the EOD.</param>
    /// <param name="count">The number of jump-table entries.</param>
    /// <returns>The jump-table bytes.</returns>
    public static byte[] ExpectedJumpBytes(IReadOnlyList<int> offsets, int count)
    {
        byte[] bytes = new byte[count];
        for (int k = count; k >= 1; k--)
        {
            int entry = 0xFF;
            for (int i = 0; i < offsets.Count; i++)
            {
                if (offsets[i] >= 256 * k)
                {
                    entry = i;
                    break;
                }
            }

            bytes[count - k] = (byte)entry;
        }

        return bytes;
    }

    /// <summary>
    /// Builds a Jet3 row as Access lays it out, with the jump-table bytes the
    /// caller gives: <c>num_cols</c>, the fixed area, the variable values in
    /// order, the EOD low byte, the offset low bytes last column first, the
    /// jump table, <c>var_len</c> and the null mask. Every column with a value
    /// has its null-mask bit set.
    /// </summary>
    /// <param name="fixedArea">The fixed area; its columns are numbered first.</param>
    /// <param name="fixedColumnCount">The number of fixed columns, all present.</param>
    /// <param name="varValues">The variable-column values; <see langword="null"/> is a null value.</param>
    /// <param name="jumpBytes">The jump-table bytes in stored order; their count must be <c>(rowLength - 1) / 256</c>.</param>
    /// <returns>The row bytes.</returns>
    public static byte[] BuildRow(ReadOnlySpan<byte> fixedArea, int fixedColumnCount, IReadOnlyList<byte[]?> varValues, ReadOnlySpan<byte> jumpBytes)
    {
        int numCols = fixedColumnCount + varValues.Count;
        Assert.InRange(numCols, 1, 255);
        int nullMaskSize = (numCols + 7) / 8;
        int dataLength = 0;
        foreach (byte[]? value in varValues)
        {
            dataLength += value?.Length ?? 0;
        }

        int eod = 1 + fixedArea.Length + dataLength;
        int rowLength = eod + 1 + varValues.Count + jumpBytes.Length + 1 + nullMaskSize;
        Assert.Equal((rowLength - 1) / 256, jumpBytes.Length);

        byte[] row = new byte[rowLength];
        row[0] = (byte)numCols;
        fixedArea.CopyTo(row.AsSpan(1));
        int pos = 1 + fixedArea.Length;
        int[] offsets = new int[varValues.Count];
        for (int i = 0; i < varValues.Count; i++)
        {
            offsets[i] = pos;
            byte[]? value = varValues[i];
            if (value is not null)
            {
                value.CopyTo(row, pos);
                pos += value.Length;
            }
        }

        row[pos++] = unchecked((byte)eod);
        for (int i = varValues.Count - 1; i >= 0; i--)
        {
            row[pos++] = unchecked((byte)offsets[i]);
        }

        jumpBytes.CopyTo(row.AsSpan(pos));
        pos += jumpBytes.Length;
        row[pos++] = (byte)varValues.Count;
        for (int col = 0; col < numCols; col++)
        {
            if (col < fixedColumnCount || varValues[col - fixedColumnCount] is not null)
            {
                row[pos + (col / 8)] |= (byte)(1 << (col % 8));
            }
        }

        return row;
    }
}
