namespace JetDatabaseWriter.Pages;

using System;

/// <summary>
/// The jump table of a Jet3 (Access 97) row. Jet3 stores the EOD and each
/// variable-column offset in one byte, so a row longer than 256 bytes carries
/// <c>(rowLength - 1) / 256</c> one-byte jump entries between the offset table
/// and <c>var_len</c>. Entry <c>k</c> (for the boundary <c>256 * (k + 1)</c>)
/// sits <c>k + 1</c> bytes before <c>var_len</c>, and holds the index of the
/// first offset at or past its boundary, the EOD counting as index
/// <c>var_len</c>. An offset's high part is 256 times the number of entries
/// that index has reached. An entry whose boundary no offset reaches is a
/// dummy (Access writes <c>0xFF</c>); readers drop the last entry when the EOD
/// lies below its boundary. This follows mdbtools <c>mdb_crack_row3</c> and
/// Jackcess <c>TableImpl.readJumpTableVarColOffsets</c>, and matches the
/// Access 97 row in test2V1997.mdb (MSP_PROJECTS: 290 bytes, EOD 255, one
/// <c>0xFF</c> dummy).
/// </summary>
internal static class Jet3JumpTable
{
    /// <summary>The span of offsets one jump entry covers.</summary>
    internal const int BoundarySize = 256;

    /// <summary>
    /// Returns the number of jump entries a Jet3 row of <paramref name="rowLength"/>
    /// bytes, jump table included, carries: <c>(rowLength - 1) / 256</c>, so a
    /// row of exactly 256 bytes has none.
    /// </summary>
    /// <param name="rowLength">The row length in bytes.</param>
    /// <returns>The number of jump entries.</returns>
    internal static int EntryCount(int rowLength) => (rowLength - 1) / BoundarySize;

    /// <summary>
    /// Returns how many of a row's <paramref name="entryCount"/> jump entries
    /// are in use: all of them, or one fewer when the EOD at
    /// <paramref name="eodPosition"/> lies below the last entry's boundary,
    /// which makes that entry a dummy.
    /// </summary>
    /// <param name="entryCount">The row's jump-entry count.</param>
    /// <param name="eodPosition">The row-relative position of the EOD byte.</param>
    /// <returns>The number of jump entries in use.</returns>
    internal static int UsableEntryCount(int entryCount, int eodPosition)
        => entryCount > 0 && eodPosition / BoundarySize < entryCount ? entryCount - 1 : entryCount;

    /// <summary>
    /// Returns the high part to add to the low byte of offset
    /// <paramref name="index"/> (the EOD when it equals <c>var_len</c>): 256
    /// times the number of leading jump entries, in boundary order, that do not
    /// decrease and do not exceed <paramref name="index"/>. This is the count
    /// Jackcess's sequential scan reaches by that index.
    /// </summary>
    /// <param name="row">The bytes holding the row.</param>
    /// <param name="rowStart">The row's start within <paramref name="row"/>.</param>
    /// <param name="jumpTableEnd">The row-relative position just past the jump table: the <c>var_len</c> byte.</param>
    /// <param name="usableEntries">The number of jump entries in use.</param>
    /// <param name="index">The offset index.</param>
    /// <returns>The value to add to the offset's low byte.</returns>
    internal static int HighPart(ReadOnlySpan<byte> row, int rowStart, int jumpTableEnd, int usableEntries, int index)
    {
        int reached = 0;
        int previous = 0;
        while (reached < usableEntries)
        {
            int entry = row[rowStart + jumpTableEnd - 1 - reached];
            if (entry > index || entry < previous)
            {
                break;
            }

            previous = entry;
            reached++;
        }

        return reached * BoundarySize;
    }
}
