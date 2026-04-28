namespace JetDatabaseWriter.Internal.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Internal;

/// <summary>
/// One step of an intermediate-page descent: the page number, a clone
/// of its raw bytes (for sibling-pointer header preservation on
/// rewrite), the decoded summary entries, and the index of the entry
/// whose <c>ChildPage</c> the descent followed.
/// </summary>
internal readonly struct DescentStep(long pageNumber, byte[] pageBytes, List<IndexLeafIncremental.DecodedIntermediateEntry> entries, int takenIndex)
{
    public long PageNumber { get; } = pageNumber;

    public byte[] PageBytes { get; } = pageBytes;

    public List<IndexLeafIncremental.DecodedIntermediateEntry> Entries { get; } = entries;

    public int TakenIndex { get; } = takenIndex;
}
