namespace JetDatabaseWriter.Internal;

using System.Collections.Generic;

/// <summary>
/// One step of an intermediate-page descent: the page number, a clone
/// of its raw bytes (for sibling-pointer header preservation on
/// rewrite), the decoded summary entries, and the index of the entry
/// whose <c>ChildPage</c> the descent followed.
/// </summary>
internal readonly struct DescentStep
{
    public DescentStep(long pageNumber, byte[] pageBytes, List<IndexLeafIncremental.DecodedIntermediateEntry> entries, int takenIndex)
    {
        PageNumber = pageNumber;
        PageBytes = pageBytes;
        Entries = entries;
        TakenIndex = takenIndex;
    }

    public long PageNumber { get; }

    public byte[] PageBytes { get; }

    public List<IndexLeafIncremental.DecodedIntermediateEntry> Entries { get; }

    public int TakenIndex { get; }
}
