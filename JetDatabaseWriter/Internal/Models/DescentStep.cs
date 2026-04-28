namespace JetDatabaseWriter.Internal.Models;

using System.Collections.Generic;

/// <summary>
/// One step of an intermediate-page descent: the page number, a clone
/// of its raw bytes (for sibling-pointer header preservation on
/// rewrite), the decoded summary entries, and the index of the entry
/// whose <c>ChildPage</c> the descent followed.
/// </summary>
internal readonly struct DescentStep(long pageNumber, byte[] pageBytes, List<DecodedIntermediateEntry> entries, int takenIndex)
{
    public long PageNumber { get; } = pageNumber;

    public byte[] PageBytes { get; } = pageBytes;

    public List<DecodedIntermediateEntry> Entries { get; } = entries;

    public int TakenIndex { get; } = takenIndex;
}
