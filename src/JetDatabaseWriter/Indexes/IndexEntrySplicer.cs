namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using System.Linq;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Builds a post-mutation index entry list by removing row pointers and
/// inserting new entries while preserving deterministic equal-key ordering.
/// </summary>
internal static class IndexEntrySplicer
{
    private static readonly IComparer<byte[]> KeyComparer = Comparer<byte[]>.Create(IndexPageCodec.CompareKeyBytes);

    /// <summary>
    /// Builds the post-mutation entry list by inserting <paramref name="adds"/>
    /// and removing every entry whose row pointer matches <paramref name="removes"/>.
    /// Returns <see langword="null"/> when any removal target is absent.
    /// </summary>
    /// <param name="existing">Decoded entries from the live leaf.</param>
    /// <param name="adds">New entries to insert; need not be sorted.</param>
    /// <param name="removes">Row pointers whose entries should be removed.</param>
    public static List<IndexEntry>? Splice(
        List<IndexEntry> existing,
        IReadOnlyList<IndexEntry> adds,
        IReadOnlyList<(long DataPage, byte DataRow)> removes)
    {
        var working = new List<IndexEntry>(existing.Count + adds.Count);
        if (removes.Count == 0)
        {
            working.AddRange(existing);
        }
        else
        {
            var removeSet = new HashSet<long>(removes.Count);
            foreach ((long page, byte row) in removes)
            {
                removeSet.Add(EncodePointer(page, row));
            }

            int removed = 0;
            foreach (IndexEntry entry in existing)
            {
                if (removeSet.Remove(EncodePointer(entry.DataPage, entry.DataRow)))
                {
                    removed++;
                    continue;
                }

                working.Add(entry);
            }

            if (removed != removes.Count)
            {
                return null;
            }
        }

        if (IsSorted(working) && IsSorted(adds))
        {
            int existingIndex = working.Count - 1;
            int addedIndex = adds.Count - 1;
            working.AddRange(adds);
            int destination = working.Count - 1;
            while (addedIndex >= 0)
            {
                // Merging backward places additions after existing equal keys.
                if (existingIndex >= 0 && IndexPageCodec.CompareKeyBytes(working[existingIndex].Key, adds[addedIndex].Key) > 0)
                {
                    working[destination--] = working[existingIndex--];
                }
                else
                {
                    working[destination--] = adds[addedIndex--];
                }
            }

            return working;
        }

        working.AddRange(adds);

        // OrderBy is stable: existing equal keys precede additions, whose input order is retained.
        return working.OrderBy(static entry => entry.Key, KeyComparer).ToList();
    }

    private static bool IsSorted(IReadOnlyList<IndexEntry> entries)
    {
        for (int i = 1; i < entries.Count; i++)
        {
            if (IndexPageCodec.CompareKeyBytes(entries[i - 1].Key, entries[i].Key) > 0)
            {
                return false;
            }
        }

        return true;
    }

    private static long EncodePointer(long page, byte row) => (page << 8) | row;
}
