namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Builds a post-mutation index entry list by removing row pointers and
/// inserting new entries in native key/page/row order.
/// </summary>
internal static class IndexEntrySplicer
{
    private static readonly IComparer<IndexEntry> EntryComparer = Comparer<IndexEntry>.Create(IndexPageCodec.CompareEntries);

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

        if (IsSorted(adds) && IsSorted(working))
        {
            int existingIndex = working.Count - 1;
            int addedIndex = adds.Count - 1;
            working.AddRange(adds);
            int destination = working.Count - 1;
            while (addedIndex >= 0)
            {
                if (existingIndex >= 0 && IndexPageCodec.CompareEntries(working[existingIndex], adds[addedIndex]) > 0)
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

        // Complete-entry ties encode identical key/bookmark bytes, so this
        // private result buffer does not require a stable sorting algorithm.
        working.Sort(EntryComparer);
        return working;
    }

    private static bool IsSorted(IReadOnlyList<IndexEntry> entries)
    {
        for (int i = 1; i < entries.Count; i++)
        {
            if (IndexPageCodec.CompareEntries(entries[i - 1], entries[i]) > 0)
            {
                return false;
            }
        }

        return true;
    }

    private static long EncodePointer(long page, byte row) => (page << 8) | row;
}
