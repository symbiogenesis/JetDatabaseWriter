namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;

/// <summary>
/// Builds a post-mutation index entry list by removing row pointers and
/// inserting new entries while preserving deterministic equal-key ordering.
/// </summary>
internal static class IndexEntrySplicer
{
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

        working.AddRange(adds);

        var indexed = new (IndexEntry Entry, int Order)[working.Count];
        for (int i = 0; i < working.Count; i++)
        {
            indexed[i] = (working[i], i);
        }

        Array.Sort(indexed, static (left, right) =>
        {
            int compare = IndexPageCodec.CompareKeyBytes(left.Entry.Key, right.Entry.Key);
            return compare != 0 ? compare : left.Order - right.Order;
        });

        var result = new List<IndexEntry>(indexed.Length);
        foreach ((IndexEntry entry, _) in indexed)
        {
            result.Add(entry);
        }

        return result;
    }

    private static long EncodePointer(long page, byte row) => (page << 8) | row;
}
