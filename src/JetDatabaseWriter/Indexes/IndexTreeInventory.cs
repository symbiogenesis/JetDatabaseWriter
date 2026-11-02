namespace JetDatabaseWriter.Indexes;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Verification;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Acquired root membership and original native leaf headers, before any page effect.</summary>
/// <param name="Pages">Every page reached through root children and tail pointers, in page order.</param>
/// <param name="Leaves">The captured leaf headers belonging to those pages.</param>
/// <param name="LeafPages">The sorted leaf identities validated by the generated chain controller.</param>
internal sealed record IndexTreeInventory(long[] Pages, Dictionary<long, IndexTreeInventory.LeafLinks> Leaves, long[] LeafPages)
{
    /// <summary>Acquires the complete physical tree and admits its captured leaf chain before any page effect.</summary>
    /// <param name="format">The native database format.</param>
    /// <param name="pager">The transaction-aware source of original or staged pages.</param>
    /// <param name="layout">The native index page layout.</param>
    /// <param name="tdefPage">The owning table definition.</param>
    /// <param name="rootPage">The physical tree root.</param>
    /// <param name="cancellationToken">Cancels page acquisition and header transport.</param>
    /// <returns>The captured inventory, or null when the child graph cannot be read.</returns>
    /// <exception cref="JetCorruptDataException">The leaf headers do not form one complete reciprocal chain within the tree.</exception>
    internal static async ValueTask<IndexTreeInventory?> ReadAsync(JetFormat format, Pager pager, IndexPageLayout layout, long tdefPage, long rootPage, CancellationToken cancellationToken)
    {
        long pageCount = pager.PageCount;
        var pages = new List<long>();
        var seen = new HashSet<long>();
        var stack = new Stack<long>();
        var leaves = new Dictionary<long, LeafLinks>();
        stack.Push(rootPage);
        while (stack.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long pageNumber = stack.Pop();
            if (pageNumber <= 2 || pageNumber >= pageCount)
            {
                return null;
            }

            if (!seen.Add(pageNumber))
            {
                continue;
            }

            byte[] page = await pager.ReadPageCopyAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            long owner = Ru32(page, 4);
            if (owner != tdefPage)
            {
                return null;
            }

            pages.Add(pageNumber);
            if (page[0] == Constants.IndexLeafPage.PageTypeLeaf)
            {
                leaves.Add(pageNumber, new(owner, page[0], IndexPageCodec.ReadPrevPage(layout, page), IndexPageCodec.ReadNextPage(layout, page)));
                continue;
            }

            if (page[0] != Constants.IndexLeafPage.PageTypeIntermediate
                || !IndexPageCodec.TryDecodeIntermediateEntries(layout, page, format.PageSize, out List<DecodedIntermediateEntry> entries))
            {
                return null;
            }

            long tailPage = IndexPageCodec.ReadTailPage(layout, page);
            if (tailPage != 0)
            {
                stack.Push(tailPage);
            }

            if (entries.Count == 0 && tailPage == 0)
            {
                return null;
            }

            for (int index = entries.Count - 1; index >= 0; index--)
            {
                stack.Push(entries[index].ChildPage);
            }
        }

        long[] leafPages = new long[leaves.Count];
        leaves.Keys.CopyTo(leafPages, 0);
        Array.Sort(leafPages);
        var inventory = new IndexTreeInventory(pages.ToArray(), leaves, leafPages);
        ValidateLeafChain(tdefPage, pageCount, inventory, cancellationToken);
        Array.Sort(inventory.Pages);
        return inventory;
    }

    /// <summary>Checks the captured sorted leaf identities through the generated membership decision.</summary>
    /// <param name="page">The candidate leaf page.</param>
    /// <returns>Whether the physical tree contains this leaf.</returns>
    internal bool ContainsLeaf(long page)
        => IndexMutationKernel.FindLeafIndex(this.LeafPages, page) < this.LeafPages.Length;

    private static void ValidateLeafChain(long owner, long pageCount, IndexTreeInventory inventory, CancellationToken cancellationToken)
    {
        int count = inventory.LeafPages.Length;
        long[] chain = new long[count];
        long[] previous = new long[count];
        long[] next = new long[count];
        long[] owners = new long[count];
        byte[] kinds = new byte[count];
        byte[] scratch = new byte[count];
        long first = count == 0 ? 0 : inventory.LeafPages[0];
        foreach ((long page, LeafLinks links) in inventory.Leaves)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (links.Prev == 0)
            {
                first = page;
                break;
            }
        }

        // This bounded transport supplies actual captured headers in candidate
        // chain order. Only the generated validator can admit their membership,
        // uniqueness, owner, reciprocal adjacency and complete coverage.
        long current = first;
        for (int index = 0; index < count && inventory.Leaves.TryGetValue(current, out LeafLinks links); index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            chain[index] = current;
            previous[index] = links.Prev;
            next[index] = links.Next;
            owners[index] = links.Owner;
            kinds[index] = links.Kind;
            current = links.Next;
        }

        if (!IndexMutationKernel.ValidateLeafChain(inventory.LeafPages, chain, previous, next, owners, kinds, owner, pageCount, scratch))
        {
            throw new JetCorruptDataException(JetErrorCode.CorruptIndex, "Index leaves must form one complete reciprocal chain within their physical tree.", new JetErrorInfo { PageNumber = first });
        }
    }

    internal readonly record struct LeafLinks(long Owner, byte Kind, long Prev, long Next);
}
