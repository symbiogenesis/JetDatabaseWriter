namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Describes proven source-page reclamation for a database-wide compact copy.
/// </summary>
/// <remarks>
/// Pages <c>0</c>, <c>1</c>, <c>2</c> and every active local table's <c>TDef</c> chain (<c>0x02</c>),
/// together with any opaque or unassigned source pages, remain pinned at their original physical
/// page numbers. Proven reclaimable pages after the last pinned page are truncated from the
/// retained prefix (<see cref="RetainedPrefixPages"/>), and proven reclaimable pages before
/// <see cref="RetainedPrefixPages"/> (<see cref="ScrubbedPrefixPages"/>) are overwritten with
/// canonical zero-scrubbed freed pages (<c>0x06</c>) and marked free (<c>1</c>) in Page 1's global usage map.
/// </remarks>
internal sealed class CompactReclamationPlan
{
    private readonly HashSet<long> scrubbedPrefixPages;

    /// <summary>
    /// Initializes a new instance of the <see cref="CompactReclamationPlan"/> class.
    /// </summary>
    /// <param name="sourcePages">The total physical page count of the source database.</param>
    /// <param name="retainedPrefixPages">The number of source prefix pages retained in the destination (<c>3 &lt;= retainedPrefixPages &lt;= sourcePages</c>).</param>
    /// <param name="scrubbedPrefixPages">Source pages in <c>3 .. retainedPrefixPages - 1</c> proven reclaimable and scrubbed to canonical free pages.</param>
    /// <param name="isComplete">Whether source discovery and ownership completed with zero uncertainty or salvage diagnostics.</param>
    /// <param name="reclamationAuthorized">Whether reclamation of proven non-pinned source pages is authorized.</param>
    internal CompactReclamationPlan(
        long sourcePages,
        long retainedPrefixPages,
        IEnumerable<long> scrubbedPrefixPages,
        bool isComplete,
        bool reclamationAuthorized)
    {
        Guard.InRange(sourcePages, 3L, 0x1000000L, nameof(sourcePages));
        Guard.InRange(retainedPrefixPages, 3L, sourcePages, nameof(retainedPrefixPages));
        Guard.NotNull(scrubbedPrefixPages, nameof(scrubbedPrefixPages));

        var scrubbed = new HashSet<long>();
        foreach (long page in scrubbedPrefixPages)
        {
            if (page < 3 || page >= retainedPrefixPages)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(scrubbedPrefixPages),
                    page,
                    "Scrubbed prefix pages must lie strictly between page 3 and RetainedPrefixPages - 1.");
            }

            _ = scrubbed.Add(page);
        }

        this.SourcePages = sourcePages;
        this.RetainedPrefixPages = retainedPrefixPages;
        this.scrubbedPrefixPages = scrubbed;
        this.IsComplete = isComplete;
        this.ReclamationAuthorized = reclamationAuthorized;
    }

    /// <summary>Gets the total physical page count of the source database.</summary>
    internal long SourcePages { get; }

    /// <summary>Gets the number of prefix pages retained from the source database (<c>0 .. RetainedPrefixPages - 1</c>).</summary>
    internal long RetainedPrefixPages { get; }

    /// <summary>Gets the number of interior prefix pages scrubbed to canonical free pages.</summary>
    internal int ScrubbedPrefixPageCount => this.scrubbedPrefixPages.Count;

    /// <summary>Gets the interior prefix pages (<c>3 .. RetainedPrefixPages - 1</c>) scrubbed to canonical free pages and marked free in the global usage map.</summary>
    internal IEnumerable<long> ScrubbedPrefixPages => this.scrubbedPrefixPages;

    /// <summary>Gets the number of trailing source pages truncated from the retained prefix.</summary>
    internal int TruncatedTailPages => checked((int)(this.SourcePages - this.RetainedPrefixPages));

    /// <summary>Gets the total number of reclaimed source pages (scrubbed interior pages plus truncated tail pages).</summary>
    internal int ReclaimedPages => checked(this.scrubbedPrefixPages.Count + this.TruncatedTailPages);

    /// <summary>Gets the number of source pages that remain pinned in the retained prefix.</summary>
    internal int PinnedPages => checked((int)(this.RetainedPrefixPages - this.scrubbedPrefixPages.Count));

    /// <summary>Gets a value indicating whether discovery and ownership completed with zero uncertainty or salvage diagnostics.</summary>
    internal bool IsComplete { get; }

    /// <summary>Gets a value indicating whether reclamation of proven non-pinned source pages is authorized.</summary>
    internal bool ReclamationAuthorized { get; }

    /// <summary>
    /// Creates a canonical zero-scrubbed freed page (<c>0x06</c>) for the given <paramref name="format"/>.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A freshly allocated canonical freed page buffer of length <see cref="JetFormat.PageSize"/>.</returns>
    internal static byte[] CreateScrubbedFreePage(JetFormat format)
    {
        Guard.NotNull(format, nameof(format));
        byte[] page = new byte[format.PageSize];
        FormatScrubbedFreePage(page, format.PageSize);
        return page;
    }

    /// <summary>
    /// Formats <paramref name="page"/> in-place as a canonical zero-scrubbed freed page (<c>0x06</c>).
    /// </summary>
    /// <param name="page">The page buffer to format.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    internal static void FormatScrubbedFreePage(Span<byte> page, int pageSize)
    {
        Span<byte> slice = page[..pageSize];
        slice.Clear();
        slice[0] = Constants.PageTypes.Freed;
        slice[1] = 1;
        BinaryPrimitives.WriteUInt16LittleEndian(slice[2..4], checked((ushort)(pageSize - 16)));
    }

    /// <summary>
    /// Checks whether <paramref name="page"/> matches the exact canonical zero-scrubbed freed page (<c>0x06</c>) layout.
    /// </summary>
    /// <param name="page">The page bytes to inspect.</param>
    /// <param name="pageSize">The page size in bytes.</param>
    /// <returns><see langword="true"/> when <paramref name="page"/> is a canonical zero-scrubbed freed page; otherwise <see langword="false"/>.</returns>
    internal static bool IsScrubbedFreePage(ReadOnlySpan<byte> page, int pageSize)
    {
        if (page.Length < pageSize
            || page[0] != Constants.PageTypes.Freed
            || page[1] != 1
            || BinaryPrimitives.ReadUInt16LittleEndian(page[2..4]) != checked((ushort)(pageSize - 16)))
        {
            return false;
        }

        ReadOnlySpan<byte> tail = page[4..pageSize];
#if NET8_0_OR_GREATER
        return tail.IndexOfAnyExcept((byte)0) < 0;
#else
        for (int index = 0; index < tail.Length; index++)
        {
            if (tail[index] != 0)
            {
                return false;
            }
        }

        return true;
#endif
    }

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="pageNumber"/> is a scrubbed interior prefix page.
    /// </summary>
    /// <param name="pageNumber">The physical page number to check.</param>
    /// <returns><see langword="true"/> if <paramref name="pageNumber"/> is in <see cref="ScrubbedPrefixPages"/>; otherwise <see langword="false"/>.</returns>
    internal bool IsScrubbedPrefixPage(long pageNumber)
        => this.scrubbedPrefixPages.Contains(pageNumber);

    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="pageNumber"/> is reclaimed (either scrubbed in the prefix or truncated after <see cref="RetainedPrefixPages"/>).
    /// </summary>
    /// <param name="pageNumber">The physical source page number to check.</param>
    /// <returns><see langword="true"/> if <paramref name="pageNumber"/> is reclaimed; otherwise <see langword="false"/>.</returns>
    internal bool IsReclaimed(long pageNumber)
        => pageNumber >= this.RetainedPrefixPages || this.scrubbedPrefixPages.Contains(pageNumber);

    /// <summary>
    /// Validates that this reclamation plan matches the given <paramref name="sourcePageCount"/>.
    /// </summary>
    /// <param name="sourcePageCount">The physical page count of the source database.</param>
    /// <exception cref="JetCorruptDataException">The reclamation plan does not match the source page geometry.</exception>
    internal void ValidateAgainstSource(long sourcePageCount)
    {
        if (this.SourcePages != sourcePageCount || this.RetainedPrefixPages > sourcePageCount || this.RetainedPrefixPages < 3)
        {
            throw new JetCorruptDataException(
                JetErrorCode.CorruptPage,
                "The compact reclamation plan does not match the immutable source page geometry.");
        }
    }
}
