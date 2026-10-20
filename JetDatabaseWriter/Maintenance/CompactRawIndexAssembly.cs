namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;

/// <summary>Composes stored-key relocation, a private tree and its exact native ownership map.</summary>
/// <remarks>
/// Construction appends only to an exclusive private decrypted destination, retaining the pinned original prefix.
/// The destination, both empty disk-capable fence streams and the immutable source observations and row ledger must
/// have distinct backing storage, including through wrappers. All streams remain caller-owned. The source observation
/// must have completed independently with complete physical index identity, ownership and live-bookmark evidence.
/// Reopened validation requires independent immutable original observations and a separately reopened row ledger, never builder fences or
/// entries decoded from the destination. It verifies supplied raw keys verbatim without regenerating collation keys.
/// Relocated row pages must precede the tree allocation; data/LVAL staging must therefore finish before index assembly.
/// Missing source observations and stored-key/row semantics remain separate proofs. Neither phase installs TDEF pointers,
/// changes catalog/global allocation, reclaims pinned storage, encrypts pages or authorizes database replacement.
/// Failure leaves partial private output and returns no completion evidence. Complete database validation remains required.
/// </remarks>
internal static class CompactRawIndexAssembly
{
    private const long AssemblyWorkingBytes = 4096;

    /// <summary>Stages a relocated stored-key tree followed by a native map containing exactly its pages.</summary>
    /// <param name="format">The verified preserved native format.</param>
    /// <param name="owner">The pinned destination table-definition page before the append allocation.</param>
    /// <param name="originalEntries">Independently completed immutable original stored-key/bookmark observations in strict order.</param>
    /// <param name="relocations">The validated monotone complete row map, preserving equal-key bookmark tie order.</param>
    /// <param name="privateDestination">The exclusive page-aligned writable seekable private decrypted destination.</param>
    /// <param name="firstFenceStream">The first distinct empty readable writable seekable fence scratch stream.</param>
    /// <param name="secondFenceStream">The second distinct empty readable writable seekable fence scratch stream.</param>
    /// <param name="budget">Aggregate allocations and component work, I/O and working-storage limits.</param>
    /// <param name="cancellationToken">Cancels admission, relocation, enumeration, packing, map work and I/O.</param>
    /// <returns>Unvalidated allocations; independently reopen and validate them before obtaining TDEF references.</returns>
    /// <exception cref="ArgumentException">The destination or scratch stream capabilities, identities or alignment are invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The pinned owner or append allocation is outside the native range.</exception>
    /// <exception cref="JetLimitationException">An allocation, I/O or working-storage allowance is exhausted.</exception>
    /// <exception cref="JetCorruptDataException">Stored entries, row mappings or private allocation geometry disagree.</exception>
    internal static async ValueTask<CompactRawIndexAssemblyAllocation> BuildAsync(
        JetFormat format,
        long owner,
        IAsyncEnumerable<IndexEntry> originalEntries,
        CompactRowRelocationMap relocations,
        Stream privateDestination,
        Stream firstFenceStream,
        Stream secondFenceStream,
        CompactRawIndexAssemblyBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(originalEntries, nameof(originalEntries));
        Guard.NotNull(relocations, nameof(relocations));
        Guard.NotNull(privateDestination, nameof(privateDestination));
        Guard.NotNull(firstFenceStream, nameof(firstFenceStream));
        Guard.NotNull(secondFenceStream, nameof(secondFenceStream));
        ValidateBudget(budget, cancellationToken);
        if (!privateDestination.CanSeek || !privateDestination.CanWrite || privateDestination.Length % format.PageSize != 0)
        {
            throw new ArgumentException("The private index assembly destination must be writable, seekable and page aligned.", nameof(privateDestination));
        }

        long firstPage = privateDestination.Length / format.PageSize;
        if (firstPage is < 3 or > 0xFFFFFE || owner <= 0 || owner >= firstPage)
        {
            throw new ArgumentOutOfRangeException(nameof(owner), "The pinned owner must precede room for both native private allocations.");
        }

        long maximumPages = Math.Min(budget.MaxPages, Math.Min(budget.MaxOutputBytes / format.PageSize, 0x1000000L - firstPage));
        if (maximumPages < 2)
        {
            throw Limit("Index assembly requires allowances for a root and an ownership-map row.");
        }

        CompactRawIndexBuilderResult tree = await CompactRawIndexBuilder.BuildAsync(
            format,
            owner,
            RelocateAsync(originalEntries, relocations, firstPage, cancellationToken),
            privateDestination,
            firstFenceStream,
            secondFenceStream,
            new()
            {
                MaxEntries = budget.MaxEntries,
                MaxCanonicalBytes = budget.MaxCanonicalBytes,
                MaxTreePages = maximumPages - 1,
                MaxOutputBytes = (maximumPages - 1) * format.PageSize,
                MaxFenceBytes = budget.MaxFenceBytes,
                MaxLevels = budget.MaxLevels,
                MaxWorkingBytes = budget.MaxWorkingBytes - AssemblyWorkingBytes,
            },
            cancellationToken).ConfigureAwait(false);
        CompactUsageMapBuilderResult owned = await CompactUsageMapBuilder.BuildAsync(
            format,
            TreeRunAsync(tree, cancellationToken),
            privateDestination,
            new()
            {
                MaxRuns = 1,
                MaxListedPages = tree.PageCount,
                MaxPages = maximumPages - tree.PageCount,
                MaxOutputBytes = (maximumPages - tree.PageCount) * format.PageSize,
                MaxWorkingBytes = budget.MaxWorkingBytes - AssemblyWorkingBytes,
            },
            cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (privateDestination.Length != (owned.FirstPageNumber + owned.PageCount) * format.PageSize)
        {
            throw Corrupt("The private index assembly changed during its final map flush.");
        }

        return new(tree, owned);
    }

    /// <summary>Independently checks reopened stored entries and exact map membership before returning replacement references.</summary>
    /// <param name="format">The verified preserved native format.</param>
    /// <param name="owner">The pinned original destination table-definition identity.</param>
    /// <param name="reopenedDestination">The exclusive immutable readable seekable reopened private decrypted destination.</param>
    /// <param name="originalEntries">Independent immutable original observations, in original stored-key/bookmark order.</param>
    /// <param name="relocations">The separately reopened validated complete immutable row map.</param>
    /// <param name="allocation">The untrusted complete tree and ownership-map allocation to compare.</param>
    /// <param name="budget">Aggregate allocations, reopened reads and simultaneous active-component working limits.</param>
    /// <param name="cancellationToken">Cancels admission, traversal, comparisons, map work and every read.</param>
    /// <returns>Verified private references and coverage evidence, without a complete-database verdict.</returns>
    /// <exception cref="JetCorruptDataException">The supplied entries, private allocations, stored tree or ownership map disagree.</exception>
    /// <exception cref="JetLimitationException">An aggregate allocation, I/O or working-storage allowance is exhausted.</exception>
    internal static async ValueTask<CompactRawIndexAssemblyResult> ValidateAsync(
        JetFormat format,
        long owner,
        Stream reopenedDestination,
        IAsyncEnumerable<IndexEntry> originalEntries,
        CompactRowRelocationMap relocations,
        CompactRawIndexAssemblyAllocation allocation,
        CompactRawIndexAssemblyBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(reopenedDestination, nameof(reopenedDestination));
        Guard.NotNull(originalEntries, nameof(originalEntries));
        Guard.NotNull(relocations, nameof(relocations));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(allocation.Tree, nameof(allocation));
        Guard.NotNull(allocation.OwnedPages, nameof(allocation));
        ValidateBudget(budget, cancellationToken);
        CompactRawIndexBuilderResult tree = allocation.Tree;
        CompactUsageMapBuilderResult owned = allocation.OwnedPages;
        if (tree.FirstPageNumber is < 3 or > 0xFFFFFF || tree.PageCount < 1 || tree.PageCount > 0x1000000L - tree.FirstPageNumber
            || tree.RootPageNumber is <= 0 or > 0xFFFFFF || owned.FirstPageNumber != tree.FirstPageNumber + tree.PageCount
            || owned.PageCount < 1 || owned.PageCount > 0x1000000L - owned.FirstPageNumber
            || owned.ListedPages != tree.PageCount || owned.InputRuns != 1)
        {
            throw Corrupt("The index assembly does not declare adjacent complete native tree and exact ownership-map allocations.");
        }

        long pages = tree.PageCount + owned.PageCount;
        if (pages > budget.MaxPages || pages > budget.MaxOutputBytes / format.PageSize)
        {
            throw Limit("Index assembly validation exceeds aggregate MaxPages or MaxOutputBytes.");
        }

        CompactRawIndexValidatorResult treeValidation = await CompactRawIndexValidator.ValidateAsync(
            format,
            owner,
            reopenedDestination,
            CheckedOriginalsAsync(originalEntries, relocations, tree.FirstPageNumber, cancellationToken),
            relocations,
            tree,
            new()
            {
                MaxTreePages = checked((int)Math.Min(budget.MaxPages, int.MaxValue)),
                MaxEntries = budget.MaxEntries,
                MaxCanonicalBytes = budget.MaxCanonicalBytes,
                MaxReadBytes = budget.MaxReadBytes,
                MaxLevels = budget.MaxLevels,
                MaxWorkingBytes = budget.MaxWorkingBytes - AssemblyWorkingBytes,
                IndexAudit = budget.IndexAudit,
            },
            cancellationToken).ConfigureAwait(false);
        CompactUsageMapValidatorResult mapValidation = await CompactUsageMapValidator.ValidateAsync(
            format,
            TreeRunAsync(tree, cancellationToken),
            reopenedDestination,
            owned,
            new()
            {
                MaxRuns = 1,
                MaxListedPages = tree.PageCount,
                MaxPages = owned.PageCount,
                MaxBitmapBytes = budget.MaxReadBytes,
                MaxReadBytes = budget.MaxReadBytes - treeValidation.ReadBytes,
                MaxWorkingBytes = budget.MaxWorkingBytes - AssemblyWorkingBytes,
            },
            cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new(new(checked((uint)tree.RootPageNumber), owned.Pointer), treeValidation, mapValidation);
    }

    private static void ValidateBudget(CompactRawIndexAssemblyBudget budget, CancellationToken token)
    {
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.IndexAudit, nameof(budget));
        token.ThrowIfCancellationRequested();
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxEntries, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxCanonicalBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxFenceBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 1L, long.MaxValue, nameof(budget));
        Guard.Positive(budget.MaxLevels, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        if (budget.MaxWorkingBytes <= AssemblyWorkingBytes)
        {
            throw Limit("Index assembly exceeds MaxWorkingBytes.");
        }
    }

    private static async IAsyncEnumerable<IndexEntry> RelocateAsync(IAsyncEnumerable<IndexEntry> entries, CompactRowRelocationMap relocations, long firstTreePage, [EnumeratorCancellation] CancellationToken token)
    {
        await foreach (IndexEntry entry in entries.WithCancellation(token).ConfigureAwait(false))
        {
            token.ThrowIfCancellationRequested();
            IndexEntry relocated = await relocations.RelocateAsync(entry, token).ConfigureAwait(false);
            CheckRowPage(relocated, firstTreePage);
            yield return relocated;
        }
    }

    private static async IAsyncEnumerable<IndexEntry> CheckedOriginalsAsync(IAsyncEnumerable<IndexEntry> entries, CompactRowRelocationMap relocations, long firstTreePage, [EnumeratorCancellation] CancellationToken token)
    {
        await foreach (IndexEntry entry in entries.WithCancellation(token).ConfigureAwait(false))
        {
            token.ThrowIfCancellationRequested();
            CheckRowPage(await relocations.RelocateAsync(entry, token).ConfigureAwait(false), firstTreePage);
            yield return entry;
        }
    }

    private static void CheckRowPage(IndexEntry relocated, long firstTreePage)
    {
        if (relocated.DataPage >= firstTreePage)
        {
            throw Corrupt("A relocated index bookmark does not name existing row storage before the private tree allocation.");
        }
    }

    private static async IAsyncEnumerable<CompactPageRun> TreeRunAsync(CompactRawIndexBuilderResult tree, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        token.ThrowIfCancellationRequested();
        yield return new(tree.FirstPageNumber, tree.PageCount);
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptIndex, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);
}
