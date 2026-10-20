namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>Composes declared physical roots and raw observations into bounded known ownership evidence.</summary>
/// <remarks>
/// Calls must be serialized. The caller supplies observations from an immutable source and declares each
/// physical descriptor before its root is observed. Same-owner root and page sharing stays observable.
/// Contradictory expected owners refuse after retaining evidence. Missing or opaque incoming references,
/// unobserved pages, catalog completeness and native sharing semantics remain unproven. No source is written.
/// Admission stays held across scans and failures until disposal; visitor retention is caller-owned.
/// </remarks>
internal sealed class CompactIndexOwnershipAudit : IDisposable
{
    private readonly long pageCount;
    private readonly CompactIndexOwnershipBudget budget;
    private readonly IDisposable? reservation;
    private Dictionary<long, PageState> pages = [];
    private long descriptors;
    private long references;
    private long graphSteps;
    private long records;
    private bool disposed;
    private bool failed;
    private bool completed;

    /// <summary>Initializes a new instance of the <see cref="CompactIndexOwnershipAudit"/> class without source I/O.</summary>
    /// <param name="pageCount">The immutable source's physical page count.</param>
    /// <param name="budget">Limits and optional surrounding discovery accounting.</param>
    /// <exception cref="JetLimitationException">Conservative retained storage exceeds its allowance.</exception>
    internal CompactIndexOwnershipAudit(long pageCount, CompactIndexOwnershipBudget budget)
    {
        Guard.InRange(pageCount, 3L, long.MaxValue, nameof(pageCount));
        Guard.NotNull(budget, nameof(budget));
        Guard.Positive(budget.MaxTrackedPages, nameof(budget));
        Guard.InRange(budget.MaxRootDescriptors, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReferences, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxGraphSteps, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxRecords, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        this.pageCount = pageCount;
        this.budget = budget;

        // One state per physical coordinate covers declarations, observations and incoming summaries.
        // Include dictionary growth and object scratch; descriptors and edges are streamed, not retained.
        long storage = (384L * Math.Min(pageCount - 1, budget.MaxTrackedPages)) + 65_536;
        if (storage > budget.MaxWorkingBytes)
        {
            throw Limit(0, "The index ownership storage reservation exceeds MaxWorkingBytes.");
        }

        this.reservation = budget.Accounting?.ReserveWorkingBytes(storage);
    }

    /// <summary>Retains one physical descriptor's declared root, allowing same-owner sharing.</summary>
    /// <param name="ownerTDefPage">The declaring table-definition page.</param>
    /// <param name="rootPage">The nonzero physical index root.</param>
    /// <param name="descriptorIndex">The physical descriptor ordinal within its definition.</param>
    /// <param name="visitor">The awaited ownership observation consumer.</param>
    /// <param name="cancellationToken">Cancels declaration and delivery.</param>
    /// <returns>A task completing after declaration and any sharing evidence are delivered.</returns>
    internal async ValueTask DeclareRootAsync(long ownerTDefPage, long rootPage, int descriptorIndex, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(visitor, nameof(visitor));
        Guard.InRange(descriptorIndex, 0, int.MaxValue, nameof(descriptorIndex));
        this.CheckAvailable();
        try
        {
            this.Step(rootPage, cancellationToken);
            this.ValidatePage(ownerTDefPage);
            this.ValidatePage(rootPage);
            if (this.descriptors >= this.budget.MaxRootDescriptors)
            {
                throw Limit(rootPage, "The physical index declarations exceed MaxRootDescriptors.");
            }

            this.descriptors++;
            CompactIndexOwnershipRecord record = new() { Kind = "root-declaration", PageNumber = rootPage, OwnerTDefPage = ownerTDefPage, RootPageNumber = rootPage, DescriptorIndex = descriptorIndex };
            PageState state = await this.ClaimAsync(record, visitor, cancellationToken).ConfigureAwait(false);
            if (state.DeclaredRoot)
            {
                record = record with { Kind = "shared-root", RelatedOwnerTDefPage = state.Owner, RelatedRootPageNumber = rootPage, RelatedDescriptorIndex = state.DescriptorIndex };
            }
            else
            {
                state.DeclaredRoot = true;
                state.DescriptorIndex = descriptorIndex;
            }

            await this.EmitAsync(record, visitor, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            this.failed = true;
            throw;
        }
    }

    /// <summary>Consumes one raw root observation without retaining canonical key buffers.</summary>
    /// <param name="record">The raw index audit observation.</param>
    /// <param name="visitor">The awaited ownership observation consumer.</param>
    /// <param name="cancellationToken">Cancels inspection and delivery.</param>
    /// <returns>A task completing after any page or incoming-reference evidence is delivered.</returns>
    internal async ValueTask ObserveAsync(CompactIndexRecord record, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(record, nameof(record));
        Guard.NotNull(visitor, nameof(visitor));
        this.CheckAvailable();
        try
        {
            this.Step(record.PageNumber, cancellationToken);
            this.ValidatePage(record.PageNumber);
            if (!this.pages.TryGetValue(record.RootPageNumber, out PageState? root) || !root.DeclaredRoot || root.Owner != record.OwnerTDefPage)
            {
                throw Corrupt(record.PageNumber, "An index observation has no matching declared physical root.");
            }

            if (record.Kind == "index-page")
            {
                CompactIndexOwnershipRecord claim = new() { Kind = "page-ownership", PageNumber = record.PageNumber, OwnerTDefPage = record.OwnerTDefPage, RootPageNumber = record.RootPageNumber };
                PageState state = await this.ClaimAsync(claim, visitor, cancellationToken).ConfigureAwait(false);
                if (state.Observed && state.ObservedRoot != record.RootPageNumber)
                {
                    await this.EmitAsync(claim with { Kind = "shared-page", RelatedOwnerTDefPage = state.Owner, RelatedRootPageNumber = state.ObservedRoot }, visitor, cancellationToken).ConfigureAwait(false);
                }
                else if (!state.Observed)
                {
                    await this.EmitAsync(claim, visitor, cancellationToken).ConfigureAwait(false);
                }

                state.Observed = true;
                state.ObservedRoot = state.ObservedRoot == 0 ? record.RootPageNumber : state.ObservedRoot;
            }
            else if (record.Kind == "index-reference")
            {
                await this.ReferenceAsync(record, visitor, cancellationToken).ConfigureAwait(false);
            }
            else if (record.Kind == "index-terminal")
            {
                if (!root.Observed || !record.NavigableGraphAcyclic || !record.EntryEdgeFencesValidated)
                {
                    throw Corrupt(record.PageNumber, "The declared index root lacks a successful raw observation terminal.");
                }

                root.CompletedRoot = true;
            }
        }
        catch
        {
            this.failed = true;
            throw;
        }
    }

    /// <summary>Completes known ownership inspection after every declared root supplied a successful terminal.</summary>
    /// <param name="visitor">Consumes unresolved targets and the explicitly limited successful terminal.</param>
    /// <param name="cancellationToken">Cancels state inspection and delivery.</param>
    /// <returns>A task completing after all bounded known-reference evidence is delivered.</returns>
    internal async ValueTask CompleteAsync(Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        Guard.NotNull(visitor, nameof(visitor));
        this.CheckAvailable();
        try
        {
            foreach (KeyValuePair<long, PageState> item in this.pages)
            {
                this.Step(item.Key, cancellationToken);
                PageState state = item.Value;
                if (state.DeclaredRoot && !state.CompletedRoot)
                {
                    throw Corrupt(item.Key, "A declared physical index root has no successful raw terminal.");
                }

                if (!state.Observed && state.IncomingReferences > 0)
                {
                    await this.EmitAsync(
                        new() { Kind = "unobserved-reference-target", PageNumber = item.Key, OwnerTDefPage = state.Owner, RootPageNumber = state.IncomingRoot, SourcePageNumber = state.IncomingSource, Role = state.IncomingRole, EntryIndex = state.IncomingEntry, IncomingReferenceCount = state.IncomingReferences },
                        visitor,
                        cancellationToken).ConfigureAwait(false);
                }
            }

            await this.EmitAsync(
                new()
                {
                    Kind = "ownership-terminal",
                    DeclaredRootsObserved = true,
                    Coverage = "Every declared distinct physical root supplied a successful raw terminal. Known declared roots, observed index pages and incoming child/tail/previous/next references have consistent expected TDEF owners. Same-owner sharing and targets without page observations remain explicit. Missing roots, opaque incoming references, physical ownership of unobserved targets, allocation, native sharing semantics and exclusive whole-database ownership remain unproven; all storage stays pinned.",
                },
                visitor,
                cancellationToken).ConfigureAwait(false);
            this.completed = true;
        }
        catch
        {
            this.failed = true;
            throw;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (!this.disposed)
        {
            this.disposed = true;
            this.pages = [];
            this.reservation?.Dispose();
        }
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptIndex, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private async ValueTask ReferenceAsync(CompactIndexRecord record, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        if (record.TargetPageNumber is not { } target || record.Role is not ("child" or "tail" or "previous" or "next"))
        {
            throw Corrupt(record.PageNumber, "An incoming index reference has no recognized role or target.");
        }

        this.ValidatePage(target);
        if (this.references >= this.budget.MaxReferences)
        {
            throw Limit(record.PageNumber, "The incoming index observations exceed MaxReferences.");
        }

        this.budget.Accounting?.Charge(CompactDiscoveryCharge.References, 1, record.PageNumber);
        this.references++;
        CompactIndexOwnershipRecord incoming = new() { Kind = "incoming-reference", PageNumber = target, OwnerTDefPage = record.OwnerTDefPage, RootPageNumber = record.RootPageNumber, SourcePageNumber = record.PageNumber, Role = record.Role, EntryIndex = record.EntryIndex };
        PageState state = await this.ClaimAsync(incoming, visitor, cancellationToken).ConfigureAwait(false);
        state.IncomingReferences++;
        if (state.IncomingReferences == 1)
        {
            state.IncomingRoot = record.RootPageNumber;
            state.IncomingSource = record.PageNumber;
            state.IncomingRole = record.Role;
            state.IncomingEntry = record.EntryIndex;
        }

        await this.EmitAsync(incoming with { IncomingReferenceCount = state.IncomingReferences }, visitor, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<PageState> ClaimAsync(CompactIndexOwnershipRecord record, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        if (!this.pages.TryGetValue(record.PageNumber, out PageState? state))
        {
            if (this.pages.Count >= this.budget.MaxTrackedPages)
            {
                throw Limit(record.PageNumber, "The known index coordinates exceed MaxTrackedPages.");
            }

            state = new(record.OwnerTDefPage, record.RootPageNumber);
            this.pages.Add(record.PageNumber, state);
        }
        else if (state.Owner != record.OwnerTDefPage)
        {
            await this.EmitAsync(record with { Kind = "ownership-contradiction", RelatedOwnerTDefPage = state.Owner, RelatedRootPageNumber = state.FirstRoot }, visitor, cancellationToken).ConfigureAwait(false);
            throw Corrupt(record.PageNumber, "Known index roots, pages or incoming references have contradictory TDEF owners.");
        }

        return state;
    }

    private async ValueTask EmitAsync(CompactIndexOwnershipRecord record, Func<CompactIndexOwnershipRecord, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (this.records >= this.budget.MaxRecords)
        {
            throw Limit(record.PageNumber, "The index ownership observations exceed MaxRecords.");
        }

        this.budget.Accounting?.Charge(CompactDiscoveryCharge.Records, 1, record.PageNumber);
        this.records++;
        await visitor(record, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        this.CheckAvailable();
    }

    private void Step(long page, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (this.graphSteps >= this.budget.MaxGraphSteps)
        {
            throw Limit(page, "The index ownership inspection exceeds MaxGraphSteps.");
        }

        this.budget.Accounting?.Charge(CompactDiscoveryCharge.GraphSteps, 1, page);
        this.graphSteps++;
    }

    private void ValidatePage(long page)
    {
        if (page <= 0 || page >= this.pageCount)
        {
            throw Corrupt(page, "An index ownership coordinate is outside the source physical page range.");
        }
    }

    private void CheckAvailable()
    {
        Guard.ThrowIfDisposed(this.disposed, this);

        if (this.failed || this.completed)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "The ownership audit already completed or refused and cannot consume more observations.");
        }
    }

    private sealed class PageState(long owner, long root)
    {
        internal long Owner { get; } = owner;

        internal long FirstRoot { get; } = root;

        internal bool DeclaredRoot { get; set; }

        internal int DescriptorIndex { get; set; }

        internal bool CompletedRoot { get; set; }

        internal bool Observed { get; set; }

        internal long ObservedRoot { get; set; }

        internal long IncomingReferences { get; set; }

        internal long IncomingRoot { get; set; }

        internal long IncomingSource { get; set; }

        internal string? IncomingRole { get; set; }

        internal int? IncomingEntry { get; set; }
    }
}
