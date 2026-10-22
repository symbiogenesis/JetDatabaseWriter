namespace JetDatabaseWriter.Maintenance;

using System;
using System.Threading;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;

/// <summary>The cumulative work categories shared by compact-discovery primitives.</summary>
internal enum CompactDiscoveryCharge
{
    PageReads = 0,
    Objects = 1,
    Rows = 2,
    DirectorySlots = 3,
    LongValues = 4,
    LongValueBytes = 5,
    LongValuePayloadBytes = 6,
    LongValueRows = 7,
    IndexPages = 8,
    IndexEntries = 9,
    IndexCanonicalBytes = 10,
    References = 11,
    GraphSteps = 12,
    Records = 13,
    CatalogObjects = 14,
    SchemaBytes = 15,
    MapSteps = 16,
    Diagnostics = 17,
    Fields = 18,
}

/// <summary>Owns cumulative counters and simultaneous working-storage admission for one discovery session.</summary>
internal sealed class CompactDiscoveryAccounting
{
    private readonly long[] counts = new long[19];
    private readonly long[] limits;
    private readonly long maxWorkingBytes;
    private readonly CompactDiscoveryAccounting? parent;
    private readonly int parentDepth;
    private long activeWorkingBytes;

    /// <summary>Initializes a new instance of the <see cref="CompactDiscoveryAccounting"/> class.</summary>
    /// <param name="budget">The fixed whole-session limits.</param>
    /// <param name="parent">Optional surrounding admission, bounded to sixteen parent levels.</param>
    /// <exception cref="ArgumentException">The parent chain exceeds its fixed depth bound.</exception>
    internal CompactDiscoveryAccounting(CompactDiscoveryBudget budget, CompactDiscoveryAccounting? parent = null)
    {
        Guard.NotNull(budget, nameof(budget));
        if (parent?.parentDepth >= 16)
        {
            throw new ArgumentException("Nested discovery accounting exceeds sixteen parent levels.", nameof(parent));
        }

        this.parent = parent;
        this.parentDepth = parent is null ? 0 : parent.parentDepth + 1;
        this.limits = [budget.MaxPageReads, budget.MaxObjects, budget.MaxRows, budget.MaxDirectorySlots, budget.MaxLongValues, budget.MaxLongValueBytes, budget.MaxLongValuePayloadBytes, budget.MaxLongValueRows, budget.MaxIndexPages, budget.MaxIndexEntries, budget.MaxIndexCanonicalBytes, budget.MaxReferences, budget.MaxGraphSteps, budget.MaxRecords, budget.MaxCatalogObjects, budget.MaxSchemaBytes, budget.MaxMapSteps, budget.MaxDiagnostics, budget.MaxFields];
        foreach (long limit in this.limits)
        {
            Guard.InRange(limit, 1L, long.MaxValue, nameof(budget));
        }

        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        this.maxWorkingBytes = budget.MaxWorkingBytes;
    }

    /// <summary>Gets the cumulative delivered live-row count.</summary>
    internal long Rows => this.ReadCount(CompactDiscoveryCharge.Rows);

    /// <summary>Gets the cumulative reconstructed index-entry count.</summary>
    internal long IndexEntries => this.ReadCount(CompactDiscoveryCharge.IndexEntries);

    /// <summary>Gets the cumulative external long-value row count.</summary>
    internal long LongValueRows => this.ReadCount(CompactDiscoveryCharge.LongValueRows);

    /// <summary>Gets the cumulative attempted physical read count.</summary>
    internal long PageReads => this.ReadCount(CompactDiscoveryCharge.PageReads);

    /// <summary>Gets the currently admitted working storage.</summary>
    internal long ActiveWorkingBytes => Volatile.Read(ref this.activeWorkingBytes);

    /// <summary>Charges work before its allocation, I/O, traversal or delivery. Failed operations retain their charges.</summary>
    /// <param name="charge">The cumulative work category.</param>
    /// <param name="count">The nonnegative amount to charge.</param>
    /// <param name="page">The relevant source page, when known.</param>
    /// <exception cref="JetLimitationException">The cumulative category allowance is exhausted.</exception>
    internal void Charge(CompactDiscoveryCharge charge, long count, long page = 0)
    {
        Guard.InRange(count, 0L, long.MaxValue, nameof(count));
        int index = (int)charge;
        if (!TryAddWithinLimit(ref this.counts[index], count, this.limits[index]))
        {
            throw Limit(page, $"The aggregate discovery allowance for {charge} is exhausted.");
        }

        // A refused parent leaves the already admitted child attempt charged;
        // no caller work has begun. Parent counters follow the same rule.
        this.parent?.Charge(charge, count, page);
    }

    /// <summary>Reserves simultaneous scan-owned storage before allocating it, releasing admission on disposal.</summary>
    /// <param name="bytes">The conservative byte reservation.</param>
    /// <param name="page">The relevant source page, when known.</param>
    /// <returns>A reservation released exactly once.</returns>
    /// <exception cref="JetLimitationException">The simultaneous working-storage allowance is exhausted.</exception>
    internal IDisposable ReserveWorkingBytes(long bytes, long page = 0)
    {
        Guard.InRange(bytes, 0L, long.MaxValue, nameof(bytes));
        if (!TryAddWithinLimit(ref this.activeWorkingBytes, bytes, this.maxWorkingBytes))
        {
            throw Limit(page, "The aggregate discovery working-storage reservation exceeds MaxWorkingBytes.");
        }

        IDisposable? parentReservation = null;
        try
        {
#pragma warning disable CA2000 // Ownership transfers to Reservation on success; finally releases every pending parent lease.
            parentReservation = this.parent?.ReserveWorkingBytes(bytes, page);
#pragma warning restore CA2000
            var reservation = new Reservation(this, bytes, parentReservation);
            parentReservation = null;
            return reservation;
        }
        catch
        {
            this.Release(bytes);
            throw;
        }
        finally
        {
            parentReservation?.Dispose();
        }
    }

    /// <summary>Reads one cumulative allowance without resetting it.</summary>
    /// <param name="charge">The work category to inspect.</param>
    /// <returns>The amount already charged.</returns>
    internal long ReadCount(CompactDiscoveryCharge charge) => Volatile.Read(ref this.counts[(int)charge]);

    private static JetLimitationException Limit(long page, string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { PageNumber = page, Reason = reason });

    private static bool TryAddWithinLimit(ref long value, long amount, long limit)
    {
        while (true)
        {
            long current = Volatile.Read(ref value);
            if (amount > limit - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref value, current + amount, current) == current)
            {
                return true;
            }
        }
    }

    private void Release(long bytes) => Interlocked.Add(ref this.activeWorkingBytes, -bytes);

    private sealed class Reservation(CompactDiscoveryAccounting owner, long bytes, IDisposable? parentReservation) : IDisposable
    {
        private CompactDiscoveryAccounting? owner = owner;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref this.owner, null) is { } reserved)
            {
                reserved.Release(bytes);
                parentReservation?.Dispose();
            }
        }
    }
}
