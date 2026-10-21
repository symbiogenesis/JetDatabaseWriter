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
    private long activeWorkingBytes;

    /// <summary>Initializes a new instance of the <see cref="CompactDiscoveryAccounting"/> class.</summary>
    /// <param name="budget">The fixed whole-session limits.</param>
    internal CompactDiscoveryAccounting(CompactDiscoveryBudget budget)
    {
        Guard.NotNull(budget, nameof(budget));
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

        return new Reservation(this, bytes);
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

    private sealed class Reservation(CompactDiscoveryAccounting owner, long bytes) : IDisposable
    {
        private CompactDiscoveryAccounting? owner = owner;

        public void Dispose() => Interlocked.Exchange(ref this.owner, null)?.Release(bytes);
    }
}
