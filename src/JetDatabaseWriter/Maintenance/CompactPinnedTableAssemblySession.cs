namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Owns cumulative source, private I/O, observation and output admission across pinned table operations.</summary>
/// <remarks>
/// Build and independent validation normally use separate sessions. Repeated operations, including failed attempts,
/// retain their work charges. Operations must run serially; overlapping assembly calls are refused rather than queued.
/// The caller keeps the source immutable, owns every stream and retained result, and serializes any direct discovery work
/// with assembly calls. Result evidence and caller buffers are outside working-storage admission. Completion proves no
/// complete database coverage, ownership, reclamation or publication permission.
/// </remarks>
internal sealed class CompactPinnedTableAssemblySession
{
    private readonly long sourcePages;
    private int active;

    /// <summary>Initializes a new instance of the <see cref="CompactPinnedTableAssemblySession"/> class.</summary>
    /// <param name="source">The caller-owned immutable decrypted original source.</param>
    /// <param name="format">The verified native format.</param>
    /// <param name="budget">Fixed whole-session limits, including per-component allowances.</param>
    internal CompactPinnedTableAssemblySession(IPageSource source, JetFormat format, CompactPinnedTableAssemblyBudget budget)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.Discovery, nameof(budget));
        Guard.NotNull(budget.TableBuilder, nameof(budget));
        Guard.NotNull(budget.TableValidator, nameof(budget));
        Guard.NotNull(budget.Index, nameof(budget));
        Guard.NotNull(budget.Definition, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxOutputBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxObservedEntries, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxObservedKeyBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        this.Source = source;
        this.Format = format;
        this.Budget = budget;
        this.sourcePages = source.PageCount;
        this.Discovery = new(source, format, SourceBudget(budget));
        this.DefinitionSource = new AdmittedSource(this);
    }

    /// <summary>Gets the original immutable source identity.</summary>
    internal IPageSource Source { get; }

    /// <summary>Gets the matching verified source format.</summary>
    internal JetFormat Format { get; }

    /// <summary>Gets the fixed cumulative and per-component allowances.</summary>
    internal CompactPinnedTableAssemblyBudget Budget { get; }

    /// <summary>Gets persistent discovery admission for serialized source inventory, traversal and table work.</summary>
    internal CompactDiscoverySession Discovery { get; }

    /// <summary>Gets source reads for pinned-chain work sharing discovery admission.</summary>
    internal IPageSource DefinitionSource { get; }

    /// <summary>Gets cumulative private destination and scratch read requests, including interrupted requests.</summary>
    internal long ReadBytes { get; private set; }

    /// <summary>Gets cumulative destination and scratch write requests, including failed writes.</summary>
    internal long WrittenBytes { get; private set; }

    /// <summary>Gets admitted appended construction bytes and independently compared allocation bytes.</summary>
    internal long OutputBytes { get; private set; }

    /// <summary>Gets cumulative original entries delivered to construction or independent validation.</summary>
    internal long ObservedEntries { get; private set; }

    /// <summary>Gets cumulative original stored-key bytes delivered to construction or validation.</summary>
    internal long ObservedKeyBytes { get; private set; }

    /// <summary>Begins an explicitly serialized operation without resetting any work counter.</summary>
    /// <param name="initialLength">The private destination length before this invocation.</param>
    /// <param name="token">Cancels admission.</param>
    /// <returns>The operation scope, released on success, refusal, cancellation or I/O failure.</returns>
    /// <exception cref="JetOperationException">An assembly call is already active in this session.</exception>
    internal Operation Enter(long initialLength, CancellationToken token)
    {
        this.CheckSource(token);
        if (Interlocked.CompareExchange(ref this.active, 1, 0) != 0)
        {
            throw new JetOperationException(JetErrorCode.InvalidOperationState, "A pinned table assembly operation is already active in this session.");
        }

        return new(this, initialLength);
    }

    /// <summary>Caps cumulative source working storage to the whole assembly-session allowance.</summary>
    /// <param name="budget">The whole-session limits.</param>
    /// <returns>The shared discovery limits.</returns>
    private static CompactDiscoveryBudget SourceBudget(CompactPinnedTableAssemblyBudget budget)
        => budget.Discovery with { MaxWorkingBytes = Math.Min(budget.Discovery.MaxWorkingBytes, budget.MaxWorkingBytes) };

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private void CheckSource(CancellationToken token)
    {
        this.Source.ThrowIfDisposedOrCancelled(token);
        if (this.Source.PageSize != this.Format.PageSize || this.Source.PageCount != this.sourcePages)
        {
            throw new JetCorruptDataException(JetErrorCode.MalformedValue, "The immutable pinned table assembly source geometry changed.");
        }
    }

    private void ChargeOutput(long bytes)
    {
        long maximum = Math.Min(this.Budget.MaxOutputBytes, Math.Min(this.Budget.MaxPages, 0x1000000L) * this.Format.PageSize);
        if (bytes < 0 || bytes > maximum - this.OutputBytes)
        {
            throw Limit("Pinned table assembly exceeds cumulative MaxPages or MaxOutputBytes.");
        }

        this.OutputBytes += bytes;
    }

    /// <summary>Shares one invocation's append high-water mark and the session's cumulative admission.</summary>
    internal sealed class Operation : IDisposable
    {
        private readonly CompactPinnedTableAssemblySession session;
        private long outputEnd;
        private int disposed;

        /// <summary>Initializes a new instance of the <see cref="Operation"/> class.</summary>
        /// <param name="session">The serialized session.</param>
        /// <param name="initialLength">The invocation's original append point.</param>
        internal Operation(CompactPinnedTableAssemblySession session, long initialLength)
        {
            this.session = session;
            this.outputEnd = initialLength;
        }

        /// <summary>Gets read requests charged by this operation without resetting the session counter.</summary>
        internal long ReadBytes { get; private set; }

        /// <summary>Charges an independent complete allocation comparison before traversal.</summary>
        /// <param name="pages">The admitted allocation's page count.</param>
        internal void Compare(long pages) => this.session.ChargeOutput(checked(pages * this.session.Format.PageSize));

        /// <summary>Charges a private read before its I/O, retaining charges after failures.</summary>
        /// <param name="count">The requested byte count.</param>
        /// <exception cref="JetLimitationException">The cumulative read allowance is exhausted.</exception>
        internal void Read(int count)
        {
            if (count > this.session.Budget.MaxReadBytes - this.session.ReadBytes)
            {
                throw Limit("Pinned table assembly exceeds cumulative MaxReadBytes.");
            }

            this.session.ReadBytes += count;
            this.ReadBytes += count;
        }

        /// <summary>Charges a write request and any newly admitted append extent before I/O.</summary>
        /// <param name="position">The stream position before the write.</param>
        /// <param name="count">The requested byte count.</param>
        /// <param name="destination">Whether cumulative appended output limits also apply.</param>
        /// <exception cref="JetLimitationException">The cumulative write or output allowance is exhausted.</exception>
        internal void Write(long position, int count, bool destination)
        {
            if (count > this.session.Budget.MaxWriteBytes - this.session.WrittenBytes)
            {
                throw Limit("Pinned table assembly exceeds cumulative MaxWriteBytes.");
            }

            if (destination)
            {
                if (position < 0 || position > long.MaxValue - count)
                {
                    throw Limit("Pinned table assembly output position exceeds native storage.");
                }

                long end = position + count;
                if (end > this.outputEnd)
                {
                    this.session.ChargeOutput(end - this.outputEnd);
                    this.outputEnd = end;
                }
            }

            this.session.WrittenBytes += count;
        }

        /// <summary>Admits every original entry and key before delivering it to a component.</summary>
        /// <param name="entries">Independent immutable original observations.</param>
        /// <param name="token">Cancels enumeration and admission.</param>
        /// <returns>The admitted original entries.</returns>
        /// <exception cref="JetCorruptDataException">An original stored-key observation is null.</exception>
        /// <exception cref="JetLimitationException">The cumulative entry or key-byte allowance is exhausted.</exception>
        internal async IAsyncEnumerable<IndexEntry> ObserveAsync(IAsyncEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
        {
            await foreach (IndexEntry entry in entries.WithCancellation(token).ConfigureAwait(false))
            {
                token.ThrowIfCancellationRequested();
                if (entry.Key is null)
                {
                    throw new JetCorruptDataException(JetErrorCode.MalformedValue, "An immutable original stored-key observation is null.");
                }

                if (this.session.ObservedEntries >= this.session.Budget.MaxObservedEntries
                    || entry.Key.Length > this.session.Budget.MaxObservedKeyBytes - this.session.ObservedKeyBytes)
                {
                    throw Limit("Pinned table assembly exceeds cumulative original-entry or key-byte allowances.");
                }

                this.session.ObservedEntries++;
                this.session.ObservedKeyBytes += entry.Key.Length;
                yield return entry;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref this.disposed, 1) == 0)
            {
                Volatile.Write(ref this.session.active, 0);
            }
        }
    }

    private sealed class AdmittedSource(CompactPinnedTableAssemblySession session) : IPageSource
    {
        public int PageSize => session.Format.PageSize;

        public long PageCount => session.sourcePages;

        public bool IsDisposed => session.Source.IsDisposed;

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            session.CheckSource(cancellationToken);
            session.Discovery.Accounting.Charge(CompactDiscoveryCharge.PageReads, 1, pageNumber);
            byte[] page = await session.Source.ReadUncachedPageAsync(pageNumber, cancellationToken).ConfigureAwait(false);
            try
            {
                session.CheckSource(cancellationToken);
                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        public void ThrowIfDisposed() => session.Source.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => session.CheckSource(cancellationToken);
    }
}
