namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independently compared active and retained inactive catalog bookmarks for one immutable destination.</summary>
/// <remarks>
/// The immutable source, destination and ledgers must have distinct backing storage, including through wrappers.
/// Complete raw table comparison establishes active membership without trusting installed map hints. Every original
/// source DATA page is separately compared before exact original catalog bookmarks may be excluded, including
/// independently recovered damaged-owner headers and moved payloads.
/// Missing source ownership can still hide identities. This evidence establishes neither complete source discovery,
/// native behavior, index completeness, incoming ownership nor permission to reclaim, publish or replace storage.
/// The caller keeps both page sources immutable for the complete evidence lifetime.
/// </remarks>
internal sealed class CompactActiveCatalogRows : IDisposable
{
    private readonly IPageSource destination;
    private readonly long destinationPages;
    private readonly int pageSize;
    private readonly HashSet<uint> inactive = [];
    private readonly HashSet<uint> active = [];
    private IDisposable? reservation;

    private CompactActiveCatalogRows(IPageSource destination)
    {
        this.destination = destination;
        this.destinationPages = destination.PageCount;
        this.pageSize = destination.PageSize;
    }

    /// <summary>Gets the independently compared active catalog row count.</summary>
    internal long ActiveRows => this.active.Count;

    /// <summary>Gets admitted destination, ledger and original-prefix comparison bytes; source scans also use session accounting.</summary>
    internal long ReadBytes { get; private set; }

    /// <summary>Gets a value indicating whether the complete database is validated; always false.</summary>
    public bool DatabaseValidated { get; }

    /// <summary>Gets a value indicating whether complete incoming ownership permits reclamation; always false.</summary>
    public bool OwnershipProven { get; }

    /// <summary>Independently compares complete raw catalog storage before retaining exact row memberships.</summary>
    /// <param name="source">The immutable original discovery source and persistent accounting.</param>
    /// <param name="catalog">The immutable original MSysObjects definition.</param>
    /// <param name="destination">The exact reopened immutable destination page source later inventoried.</param>
    /// <param name="rowLedger">The distinct immutable complete relocation ledger.</param>
    /// <param name="allocationLedger">The distinct immutable complete original-column LVAL allocation ledger.</param>
    /// <param name="allocation">The untrusted complete private catalog table allocation.</param>
    /// <param name="indexes">Separately verified physical index replacements in original order.</param>
    /// <param name="budget">Read, row and simultaneous storage limits.</param>
    /// <param name="cancellationToken">Cancels preflight, reads and source scans.</param>
    /// <returns>Disposable evidence bound to the exact destination identity and geometry.</returns>
    /// <exception cref="ArgumentException">The ledgers cannot be read independently.</exception>
    /// <exception cref="JetCorruptDataException">Raw storage, membership, retained prefix or catalog identities disagree.</exception>
    /// <exception cref="JetLimitationException">A row, read or working-storage allowance is exhausted.</exception>
    internal static async ValueTask<CompactActiveCatalogRows> ValidateAsync(
        CompactDiscoverySession source,
        TDefImage catalog,
        IPageSource destination,
        Stream rowLedger,
        Stream allocationLedger,
        CompactRawTableBuilderResult allocation,
        IReadOnlyList<CompactTDefIndexReferences> indexes,
        CompactActiveCatalogBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        return await source.WithCatalogRecoveryAsync(
            token => ValidateCoreAsync(source, catalog, destination, rowLedger, allocationLedger, allocation, indexes, budget, token),
            cancellationToken).ConfigureAwait(false);
    }

    private static async ValueTask<CompactActiveCatalogRows> ValidateCoreAsync(
        CompactDiscoverySession source,
        TDefImage catalog,
        IPageSource destination,
        Stream rowLedger,
        Stream allocationLedger,
        CompactRawTableBuilderResult allocation,
        IReadOnlyList<CompactTDefIndexReferences> indexes,
        CompactActiveCatalogBudget budget,
        CancellationToken cancellationToken)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(catalog, nameof(catalog));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(rowLedger, nameof(rowLedger));
        Guard.NotNull(allocationLedger, nameof(allocationLedger));
        Guard.NotNull(allocation, nameof(allocation));
        Guard.NotNull(indexes, nameof(indexes));
        Guard.NotNull(budget, nameof(budget));
        Guard.NotNull(budget.TableValidator, nameof(budget));
        Guard.InRange(budget.MaxRows, 1L, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 1L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        destination.ThrowIfDisposedOrCancelled(cancellationToken);
        if (!rowLedger.CanRead || !rowLedger.CanSeek || !allocationLedger.CanRead || !allocationLedger.CanSeek
            || ReferenceEquals(rowLedger, allocationLedger)
            || (rowLedger is FileStream rowsFile && allocationLedger is FileStream valuesFile
                && string.Equals(rowsFile.Name, valuesFile.Name, StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException("Catalog ledgers must be distinct immutable readable seekable streams.", nameof(rowLedger));
        }

        IPageSource original = source.SourcePages;
        JetFormat format = source.SourceFormat;
        if (ReferenceEquals(original, destination) || destination.PageSize != format.PageSize
            || destination.PageCount < original.PageCount || destination.PageCount > 0x1000000 || allocation.Data.FirstPageNumber < original.PageCount)
        {
            throw Corrupt("Active catalog evidence requires a distinct append-only destination with the original source prefix.");
        }

        if (allocation.Data.RowCount < 0 || allocation.Data.RowCount > budget.MaxRows)
        {
            throw Limit("Active catalog membership exceeds MaxRows.");
        }

        long retained = (256L * allocation.Data.RowCount) + 65_536 + (2L * format.PageSize);
        if (retained >= budget.MaxWorkingBytes)
        {
            throw Limit("Active catalog membership exceeds MaxWorkingBytes.");
        }

        CompactActiveCatalogRows? pending = new(destination);
        try
        {
            CompactActiveCatalogRows evidence = pending;
            evidence.reservation = source.Accounting.ReserveWorkingBytes(retained, 2);
            var reads = new ReadAdmission(budget.MaxReadBytes);
            await using var privateFile = new PageStream(destination, reads);
            await using var comparedRows = new AdmittedStream(rowLedger, reads);
            await using var comparedAllocations = new AdmittedStream(allocationLedger, reads);
            CompactRawTableValidatorBudget table = budget.TableValidator;
            await CompactRawTableValidator.ValidateAsync(
                source,
                2,
                catalog,
                privateFile,
                comparedRows,
                comparedAllocations,
                allocation,
                indexes,
                new()
                {
                    RawValues = table.RawValues,
                    Maps = table.Maps,
                    MaxReadBytes = Math.Min(table.MaxReadBytes, budget.MaxReadBytes),
                    MaxWorkingBytes = Math.Min(table.MaxWorkingBytes, budget.MaxWorkingBytes - retained),
                },
                cancellationToken).ConfigureAwait(false);

            // Includes original overflow headers, intermediate slots and deleted payload storage, not only final row bytes.
            for (long number = 3; number < original.PageCount; number++)
            {
                source.Accounting.Charge(CompactDiscoveryCharge.PageReads, 1, number);
                reads.Charge(format.PageSize);
                byte[] page = await original.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
                try
                {
                    if (page[0] != Constants.PageTypes.Data)
                    {
                        continue;
                    }

                    reads.Charge(format.PageSize);
                    byte[] installed = await destination.ReadUncachedPageAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        if (!page.AsSpan(0, format.PageSize).SequenceEqual(installed.AsSpan(0, format.PageSize)))
                        {
                            throw Corrupt("A retained original catalog data page differs from the immutable source.");
                        }
                    }
                    finally
                    {
                        PageBuffers.Return(installed);
                    }
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            var ids = new HashSet<int>();
            byte[] record = new byte[8];
            rowLedger.Position = 0;
            await source.ScanRowsAsync(
                2,
                async (location, bytes, token) =>
            {
                int? id = null;
                await CompactRawFieldReader.ScanAsync(
                    format,
                    catalog,
                    location,
                    bytes,
                    (field, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        source.Accounting.Charge(CompactDiscoveryCharge.Fields, 1, location.PageNumber);
                        if (field.Column.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                        {
                            if (field.Kind != CompactRawFieldKind.Stored || field.Column.Type != ColumnType.LongIntegerType || field.RawBytes.Length != sizeof(int))
                            {
                                throw Corrupt("An original catalog row has no complete signed stored Id.");
                            }

                            id = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                        }

                        return default;
                    },
                    token).ConfigureAwait(false);
                if (id is not { } identity || !ids.Add(identity))
                {
                    throw Corrupt("Original catalog rows have absent or duplicate complete signed stored Ids.");
                }

                await ReadRecordAsync(rowLedger, record, reads, token).ConfigureAwait(false);
                uint originalBookmark = Bookmark(location.PageNumber, location.RowIndex);
                uint activeBookmark = BinaryPrimitives.ReadUInt32LittleEndian(record.AsSpan(4));
                if (BinaryPrimitives.ReadUInt32LittleEndian(record) != originalBookmark
                    || !evidence.inactive.Add(originalBookmark) || !evidence.active.Add(activeBookmark)
                    || evidence.active.Count > allocation.Data.RowCount)
                {
                    throw Corrupt("Catalog row membership disagrees with the independently compared complete relocation ledger.");
                }
            },
                cancellationToken).ConfigureAwait(false);
            if (evidence.active.Count != allocation.Data.RowCount || rowLedger.Length != allocation.Data.RelocationBytes)
            {
                throw Corrupt("Catalog row membership does not cover the complete independently compared allocation.");
            }

            evidence.CheckDestination(destination, cancellationToken);
            evidence.ReadBytes = reads.Bytes;
            pending = null;
            return evidence;
        }
        finally
        {
            pending?.Dispose();
        }
    }

    /// <summary>Checks that exclusions are scoped to the exact immutable destination before inventory wraps it.</summary>
    /// <param name="pages">The unwrapped inventory source.</param>
    /// <param name="token">The cancellation token.</param>
    /// <exception cref="JetCorruptDataException">The destination identity, geometry or evidence lifetime disagrees.</exception>
    internal void CheckDestination(IPageSource pages, CancellationToken token)
    {
        pages.ThrowIfDisposedOrCancelled(token);
        if (this.reservation is null || !ReferenceEquals(pages, this.destination)
            || pages.PageSize != this.pageSize || pages.PageCount != this.destinationPages)
        {
            throw Corrupt("Active catalog evidence belongs to a different, changed or expired destination.");
        }
    }

    /// <summary>Excludes only an exact independently compared retained header; any unknown physical row refuses.</summary>
    /// <param name="page">The physically observed live catalog header page.</param>
    /// <param name="row">The physically observed live catalog header slot.</param>
    /// <returns>True only for an independently compared active row.</returns>
    /// <exception cref="JetCorruptDataException">The physical row has no independently compared membership.</exception>
    internal bool Include(long page, int row)
    {
        this.CheckDestination(this.destination, default);
        uint bookmark = Bookmark(page, row);
        if (this.inactive.Contains(bookmark))
        {
            return false;
        }

        if (!this.active.Contains(bookmark))
        {
            throw Corrupt("A physically live catalog row has no independently verified active or retained membership.");
        }

        return true;
    }

    /// <summary>Excludes an exact retained catalog bookmark even when its damaged owner mimics another table.</summary>
    /// <param name="page">The physically observed live header page.</param>
    /// <param name="row">The physically observed live header slot.</param>
    /// <returns>True only for an independently compared retained original catalog bookmark.</returns>
    internal bool ExcludesRetained(long page, int row)
    {
        this.CheckDestination(this.destination, default);
        return this.inactive.Contains(Bookmark(page, row));
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.inactive.Clear();
        this.active.Clear();
        Interlocked.Exchange(ref this.reservation, null)?.Dispose();
    }

    private static uint Bookmark(long page, int row)
    {
        if (page is <= 0 or > 0xFFFFFF || row is < 0 or > byte.MaxValue)
        {
            throw Corrupt("A catalog bookmark exceeds the native identity range.");
        }

        return checked(((uint)page << 8) | (uint)row);
    }

    private static JetCorruptDataException Corrupt(string reason) => new(JetErrorCode.CorruptCatalog, reason);

    private static JetLimitationException Limit(string reason) => new(JetErrorCode.ValueTooLarge, reason);

    private static async ValueTask ReadRecordAsync(Stream stream, byte[] record, ReadAdmission reads, CancellationToken token)
    {
        int offset = 0;
        while (offset < record.Length)
        {
            token.ThrowIfCancellationRequested();
            reads.Charge(record.Length - offset);
            int count = await stream.ReadAsync(record.AsMemory(offset), token).ConfigureAwait(false);
            if (count <= 0 || count > record.Length - offset)
            {
                throw Corrupt("The complete catalog relocation ledger is truncated.");
            }

            offset = StreamRead.AdvanceRead(record.Length, offset, count);
        }
    }

    private sealed class ReadAdmission(long maximum)
    {
        internal long Bytes { get; private set; }

        internal void Charge(int count)
        {
            if (count > maximum - this.Bytes)
            {
                throw Limit("Independent active catalog comparison exceeds MaxReadBytes.");
            }

            this.Bytes += count;
        }
    }

    /// <summary>Non-owning ledger view admitting all nested requested bytes, including failed reads and retries.</summary>
    /// <param name="inner">The caller-owned immutable ledger.</param>
    /// <param name="reads">The cumulative physical and ledger admission.</param>
    private sealed class AdmittedStream(Stream inner, ReadAdmission reads) : Stream
    {
        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => inner.Length;

        public override long Position { get => inner.Position; set => inner.Position = value; }

        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            reads.Charge(count);
            return inner.Read(buffer, offset, count);
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            reads.Charge(buffer.Length);
            return inner.ReadAsync(buffer, cancellationToken);
        }

        public override long Seek(long offset, SeekOrigin origin) => inner.Seek(offset, origin);

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>Non-owning decrypted page view ensuring comparison reads the exact evidence-bound source.</summary>
    /// <param name="pages">The caller-owned immutable reopened destination.</param>
    /// <param name="reads">The cumulative physical read admission.</param>
    private sealed class PageStream(IPageSource pages, ReadAdmission reads) : Stream
    {
        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => checked(pages.PageCount * pages.PageSize);

        public override long Position { get; set; }

        public override void Flush() => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int total = 0;
            while (total < buffer.Length && this.Position < this.Length)
            {
                pages.ThrowIfDisposedOrCancelled(cancellationToken);
                reads.Charge(pages.PageSize);
                byte[] page = await pages.ReadUncachedPageAsync(this.Position / pages.PageSize, cancellationToken).ConfigureAwait(false);
                try
                {
                    int start = checked((int)(this.Position % pages.PageSize));
                    int count = Math.Min(buffer.Length - total, pages.PageSize - start);
                    page.AsMemory(start, count).CopyTo(buffer.Slice(total, count));
                    this.Position += count;
                    total += count;
                }
                finally
                {
                    PageBuffers.Return(page);
                }
            }

            return total;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
