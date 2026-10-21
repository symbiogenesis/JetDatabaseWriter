namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Consumes only final accepted catalog recovery evidence from an immutable discovery source.</summary>
/// <remarks>Original header, complete directory and moved-chain evidence comes from recovery on the same immutable source. Target rechecks here do not constitute an independent recovery or ownership proof.</remarks>
/// <param name="pages">The bounded immutable session source.</param>
/// <param name="format">The verified source format.</param>
/// <param name="budget">The whole-session limits.</param>
/// <param name="accounting">The persistent session accounting.</param>
internal sealed class CompactRecoveredCatalogRows(IPageSource pages, JetFormat format, CompactDiscoveryBudget budget, CompactDiscoveryAccounting accounting) : IDisposable
{
    private readonly Dictionary<(long Page, int Row), Entry> bookmarks = [];
    private readonly Dictionary<long, RowLocation> identities = [];
    private readonly List<IDisposable> reservations = [];
    private TDefImage? catalog;
    private IDisposable? catalogStorage;

    /// <summary>Retains the verified catalog definition while recovered rows may be consumed.</summary>
    /// <param name="image">The inventory's catalog image.</param>
    internal void SetCatalog(TDefImage image)
    {
        if (this.bookmarks.Count == 0)
        {
            return;
        }

        this.catalogStorage?.Dispose();
        this.catalogStorage = accounting.ReserveWorkingBytes((16L * image.LogicalByteLength) + 65_536, 2);
        this.catalog = image;
    }

    /// <summary>Admits one final accepted recovery, never an arbitrary damaged-owner row.</summary>
    /// <param name="id">The independently matched complete signed catalog Id.</param>
    /// <param name="location">The recovery's verified original bookmark and stored-payload coordinates.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <param name="hasDamagedMovedOwner">Whether final accepted complete Id-index evidence also established the moved payload despite a damaged owner header.</param>
    /// <exception cref="JetCorruptDataException">Accepted evidence has invalid or conflicting identities.</exception>
    /// <exception cref="JetLimitationException">The bookmark or storage allowance is exhausted.</exception>
    internal void Accept(long id, RowLocation location, CancellationToken cancellationToken, bool hasDamagedMovedOwner = false)
    {
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, location.PageNumber);
        if (id < int.MinValue || id > int.MaxValue || location.PageNumber < 3 || location.PageNumber >= pages.PageCount
            || location.DataPageNumber < 3 || location.DataPageNumber >= pages.PageCount
            || location.RowIndex < 0 || location.RowIndex > byte.MaxValue || location.DataRowIndex < 0 || location.DataRowIndex > byte.MaxValue
            || location.RowStart < format.DataPage.RowsStart || location.RowSize <= 0 || location.RowSize > format.PageSize - location.RowStart)
        {
            throw Corrupt(location.PageNumber, "Accepted catalog recovery has invalid identity or payload coordinates.");
        }

        if (this.bookmarks.TryGetValue((location.PageNumber, location.RowIndex), out Entry previous))
        {
            if (previous.Id != id || previous.Location != location || previous.HasDamagedMovedOwner != hasDamagedMovedOwner)
            {
                throw Corrupt(location.PageNumber, "Accepted catalog recoveries disagree about one original bookmark.");
            }

            return;
        }

        if (this.identities.ContainsKey(id))
        {
            throw Corrupt(location.PageNumber, "Several accepted catalog recoveries have the same complete signed Id.");
        }

        if (this.bookmarks.Count >= budget.MaxCatalogObjects)
        {
            throw new JetLimitationException(JetErrorCode.ValueTooLarge, "Retained catalog recovery bookmarks exceed MaxCatalogObjects.");
        }

        // Includes both dictionaries, transient resize arrays, and reservation objects.
        IDisposable storage = accounting.ReserveWorkingBytes(512, location.PageNumber);
        this.reservations.Add(storage);
        this.bookmarks.Add((location.PageNumber, location.RowIndex), new(id, location, hasDamagedMovedOwner));
        this.identities.Add(id, location);
    }

    /// <summary>Tests whether a physical scan encountered an already recovered catalog identity.</summary>
    /// <param name="location">The original header bookmark being scanned.</param>
    /// <returns>True only for an exact accepted catalog bookmark.</returns>
    internal bool Contains(RowLocation location)
    {
        accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, location.PageNumber);
        return this.bookmarks.ContainsKey((location.PageNumber, location.RowIndex));
    }

    /// <summary>Streams every accepted recovered row using its original bookmark.</summary>
    /// <param name="visitor">The awaited borrowed-row consumer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task completed after all recovered rows have been consumed.</returns>
    internal async ValueTask ScanAsync(Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        foreach (Entry entry in this.bookmarks.Values)
        {
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
            accounting.Charge(CompactDiscoveryCharge.Rows, 1, entry.Location.PageNumber);
            await this.VisitAsync(entry, visitor, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Rechecks only an exact accepted bookmark before disclosing recovered index evidence.</summary>
    /// <param name="page">The original header page named by the index.</param>
    /// <param name="row">The original header slot named by the index.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The accepted payload coordinates, or null when the bookmark was not recovered.</returns>
    internal async ValueTask<RowLocation?> ResolveAsync(long page, int row, CancellationToken cancellationToken)
    {
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, page);
        if (!this.bookmarks.TryGetValue((page, row), out Entry entry))
        {
            return null;
        }

        await this.VisitAsync(
            entry,
            static (_, _, token) =>
            {
                token.ThrowIfCancellationRequested();
                return default;
            },
            cancellationToken).ConfigureAwait(false);
        return entry.Location;
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.bookmarks.Clear();
        this.identities.Clear();
        this.catalog = null;
        this.catalogStorage?.Dispose();
        this.catalogStorage = null;
        foreach (IDisposable reservation in this.reservations)
        {
            reservation.Dispose();
        }

        this.reservations.Clear();
    }

    private static JetCorruptDataException Corrupt(long page, string reason)
        => new(JetErrorCode.CorruptCatalog, reason, new JetErrorInfo { TableName = "MSysObjects", PageNumber = page, Reason = reason });

    private async ValueTask VisitAsync(Entry entry, Func<RowLocation, ReadOnlyMemory<byte>, CancellationToken, ValueTask> visitor, CancellationToken cancellationToken)
    {
        TDefImage definition = this.catalog ?? throw Corrupt(entry.Location.PageNumber, "The catalog definition is unavailable for recovered-row validation.");
        RowLocation location = entry.Location;
        using IDisposable storage = accounting.ReserveWorkingBytes((2L * format.PageSize) + 65_536, location.DataPageNumber);
        pages.ThrowIfDisposedOrCancelled(cancellationToken);
        byte[] page = await pages.ReadUncachedPageAsync(location.DataPageNumber, cancellationToken).ConfigureAwait(false);
        try
        {
            this.ValidatePayload(page, location, entry.HasDamagedMovedOwner, cancellationToken);
            ReadOnlyMemory<byte> bytes = page.AsMemory(location.RowStart, location.RowSize);
            CompactRawField id = default;
            await CompactRawFieldReader.ScanAsync(
                format,
                definition,
                location,
                bytes,
                (field, token) =>
                {
                    token.ThrowIfCancellationRequested();
                    accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, location.DataPageNumber);
                    if (field.Column.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                    {
                        id = field;
                    }

                    return default;
                },
                cancellationToken).ConfigureAwait(false);
            if (id.Column is null || id.Kind != CompactRawFieldKind.Stored || id.Column.Type != ColumnType.LongIntegerType
                || id.RawBytes.Length != sizeof(int) || BinaryPrimitives.ReadInt32LittleEndian(id.RawBytes.Span) != entry.Id)
            {
                throw Corrupt(location.DataPageNumber, "The accepted recovered catalog bookmark no longer has its complete signed stored Id.");
            }

            accounting.Charge(CompactDiscoveryCharge.Records, 1, location.PageNumber);
            await visitor(location, bytes, cancellationToken).ConfigureAwait(false);
            pages.ThrowIfDisposedOrCancelled(cancellationToken);
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    private void ValidatePayload(byte[] page, RowLocation location, bool hasDamagedMovedOwner, CancellationToken cancellationToken)
    {
        if (page.Length < format.PageSize || page[0] != Constants.PageTypes.Data
            || (location.IsOverflow && !hasDamagedMovedOwner && Ru32(page, format.DataPage.TDefOff) != 2))
        {
            throw Corrupt(location.DataPageNumber, "The accepted recovered catalog payload is not its verified data page.");
        }

        int count = Ru16(page, format.DataPage.NumRows);
        int directoryEnd = format.DataPage.RowsStart + (count * 2);
        if (directoryEnd > format.PageSize || location.DataRowIndex >= count)
        {
            throw Corrupt(location.DataPageNumber, "The accepted recovered catalog payload slot is absent.");
        }

        int raw = Ru16(page, format.DataPage.RowsStart + (location.DataRowIndex * 2));
        int start = raw & Constants.DataPage.RowOffsetMask;
        int end = format.PageSize;
        if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.DeletedRowFlag)) != 0
            || (raw & Constants.DataPage.DeletedRowFlag) != 0 != location.IsOverflow || start != location.RowStart || start < directoryEnd)
        {
            throw Corrupt(location.DataPageNumber, "The accepted recovered catalog payload flags or coordinates changed.");
        }

        for (int row = 0; row < count; row++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            accounting.Charge(CompactDiscoveryCharge.DirectorySlots, 1, location.DataPageNumber);
            int candidate = Ru16(page, format.DataPage.RowsStart + (row * 2)) & Constants.DataPage.RowOffsetMask;
            if (candidate > start && candidate < end)
            {
                end = candidate;
            }
        }

        if (end - start != location.RowSize)
        {
            throw Corrupt(location.DataPageNumber, "The accepted recovered catalog payload size changed.");
        }
    }

    private readonly record struct Entry(long Id, RowLocation Location, bool HasDamagedMovedOwner);
}
