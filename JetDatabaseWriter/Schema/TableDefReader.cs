namespace JetDatabaseWriter.Schema;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Reads owned layouts from immutable, write-observed TDEF images.</summary>
internal sealed class TableDefReader : IDisposable, IPageWriteObserver
{
    private readonly IPageSource pages;
    private readonly JetFormat format;
    private readonly bool cacheResults;
    private readonly TDefImageCache images;

    /// <summary>Initializes a new instance of the <see cref="TableDefReader"/> class.</summary>
    /// <param name="pages">The page source.</param>
    /// <param name="format">The format profile.</param>
    /// <param name="cacheResults">Whether parsed structural images are cached.</param>
    internal TableDefReader(IPageSource pages, JetFormat format, bool cacheResults)
    {
        this.pages = pages;
        this.format = format;
        this.cacheResults = cacheResults;
        this.images = new TDefImageCache(format);
    }

    /// <summary>Gets or sets a value indicating whether newly constructed readers verify structural cache hits.</summary>
    internal static bool VerifyCacheHits { get; set; }

    /// <summary>Gets or sets a value indicating whether cache hits are checked against the source.</summary>
    internal bool VerifyOnHit { get; set; } = VerifyCacheHits;

    /// <inheritdoc/>
    public void Dispose() => this.images.OnInvalidateAll();

    /// <inheritdoc/>
    public void OnPageWritten(long pageNumber, ReadOnlySpan<byte> before, ReadOnlySpan<byte> after)
        => this.images.OnPageWritten(pageNumber, before, after);

    /// <inheritdoc/>
    public void OnInvalidateAll() => this.images.OnInvalidateAll();

    /// <summary>Reads current counters from the root page alone, never from the structural cache.</summary>
    /// <param name="tdefPage">The root page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The current counters, or null when the root is not a TDEF.</returns>
    internal async ValueTask<TableCounters?> ReadTableCountersAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        byte[] page = await this.pages.ReadPageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        try
        {
            if (page[0] != Constants.PageTypes.TableDefinition)
            {
                return null;
            }

            Pages.TDefHeaderLayout header = this.format.TDefFormat.Header;
            return new TableCounters(
                Ru32(page, header.NumRows),
                Ru32(page, header.AutoNumber),
                header.ComplexAutoNumber < 0 ? 0 : Ru32(page, header.ComplexAutoNumber));
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>Returns independent logical bytes. Writable sources always read their current chain.</summary>
    /// <param name="startPage">The root page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The logical bytes, or null.</returns>
    internal async ValueTask<byte[]?> ReadTDefBytesAsync(long startPage, CancellationToken cancellationToken = default)
    {
        if (!this.cacheResults || this.pages is Pager)
        {
            return (await this.ReadChainAsync(startPage, cancellationToken).ConfigureAwait(false))?.Bytes;
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (this.images.TryGet(startPage, out _))
        {
            return (await this.ReadImageAsync(startPage, cancellationToken).ConfigureAwait(false))?.CopyBytes();
        }

        long epoch = this.images.Epoch;
        LogicalTDefChain? chain = await this.ReadChainAsync(startPage, cancellationToken).ConfigureAwait(false);
        TDefImage? image = TDefCodec.Parse(this.format, chain?.Bytes);
        if (image is not null && chain is not null)
        {
            _ = this.images.Publish(startPage, image, chain.PageNumbers, epoch);
        }

        // Byte-level callers retain the original tolerant contract even when
        // the structural parser refuses the columns of an otherwise valid chain.
        return chain?.Bytes;
    }

    /// <summary>Reads a fresh chain with its physical page mapping for in-place writes.</summary>
    /// <param name="startPage">The root page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The chain.</returns>
    /// <exception cref="InvalidDataException">The root is not a table definition.</exception>
    internal async ValueTask<LogicalTDefChain> ReadTDefChainAsync(long startPage, CancellationToken cancellationToken = default)
        => await this.ReadChainAsync(startPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"The table definition at page {startPage} could not be read.");

    /// <summary>Reads an immutable parsed structural image, with guarded cache publication.</summary>
    /// <param name="tdefPage">The root page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The image, or null for an unreadable definition.</returns>
    /// <exception cref="InvalidDataException">Verification detects stale cached structure.</exception>
    internal async ValueTask<TDefImage?> ReadImageAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (this.cacheResults && this.images.TryGet(tdefPage, out TDefImage? cached))
        {
            if (this.VerifyOnHit)
            {
                LogicalTDefChain? current = await this.ReadChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
                if (current is null || !this.images.StructuralBytesEqual(cached.CopyBytes(), current.Bytes))
                {
                    throw new InvalidDataException($"The cached table definition at page {tdefPage} is stale.");
                }
            }

            return cached;
        }

        long epoch = this.images.Epoch;
        LogicalTDefChain? chain = await this.ReadChainAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        TDefImage? image = TDefCodec.Parse(this.format, chain?.Bytes);
        if (this.cacheResults && image is not null && chain is not null)
        {
            _ = this.images.Publish(tdefPage, image, chain.PageNumbers, epoch);
        }

        return image;
    }

    /// <summary>Returns an immutable column layout without live counter state.</summary>
    /// <param name="tdefPage">The root page.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The layout, or null.</returns>
    internal async ValueTask<TableDef?> ReadTableDefAsync(long tdefPage, CancellationToken cancellationToken = default)
    {
        TDefImage? image = await this.ReadImageAsync(tdefPage, cancellationToken).ConfigureAwait(false);
        if (image is null)
        {
            return null;
        }

        return image.Definition;
    }

    /// <summary>Reads the required owned layout.</summary>
    /// <param name="tdefPage">The root page.</param>
    /// <param name="tableName">The name for error reporting.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The layout.</returns>
    /// <exception cref="InvalidDataException">The definition cannot be read.</exception>
    internal async ValueTask<TableDef> ReadRequiredTableDefAsync(long tdefPage, string tableName, CancellationToken cancellationToken = default)
        => await this.ReadTableDefAsync(tdefPage, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidDataException($"Table definition for '{tableName}' could not be read.");

    private ValueTask<LogicalTDefChain?> ReadChainAsync(long startPage, CancellationToken cancellationToken)
        => LogicalTDefChain.ReadAsync(startPage, this.format.PageSize, this.pages.ReadPageAsync, PageBuffers.Return, retainPageNumbers: true, cancellationToken);
}
