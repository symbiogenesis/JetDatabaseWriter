namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1
using JetDatabaseWriter.Infrastructure;
#endif

/// <summary>Retains the first raw image of each overwritten page, never decoded plaintext.</summary>
internal sealed class StatementUndoLog : IAsyncDisposable
{
    private readonly Dictionary<long, Record> records = [];
    private readonly bool fileBacked;
    private readonly int memoryPageLimit;
    private FileStream? spill;

    /// <summary>Initializes a new instance of the <see cref="StatementUndoLog"/> class.</summary>
    /// <param name="originalLength">The original physical length.</param>
    /// <param name="fileBacked">Whether raw records can use a temporary file.</param>
    /// <param name="memoryPageLimit">The raw image count kept in memory before spilling.</param>
    internal StatementUndoLog(long originalLength, bool fileBacked, int memoryPageLimit)
    {
        this.OriginalLength = originalLength;
        this.fileBacked = fileBacked;
        this.memoryPageLimit = memoryPageLimit;
    }

    /// <summary>Gets a value indicating whether undo records have spilled to a temporary file.</summary>
    internal bool IsFileSpilled => this.spill is not null;

    /// <summary>Gets the original physical length.</summary>
    internal long OriginalLength { get; }

    /// <summary>Gets or sets a value indicating whether physical writes require undo.</summary>
    internal bool HasWrites { get; set; }

    /// <summary>Captures an original raw page once before any overwrite.</summary>
    /// <param name="store">The physical store.</param>
    /// <param name="offset">The physical byte offset.</param>
    /// <param name="pageSize">The page size.</param>
    /// <param name="cancellationToken">Cancellation during preparation.</param>
    /// <returns>The capture completion.</returns>
    internal async ValueTask CaptureAsync(IPageStore store, long offset, int pageSize, CancellationToken cancellationToken)
    {
        if (offset >= this.OriginalLength || this.records.ContainsKey(offset))
        {
            return;
        }

        byte[] bytes = new byte[checked((int)Math.Min(pageSize, this.OriginalLength - offset))];
        await store.ReadAsync(offset, bytes, false, cancellationToken).ConfigureAwait(false);
        if (!this.fileBacked || (this.spill is null && this.records.Count + 1 < this.memoryPageLimit))
        {
            this.records.Add(offset, new Record(0, bytes.Length, bytes));
            return;
        }

        if (this.spill is null)
        {
            this.spill = new FileStream(Path.Combine(Path.GetTempPath(), "jdw-undo-" + Guid.NewGuid().ToString("N") + ".tmp"), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.Asynchronous | FileOptions.DeleteOnClose);
            foreach (long key in new List<long>(this.records.Keys))
            {
                Record record = this.records[key];
                long recordPosition = this.spill.Position;
                await this.spill.WriteAsync(record.Bytes.AsMemory(), cancellationToken).ConfigureAwait(false);
                this.records[key] = new Record(recordPosition, record.Length, null);
            }
        }

        long position = this.spill.Length;
        this.spill.Position = position;
        await this.spill.WriteAsync(bytes.AsMemory(), cancellationToken).ConfigureAwait(false);
        this.records.Add(offset, new Record(position, bytes.Length, null));
    }

    /// <summary>Restores raw pages and the original physical length.</summary>
    /// <param name="store">The physical store.</param>
    /// <param name="durable">Whether restoration requires a device flush.</param>
    /// <returns>The restoration completion.</returns>
    internal async ValueTask RestoreAsync(IPageStore store, bool durable)
    {
        foreach (KeyValuePair<long, Record> entry in this.records)
        {
            byte[] bytes = await this.ReadRawImageAsync(entry.Key).ConfigureAwait(false);
            await store.WriteAsync(entry.Key, bytes, CancellationToken.None).ConfigureAwait(false);
        }

        await store.SetLengthAsync(this.OriginalLength, CancellationToken.None).ConfigureAwait(false);
        await store.FlushAsync(durable, CancellationToken.None).ConfigureAwait(false);
        this.HasWrites = false;
    }

    /// <summary>Reads a captured raw image without applying a page codec.</summary>
    /// <param name="offset">The original page byte offset.</param>
    /// <returns>The original raw bytes.</returns>
    internal async ValueTask<byte[]> ReadRawImageAsync(long offset)
    {
        Record record = this.records[offset];
        if (record.Bytes is { } bytes)
        {
            return bytes;
        }

        byte[] raw = new byte[record.Length];
        this.spill!.Position = record.Position;
        await this.spill.ReadExactlyAsync(raw.AsMemory(), CancellationToken.None).ConfigureAwait(false);
        return raw;
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        try
        {
            if (this.spill is not null)
            {
                await this.spill.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (IOException)
        {
            // Temporary-log cleanup must not change a successful commit into a rollback.
        }
        catch (UnauthorizedAccessException)
        {
            // The committed database no longer depends on this temporary log.
        }
        finally
        {
            this.spill = null;
            this.records.Clear();
        }
    }

    private sealed record Record(long Position, int Length, byte[]? Bytes);
}
