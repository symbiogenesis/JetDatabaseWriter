namespace JetDatabaseWriter.Pages.Paging;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1
using JetDatabaseWriter.Infrastructure;
#endif

/// <summary>Owns physical rollback and its selective raw file or stream before-images.</summary>
internal sealed class PhysicalRollback : IAsyncDisposable
{
    private readonly IPageStore store;
    private readonly int pageSize;
    private readonly Dictionary<long, Record> records = [];
    private readonly int memoryPageLimit;
    private readonly bool spillAllowed;
    private readonly long originalLength;
    private PersistentRollbackJournal? recovery;
    private FileStream? spill;
    private bool hasWrites;

    /// <summary>Initializes a new instance of the <see cref="PhysicalRollback"/> class.</summary>
    /// <param name="store">The borrowed physical store.</param>
    /// <param name="originalLength">The original physical length.</param>
    /// <param name="pageSize">The raw page size.</param>
    /// <param name="memoryPageLimit">The raw image count kept in memory before spilling.</param>
    /// <param name="spill">Whether custom-stream undo may spill to a temporary file.</param>
    internal PhysicalRollback(IPageStore store, long originalLength, int pageSize, int memoryPageLimit, bool spill = false)
    {
        this.store = store;
        this.originalLength = originalLength;
        this.pageSize = pageSize;
        this.memoryPageLimit = memoryPageLimit;
        this.spillAllowed = spill || (store.Capabilities.IsFileBacked && store.Capabilities.InPlacePages);
    }

    /// <summary>Gets a value indicating whether preparation or a durable decision has an uncertain outcome.</summary>
    internal bool IsFaulted { get; private set; }

    /// <summary>Captures an original raw page once before any overwrite.</summary>
    /// <param name="offset">The physical byte offset.</param>
    /// <param name="cancellationToken">Cancellation during preparation.</param>
    /// <returns>The capture completion.</returns>
    internal async ValueTask CaptureAsync(long offset, CancellationToken cancellationToken)
    {
        if (this.store is StreamPageStore { Stream: FileStream } || offset >= this.originalLength || this.records.ContainsKey(offset))
        {
            return;
        }

        byte[] bytes = new byte[checked((int)Math.Min(this.pageSize, this.originalLength - offset))];
        await this.store.ReadAsync(offset, bytes, false, cancellationToken).ConfigureAwait(false);
        if (!this.spillAllowed || (this.spill is null && this.records.Count + 1 < this.memoryPageLimit))
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

    /// <summary>Prepares durable file recovery before permitting physical writes.</summary>
    internal void BeginWrite()
    {
        if (this.store is StreamPageStore { Stream: FileStream database } streamStore)
        {
            this.IsFaulted = true;
            this.recovery ??= PersistentRollbackJournal.Create(database, this.pageSize);
            streamStore.RollbackJournal = this.recovery;
            this.IsFaulted = false;
        }

        this.hasWrites = true;
    }

    /// <summary>Flushes writes and records the durable decision before cleanup.</summary>
    /// <param name="durable">Whether stream writes require a device flush.</param>
    /// <returns>The commit completion.</returns>
    internal async ValueTask CommitAsync(bool durable)
    {
        await this.store.FlushAsync(durable || this.recovery is not null, CancellationToken.None).ConfigureAwait(false);
        this.IsFaulted = this.recovery is not null;
        this.recovery?.Complete();
        this.IsFaulted = false;
        this.hasWrites = false;
    }

    /// <summary>Restores raw pages and the original physical length.</summary>
    /// <param name="durable">Whether restoration requires a device flush.</param>
    /// <returns>The restoration completion.</returns>
    internal async ValueTask RestoreAsync(bool durable)
    {
        this.Detach();
        if (this.IsFaulted)
        {
            return;
        }

        if (this.recovery is not null)
        {
            await this.recovery.RestoreAsync(this.store).ConfigureAwait(false);
        }
        else if (this.hasWrites)
        {
            foreach (KeyValuePair<long, Record> entry in this.records)
            {
                byte[] bytes = await this.ReadRawImageAsync(entry.Key).ConfigureAwait(false);
                await this.store.WriteAsync(entry.Key, bytes, CancellationToken.None).ConfigureAwait(false);
            }

            await this.store.SetLengthAsync(this.originalLength, CancellationToken.None).ConfigureAwait(false);
            await this.store.FlushAsync(durable, CancellationToken.None).ConfigureAwait(false);
        }

        this.hasWrites = false;
    }

    private async ValueTask<byte[]> ReadRawImageAsync(long offset)
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
        this.Detach();
        this.recovery?.Dispose();
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

    private void Detach()
    {
        if (this.store is StreamPageStore streamStore && ReferenceEquals(streamStore.RollbackJournal, this.recovery))
        {
            streamStore.RollbackJournal = null;
        }
    }

    private sealed record Record(long Position, int Length, byte[]? Bytes);
}
