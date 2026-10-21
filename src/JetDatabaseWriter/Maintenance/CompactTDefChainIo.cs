namespace JetDatabaseWriter.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;

/// <summary>Shared geometry and low-level I/O admission; chain traversal and byte comparison remain independent.</summary>
internal sealed class CompactTDefChainIo
{
    private readonly IPageSource source;
    private readonly Stream destination;
    private readonly CompactTDefChainBudget budget;
    private readonly int pageSize;
    private readonly long sourcePages;
    private readonly long destinationLength;

    internal CompactTDefChainIo(IPageSource source, JetFormat format, IReadOnlyList<CompactTDefChainInstallation> installations, Stream destination, CompactTDefChainBudget budget, bool writable, CancellationToken token)
    {
        Guard.NotNull(source, nameof(source));
        Guard.NotNull(format, nameof(format));
        Guard.NotNull(installations, nameof(installations));
        Guard.NotNull(destination, nameof(destination));
        Guard.NotNull(budget, nameof(budget));
        Guard.InRange(budget.MaxPages, 0L, int.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWriteBytes, 0L, long.MaxValue, nameof(budget));
        Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
        source.ThrowIfDisposedOrCancelled(token);
        Guard.RequireReadableSeekableStream(destination, nameof(destination));
        if (writable && !destination.CanWrite)
        {
            throw new ArgumentException("The exclusive private destination must be writable.", nameof(destination));
        }

        this.source = source;
        this.destination = destination;
        this.budget = budget;
        this.pageSize = format.PageSize;
        this.sourcePages = source.PageCount;
        this.destinationLength = destination.Length;
        if (source.PageSize != this.pageSize || this.sourcePages is < 3 or > 0x1000000
            || this.destinationLength % this.pageSize != 0)
        {
            throw Corrupt("The immutable source or private destination has invalid native page geometry.");
        }

        long pages = 0;
        foreach (CompactTDefChainInstallation installation in installations)
        {
            token.ThrowIfCancellationRequested();
            Guard.NotNull(installation, nameof(installations));
            Guard.NotNull(installation.Original, nameof(installations));
            TDefImage image = installation.Original;
            int length = image.LogicalByteLength;
            long end = (long)image.Header.LogicalLength + 8;
            long expectedLength = end <= this.pageSize ? this.pageSize
                : this.pageSize + ((((end - this.pageSize - 1) / (this.pageSize - 8)) + 1) * (this.pageSize - 8));
            if (installation.RootPage is <= 0 or > 0xFFFFFF || installation.RootPage >= this.sourcePages
                || installation.RootPage >= this.destinationLength / this.pageSize
                || (image.TDefPageNumber.HasValue && image.TDefPageNumber != installation.RootPage)
                || image.Issues != TDefParseIssues.None || length < this.pageSize
                || (length - this.pageSize) % (this.pageSize - 8) != 0
                || end < format.TDef.BlockEnd || end > length
                || expectedLength != length
                || installation.PatchedBytes.Length != length
                || image.Bytes[0] != Constants.PageTypes.TableDefinition
                || !image.Bytes[..12].SequenceEqual(installation.PatchedBytes.Span[..12]))
            {
                throw Corrupt("The pinned definition identity, complete chain length or patched fixed header disagrees.");
            }

            int count = 1 + ((length - this.pageSize) / (this.pageSize - 8));
            if (count > budget.MaxPages - pages)
            {
                throw Limit("Pinned definition traversal exceeds MaxPages.");
            }

            pages += count;
            if (pages >= this.sourcePages || pages >= this.destinationLength / this.pageSize)
            {
                throw Corrupt("The requested pinned chains exceed the distinct source or private physical page capacity.");
            }

            this.LogicalBytes += length;
        }

        this.Pages = checked((int)pages);
        long working = 65_536L + (2L * this.pageSize) + (128L * pages);
        if (working > budget.MaxWorkingBytes)
        {
            throw Limit("Pinned definition traversal exceeds MaxWorkingBytes.");
        }
    }

    internal int Pages { get; }

    internal long LogicalBytes { get; }

    internal long ReadBytes { get; private set; }

    internal long WrittenBytes { get; private set; }

    internal static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptTableDefinition, reason, new JetErrorInfo { Reason = reason });

    internal void Check(CancellationToken token)
    {
        this.source.ThrowIfDisposedOrCancelled(token);
        if (this.source.PageSize != this.pageSize || this.source.PageCount != this.sourcePages || this.destination.Length != this.destinationLength)
        {
            throw Corrupt("Source or private destination geometry changed during pinned definition work.");
        }
    }

    internal void CheckPage(long number)
    {
        if (number is <= 0 or > 0xFFFFFF || number >= this.sourcePages || number >= this.destinationLength / this.pageSize)
        {
            throw Corrupt("A pinned definition chain points outside source or private destination storage.");
        }
    }

    internal async ValueTask<byte[]> ReadSourceAsync(long number, CancellationToken token)
    {
        this.Check(token);
        this.CheckPage(number);
        this.AdmitRead(this.pageSize);
        return await this.source.ReadPageAsync(number, token).ConfigureAwait(false);
    }

    internal async ValueTask ReadPrivateAsync(long number, byte[] buffer, CancellationToken token)
    {
        this.CheckPage(number);
        this.destination.Position = number * this.pageSize;
        int offset = 0;
        while (offset < buffer.Length)
        {
            this.Check(token);
            int requested = buffer.Length - offset;
            this.AdmitRead(requested);
            int read = await this.destination.ReadAsync(buffer.AsMemory(offset), token).ConfigureAwait(false);
            if (read <= 0 || read > requested)
            {
                throw Corrupt("A complete pinned private definition page could not be read.");
            }

            offset += read;
        }

        this.Check(token);
    }

    internal void AdmitWrites(long bytes)
    {
        if (bytes > this.budget.MaxWriteBytes)
        {
            throw Limit("Pinned definition installation exceeds MaxWriteBytes.");
        }
    }

    internal async ValueTask WritePrivateAsync(long number, byte[] page, CancellationToken token)
    {
        this.Check(token);
        if (page.Length > this.budget.MaxWriteBytes - this.WrittenBytes)
        {
            throw Limit("Pinned definition installation exceeds MaxWriteBytes.");
        }

        this.destination.Position = number * this.pageSize;
        await this.destination.WriteAsync(page.AsMemory(), token).ConfigureAwait(false);
        this.WrittenBytes += page.Length;
        this.Check(token);
    }

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private void AdmitRead(int bytes)
    {
        if (bytes > this.budget.MaxReadBytes - this.ReadBytes)
        {
            throw Limit("Pinned definition work exceeds cumulative MaxReadBytes.");
        }

        this.ReadBytes += bytes;
    }
}
