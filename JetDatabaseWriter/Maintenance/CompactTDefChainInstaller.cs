namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Installs verified logical patches only on their original complete private pinned chains.</summary>
/// <remarks>
/// The caller owns an immutable decrypted source and an exclusive private decrypted destination with distinct backing
/// storage, including through wrappers. Images must be verified pointer-only patches and remain immutable.
/// Every requested source/private chain and cross-chain overlap is checked before the first write. All physical
/// identities, eight-byte headers, chain lengths and padding are retained. Original storage is never reclaimed.
/// An I/O failure or cancellation can leave partial private writes and returns no completion evidence.
/// This component does not prove incoming ownership, global allocation, database validity or permission to publish.
/// </remarks>
internal static class CompactTDefChainInstaller
{
    /// <summary>Preflights all chains, writes changed complete private pages and flushes without changing file length.</summary>
    /// <param name="source">The immutable decrypted original page source.</param>
    /// <param name="format">The verified original format.</param>
    /// <param name="installations">Complete original logical images and verified same-length reference patches.</param>
    /// <param name="privateDestination">The exclusive readable writable seekable private destination.</param>
    /// <param name="budget">Aggregate physical traversal, read, write and retained-storage allowances.</param>
    /// <param name="cancellationToken">Cancels every traversal, page read, write and flush.</param>
    /// <returns>Private chain installation evidence without whole-database acceptance.</returns>
    /// <exception cref="JetCorruptDataException">A complete source/private chain, fixed header, identity or logical image disagrees.</exception>
    internal static async ValueTask<CompactTDefChainResult> InstallAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactTDefChainInstallation> installations,
        Stream privateDestination,
        CompactTDefChainBudget budget,
        CancellationToken cancellationToken)
    {
        var io = new CompactTDefChainIo(source, format, installations, privateDestination, budget, writable: true, cancellationToken);
        long position = privateDestination.Position;
        try
        {
            var slots = new PageSlot[io.Pages];
            var seen = new HashSet<long>();
            byte[] page = new byte[format.PageSize];
            int slotIndex = 0;
            long writeBytes = 0;
            for (int definitionIndex = 0; definitionIndex < installations.Count; definitionIndex++)
            {
                CompactTDefChainInstallation installation = installations[definitionIndex];
                long number = installation.RootPage;
                int logical = 0;
                while (logical < installation.Original.LogicalByteLength)
                {
                    io.Check(cancellationToken);
                    io.CheckPage(number);
                    if (!seen.Add(number))
                    {
                        throw CompactTDefChainIo.Corrupt("Pinned definition chains contain a cycle, duplicate identity or shared physical page.");
                    }

                    int start = logical == 0 ? 0 : 8;
                    int length = format.PageSize - start;
                    byte[] sourcePage = await io.ReadSourceAsync(number, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        io.Check(cancellationToken);
                        if (sourcePage.Length < format.PageSize || sourcePage[0] != Constants.PageTypes.TableDefinition
                            || !sourcePage.AsSpan(start, length).SequenceEqual(installation.Original.Bytes.Slice(logical, length)))
                        {
                            throw CompactTDefChainIo.Corrupt("A complete source definition page disagrees with its verified original logical image.");
                        }

                        long next = BinaryPrimitives.ReadUInt32LittleEndian(sourcePage.AsSpan(4));
                        bool last = logical + length == installation.Original.LogicalByteLength;
                        if (next == 0 != last)
                        {
                            throw CompactTDefChainIo.Corrupt("A source definition's complete continuation chain disagrees with its declared logical length.");
                        }

                        await io.ReadPrivateAsync(number, page, cancellationToken).ConfigureAwait(false);
                        if (!sourcePage.AsSpan(0, format.PageSize).SequenceEqual(page))
                        {
                            throw CompactTDefChainIo.Corrupt("A private pinned definition page or continuation header differs from its complete original source page.");
                        }

                        bool changed = !page.AsSpan(start, length).SequenceEqual(installation.PatchedBytes.Span.Slice(logical, length));
                        slots[slotIndex++] = new(number, definitionIndex, logical, start, BinaryPrimitives.ReadUInt64LittleEndian(page), changed);
                        if (changed)
                        {
                            writeBytes += format.PageSize;
                        }

                        logical += length;
                        number = next;
                    }
                    finally
                    {
                        PageBuffers.Return(sourcePage);
                    }
                }
            }

            io.AdmitWrites(writeBytes);
            foreach (PageSlot slot in slots)
            {
                io.Check(cancellationToken);
                if (!slot.Changed)
                {
                    continue;
                }

                CompactTDefChainInstallation installation = installations[slot.DefinitionIndex];
                int length = format.PageSize - slot.PhysicalStart;
                await io.ReadPrivateAsync(slot.Number, page, cancellationToken).ConfigureAwait(false);
                if (BinaryPrimitives.ReadUInt64LittleEndian(page) != slot.Header
                    || !page.AsSpan(slot.PhysicalStart, length).SequenceEqual(installation.Original.Bytes.Slice(slot.LogicalStart, length)))
                {
                    throw CompactTDefChainIo.Corrupt("A private pinned definition page changed after complete preflight.");
                }

                installation.PatchedBytes.Span.Slice(slot.LogicalStart, length).CopyTo(page.AsSpan(slot.PhysicalStart));
                await io.WritePrivateAsync(slot.Number, page, cancellationToken).ConfigureAwait(false);
            }

            io.Check(cancellationToken);
            await privateDestination.FlushAsync(cancellationToken).ConfigureAwait(false);
            io.Check(cancellationToken);
            return new(installations.Count, io.Pages, io.LogicalBytes, io.ReadBytes, io.WrittenBytes);
        }
        finally
        {
            privateDestination.Position = position;
        }
    }

    private readonly record struct PageSlot(long Number, int DefinitionIndex, int LogicalStart, int PhysicalStart, ulong Header, bool Changed);
}
