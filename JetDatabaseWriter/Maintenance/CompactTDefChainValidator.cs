namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages.Paging;

/// <summary>Independently traverses reopened private pinned definitions against immutable source and expected patches.</summary>
/// <remarks>
/// This comparison consumes no installer plan or result. The caller keeps source, expected images and the reopened
/// readable seekable decrypted destination immutable and on distinct backing storage, including through wrappers.
/// Every physical header, chain link, original logical byte, patched logical byte and padding byte is checked.
/// Global allocation, incoming ownership, other storage, native interoperability and complete database acceptance remain separate.
/// </remarks>
internal static class CompactTDefChainValidator
{
    /// <summary>Compares complete reopened chains without allocating, reclaiming or writing any page.</summary>
    /// <param name="source">The immutable decrypted original source.</param>
    /// <param name="format">The matching verified original format.</param>
    /// <param name="installations">The immutable complete original images and expected reference patches.</param>
    /// <param name="reopenedDestination">The independently reopened readable seekable decrypted destination.</param>
    /// <param name="budget">Aggregate traversal, cumulative read and working-storage allowances.</param>
    /// <param name="cancellationToken">Cancels every traversal, read and byte comparison.</param>
    /// <returns>Complete pinned-chain comparison evidence without whole-database acceptance.</returns>
    /// <exception cref="JetCorruptDataException">A complete source/private chain, fixed header, identity or logical image disagrees.</exception>
    internal static async ValueTask<CompactTDefChainResult> ValidateAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactTDefChainInstallation> installations,
        Stream reopenedDestination,
        CompactTDefChainBudget budget,
        CancellationToken cancellationToken)
    {
        var io = new CompactTDefChainIo(source, format, installations, reopenedDestination, budget, writable: false, cancellationToken);
        long position = reopenedDestination.Position;
        try
        {
            var visited = new HashSet<long>();
            byte[] actual = new byte[format.PageSize];
            long comparedPages = 0;
            foreach (CompactTDefChainInstallation installation in installations)
            {
                long originalNumber = installation.RootPage;
                long privateNumber = installation.RootPage;
                int pageIndex = 0;
                int count = 1 + ((installation.Original.LogicalByteLength - format.PageSize) / (format.PageSize - 8));
                for (; pageIndex < count; pageIndex++)
                {
                    io.Check(cancellationToken);
                    if (originalNumber != privateNumber || !visited.Add(originalNumber))
                    {
                        throw CompactTDefChainIo.Corrupt("Reopened definition identities overlap, cycle or differ from their pinned source identities.");
                    }

                    byte[] original = await io.ReadSourceAsync(originalNumber, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        io.Check(cancellationToken);
                        int physical = pageIndex == 0 ? 0 : 8;
                        int logical = pageIndex == 0 ? 0 : format.PageSize + ((pageIndex - 1) * (format.PageSize - 8));
                        int length = format.PageSize - physical;
                        if (original.Length < format.PageSize || original[0] != Constants.PageTypes.TableDefinition
                            || !original.AsSpan(physical, length).SequenceEqual(installation.Original.Bytes.Slice(logical, length)))
                        {
                            throw CompactTDefChainIo.Corrupt("The immutable original definition chain no longer matches the expected complete source image.");
                        }

                        await io.ReadPrivateAsync(privateNumber, actual, cancellationToken).ConfigureAwait(false);
                        if (!actual.AsSpan(0, 8).SequenceEqual(original.AsSpan(0, 8))
                            || !actual.AsSpan(physical, length).SequenceEqual(installation.PatchedBytes.Span.Slice(logical, length)))
                        {
                            throw CompactTDefChainIo.Corrupt("A reopened pinned definition differs from its source physical header or complete expected patched logical bytes.");
                        }

                        originalNumber = BinaryPrimitives.ReadUInt32LittleEndian(original.AsSpan(4));
                        privateNumber = BinaryPrimitives.ReadUInt32LittleEndian(actual.AsSpan(4));
                        if (originalNumber == 0 != (pageIndex == count - 1) || privateNumber == 0 != (pageIndex == count - 1))
                        {
                            throw CompactTDefChainIo.Corrupt("A reopened or original chain terminates outside the expected final logical page.");
                        }

                        comparedPages++;
                    }
                    finally
                    {
                        PageBuffers.Return(original);
                    }
                }
            }

            io.Check(cancellationToken);
            return new(installations.Count, comparedPages, io.LogicalBytes, io.ReadBytes, 0);
        }
        finally
        {
            reopenedDestination.Position = position;
        }
    }
}
