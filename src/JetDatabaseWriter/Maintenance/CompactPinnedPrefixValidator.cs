namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;

/// <summary>Independently compares every retained original byte with reopened private append-only staging.</summary>
/// <remarks>
/// The caller keeps the decrypted original source, verified original images, replacement inputs and independently
/// reopened decrypted destination immutable on distinct backing storage, including through wrappers. Only complete
/// known TDEF reference replacements are reconstructed; caller-supplied patched bytes and arbitrary exception ranges
/// cannot authorize changes. Unknown application, security, replication and other storage remains byte-identical.
/// Global-map mutation requires a concrete independent comparison of every changed map byte and unchanged surround.
/// Appended contents outside that map, catalog completeness, reference ownership, native
/// acceptance, reclamation and publication remain separate requirements. This component reads and never writes.
/// </remarks>
internal static class CompactPinnedPrefixValidator
{
    /// <summary>Streams the entire pinned prefix against source bytes with independently reconstructed defined pointer changes.</summary>
    /// <param name="source">The immutable original decrypted page source, including its physical header page.</param>
    /// <param name="format">The matching verified original native format.</param>
    /// <param name="definitions">Complete original images and expected reference inputs for every changed pinned definition.</param>
    /// <param name="reopenedDestination">The independently reopened readable seekable decrypted private destination.</param>
    /// <param name="budget">Aggregate physical, repeated-read and conservative additional storage admission.</param>
    /// <param name="cancellationToken">Cancels every traversal, page read and page comparison.</param>
    /// <param name="globalMap">Optional independent pre-map EOF and limits for validating the replacement native global map.</param>
    /// <returns>Exact original-prefix preservation evidence, without whole-database or reclaimability acceptance.</returns>
    /// <exception cref="JetCorruptDataException">Geometry, source evidence, chain identities or expected private bytes disagree.</exception>
    internal static async ValueTask<CompactPinnedPrefixValidatorResult> ValidateAsync(
        IPageSource source,
        JetFormat format,
        IReadOnlyList<CompactPinnedTDefReferences> definitions,
        Stream reopenedDestination,
        CompactPinnedPrefixValidatorBudget budget,
        CancellationToken cancellationToken,
        CompactPinnedGlobalMapValidation? globalMap = null)
    {
        var audit = new Audit(source, format, definitions, reopenedDestination, budget, cancellationToken);
        long position = reopenedDestination.Position;
        try
        {
            if (globalMap is not null)
            {
                Guard.NotNull(globalMap.Budget, nameof(globalMap));
                CompactGlobalMapBudget limits = globalMap.Budget;
                CompactGlobalMapResult compared = await CompactGlobalMapValidator.ValidateAsync(
                    source,
                    format,
                    globalMap.AppendPointPages,
                    reopenedDestination,
                    new()
                    {
                        MaxPages = limits.MaxPages,
                        MaxBitmapPages = limits.MaxBitmapPages,
                        MaxReadBytes = Math.Min(limits.MaxReadBytes, budget.MaxReadBytes),
                        MaxWriteBytes = limits.MaxWriteBytes,
                        MaxWorkingBytes = Math.Min(limits.MaxWorkingBytes, budget.MaxWorkingBytes),
                    },
                    cancellationToken).ConfigureAwait(false);
                audit.AcceptGlobalMapComparison(compared.ReadBytes);
            }

            await audit.ReconstructAsync(definitions, cancellationToken).ConfigureAwait(false);
            await audit.CompareAsync(cancellationToken).ConfigureAwait(false);
            return new(audit.PinnedPages, definitions.Count, audit.DefinitionPages, audit.PatchedBytes, audit.ReadBytes);
        }
        finally
        {
            reopenedDestination.Position = position;
        }
    }

    private static JetCorruptDataException Corrupt(string reason)
        => new(JetErrorCode.CorruptPage, reason, new JetErrorInfo { Reason = reason });

    private static JetLimitationException Limit(string reason)
        => new(JetErrorCode.ValueTooLarge, reason, new JetErrorInfo { Reason = reason });

    private sealed class Audit
    {
        private readonly IPageSource source;
        private readonly JetFormat format;
        private readonly Stream destination;
        private readonly CompactPinnedPrefixValidatorBudget budget;
        private readonly long destinationLength;
        private readonly List<BytePatch> patches = [];
        private bool globalMapValidated;

        internal Audit(IPageSource source, JetFormat format, IReadOnlyList<CompactPinnedTDefReferences> definitions, Stream destination, CompactPinnedPrefixValidatorBudget budget, CancellationToken token)
        {
            Guard.NotNull(source, nameof(source));
            Guard.NotNull(format, nameof(format));
            Guard.NotNull(definitions, nameof(definitions));
            Guard.NotNull(destination, nameof(destination));
            Guard.NotNull(budget, nameof(budget));
            Guard.InRange(budget.MaxPages, 0L, 0x1000000L, nameof(budget));
            Guard.InRange(budget.MaxDefinitions, 0, int.MaxValue, nameof(budget));
            Guard.InRange(budget.MaxDefinitionPages, 0L, 0x1000000L, nameof(budget));
            Guard.InRange(budget.MaxReadBytes, 0L, long.MaxValue, nameof(budget));
            Guard.InRange(budget.MaxWorkingBytes, 1L, long.MaxValue, nameof(budget));
            source.ThrowIfDisposedOrCancelled(token);
            Guard.RequireReadableSeekableStream(destination, nameof(destination));
            this.source = source;
            this.format = format;
            this.destination = destination;
            this.budget = budget;
            this.PinnedPages = source.PageCount;
            this.destinationLength = destination.Length;
            if (source.PageSize != format.PageSize || this.PinnedPages is < 3 or > 0x1000000
                || this.destinationLength % format.PageSize != 0
                || this.destinationLength / format.PageSize < this.PinnedPages
                || this.destinationLength / format.PageSize > 0x1000000)
            {
                throw Corrupt("The source or reopened private destination has invalid pinned-prefix geometry.");
            }

            if (this.PinnedPages > budget.MaxPages || definitions.Count > budget.MaxDefinitions)
            {
                throw Limit("Pinned-prefix comparison exceeds MaxPages or MaxDefinitions.");
            }

            long maxImage = 0;
            long maxParser = 0;
            long pointerBytes = 0;
            foreach (CompactPinnedTDefReferences definition in definitions)
            {
                token.ThrowIfCancellationRequested();
                Guard.NotNull(definition, nameof(definitions));
                Guard.NotNull(definition.Original, nameof(definitions));
                Guard.NotNull(definition.Indexes, nameof(definitions));
                Guard.NotNull(definition.LongValues, nameof(definitions));
                TDefImage image = definition.Original;
                int length = image.LogicalByteLength;
                long end = (long)image.Header.LogicalLength + 8;
                long expectedLength = end <= format.PageSize
                    ? format.PageSize
                    : format.PageSize + ((((end - format.PageSize - 1) / (format.PageSize - 8)) + 1) * (format.PageSize - 8));
                if (definition.RootPage is <= 0 or > 0xFFFFFF || definition.RootPage >= this.PinnedPages
                    || (image.TDefPageNumber.HasValue && image.TDefPageNumber != definition.RootPage)
                    || image.Issues != TDefParseIssues.None || length < format.PageSize
                    || (length - format.PageSize) % (format.PageSize - 8) != 0 || length != expectedLength
                    || end < format.TDef.BlockEnd || end > length || image.Bytes[0] != Constants.PageTypes.TableDefinition)
                {
                    throw Corrupt("An original definition lacks a complete verified pinned identity and logical image.");
                }

                long pages = 1 + ((length - format.PageSize) / (format.PageSize - 8));
                if (pages > budget.MaxDefinitionPages - this.DefinitionPages)
                {
                    throw Limit("Pinned-prefix comparison exceeds MaxDefinitionPages.");
                }

                this.DefinitionPages += pages;
                if (definition.Indexes.Count > Constants.TableDefinition.MaxIndexes || definition.LongValues.Count > Constants.TableDefinition.MaxColumns)
                {
                    throw new ArgumentException("A complete known reference list exceeds native descriptor capacity.", nameof(definitions));
                }

                pointerBytes += 8L + (8L * definition.Indexes.Count) + (8L * definition.LongValues.Count);
                maxImage = Math.Max(maxImage, length);
                maxParser = Math.Max(maxParser, 65_536L + (256L * Math.Min(Constants.TableDefinition.MaxColumns, length / 10)));
            }

            long working = 131_072L + (3L * format.PageSize) + (128L * this.DefinitionPages) + (64L * pointerBytes) + maxImage + maxParser;
            if (working > budget.MaxWorkingBytes || pointerBytes > int.MaxValue)
            {
                throw Limit("Pinned-prefix comparison exceeds MaxWorkingBytes or representable patch storage.");
            }
        }

        internal long PinnedPages { get; }

        internal long DefinitionPages { get; }

        internal long ReadBytes { get; private set; }

        internal long PatchedBytes => this.patches.Count;

        internal void AcceptGlobalMapComparison(long readBytes)
        {
            this.AdmitRead(readBytes);
            this.globalMapValidated = true;
        }

        internal async ValueTask ReconstructAsync(IReadOnlyList<CompactPinnedTDefReferences> definitions, CancellationToken token)
        {
            var visited = new HashSet<long>();
            foreach (CompactPinnedTDefReferences definition in definitions)
            {
                this.Check(token);
                byte[] expected = CompactTDefReferencePatcher.Patch(this.format, definition.Original, definition.OwnedPages, definition.FreePages, definition.Indexes, definition.LongValues, this.budget.MaxWorkingBytes, token);
                long number = definition.RootPage;
                int logical = 0;
                while (logical < expected.Length)
                {
                    this.Check(token);
                    if (number <= 0 || number >= this.PinnedPages || !visited.Add(number))
                    {
                        throw Corrupt("Original pinned definition chains overlap, cycle or leave original storage.");
                    }

                    int physical = logical == 0 ? 0 : 8;
                    int length = this.format.PageSize - physical;
                    byte[] original = await this.ReadSourceAsync(number, token).ConfigureAwait(false);
                    try
                    {
                        if (original[0] != Constants.PageTypes.TableDefinition
                            || !original.AsSpan(physical, length).SequenceEqual(definition.Original.Bytes.Slice(logical, length)))
                        {
                            throw Corrupt("An original physical chain disagrees with its complete verified logical image.");
                        }

                        long next = BinaryPrimitives.ReadUInt32LittleEndian(original.AsSpan(4));
                        if (next == 0 != (logical + length == expected.Length))
                        {
                            throw Corrupt("An original pinned chain ends outside its complete logical image.");
                        }

                        for (int index = 0; index < length; index++)
                        {
                            byte value = expected[logical + index];
                            if (value != original[physical + index])
                            {
                                this.patches.Add(new((number * this.format.PageSize) + physical + index, value));
                            }
                        }

                        logical += length;
                        number = next;
                    }
                    finally
                    {
                        PageBuffers.Return(original);
                    }
                }
            }

            this.Check(token);
            this.patches.Sort(static (left, right) => left.Offset.CompareTo(right.Offset));
        }

        internal async ValueTask CompareAsync(CancellationToken token)
        {
            byte[] actual = new byte[this.format.PageSize];
            int patchIndex = 0;
            for (long number = 0; number < this.PinnedPages; number++)
            {
                if (number == 1 && this.globalMapValidated)
                {
                    // The concrete map comparison checked this entire page, including every byte outside row zero.
                    this.Check(token);
                    continue;
                }

                byte[] expected = await this.ReadSourceAsync(number, token).ConfigureAwait(false);
                try
                {
                    long start = number * this.format.PageSize;
                    long end = start + this.format.PageSize;
                    while (patchIndex < this.patches.Count && this.patches[patchIndex].Offset < end)
                    {
                        BytePatch patch = this.patches[patchIndex++];
                        expected[checked((int)(patch.Offset - start))] = patch.Value;
                    }

                    await this.ReadPrivateAsync(start, actual, token).ConfigureAwait(false);
                    this.Check(token);
                    if (!expected.AsSpan(0, this.format.PageSize).SequenceEqual(actual))
                    {
                        throw Corrupt("A retained original byte differs from its independently reconstructed expected pinned value.");
                    }
                }
                finally
                {
                    PageBuffers.Return(expected);
                }
            }

            this.Check(token);
            if (patchIndex != this.patches.Count)
            {
                throw Corrupt("Expected definition patches were not completely covered by the pinned-prefix comparison.");
            }
        }

        private void Check(CancellationToken token)
        {
            this.source.ThrowIfDisposedOrCancelled(token);
            if (this.source.PageCount != this.PinnedPages || this.source.PageSize != this.format.PageSize || this.destination.Length != this.destinationLength)
            {
                throw Corrupt("Original or private destination geometry changed during pinned-prefix comparison.");
            }
        }

        private async ValueTask<byte[]> ReadSourceAsync(long number, CancellationToken token)
        {
            this.Check(token);
            this.AdmitRead(this.format.PageSize);
            byte[] page = await this.source.ReadPageAsync(number, token).ConfigureAwait(false);
            try
            {
                this.Check(token);
                if (page.Length < this.format.PageSize)
                {
                    throw Corrupt("A complete original physical page could not be read.");
                }

                return page;
            }
            catch
            {
                PageBuffers.Return(page);
                throw;
            }
        }

        private async ValueTask ReadPrivateAsync(long start, byte[] buffer, CancellationToken token)
        {
            this.destination.Position = start;
            int offset = 0;
            while (offset < buffer.Length)
            {
                this.Check(token);
                int requested = buffer.Length - offset;
                this.AdmitRead(requested);
                int read = await this.destination.ReadAsync(buffer.AsMemory(offset), token).ConfigureAwait(false);
                if (read <= 0 || read > requested)
                {
                    throw Corrupt("A complete reopened pinned private page could not be read.");
                }

                offset = StreamRead.AdvanceRead(buffer.Length, offset, read);
            }
        }

        private void AdmitRead(long bytes)
        {
            if (bytes > this.budget.MaxReadBytes - this.ReadBytes)
            {
                throw Limit("Pinned-prefix comparison exceeds cumulative MaxReadBytes.");
            }

            this.ReadBytes += bytes;
        }

        private readonly record struct BytePatch(long Offset, byte Value);
    }
}
