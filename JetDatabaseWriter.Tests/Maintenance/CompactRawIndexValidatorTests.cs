namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class CompactRawIndexValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ReopenedMultilevelAllocation_ComparesEveryOpaqueKeyAndRelocatedBookmark(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        IndexEntry[] original = Entries(600, 300);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        byte[] unchanged = bytes.ToArray();
        await using var destination = new MemoryStream(bytes, writable: false);
        CompactRawIndexValidatorResult result = await ValidateAsync(format, destination, original, allocation);
        Assert.Equal(original.Length, result.EntryCount);
        Assert.Equal(allocation.PageCount, result.PageCount);
        Assert.Equal(allocation.Levels, result.Levels);
        Assert.True(result.PrivateAllocationValidated);
        Assert.False(result.DatabaseValidated);
        Assert.Equal(unchanged, bytes);
        Assert.True(destination.CanRead);
    }

    [Fact]
    public async Task EmptyInput_RequiresExactlyOneEmptyLeaf()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, []);
        await using var destination = new MemoryStream(bytes, writable: false);
        CompactRawIndexValidatorResult result = await ValidateAsync(format, destination, [], allocation);
        Assert.Equal(0, result.EntryCount);
        Assert.Equal(1, result.PageCount);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, Entries(1, 8), allocation));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChangedLeafKeyOrBookmark_RefusesEvenWithValidPageOrder(bool bookmark)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        byte[] page = Page(format, bytes, allocation.FirstPageNumber);
        List<IndexEntry> entries = IndexPageCodec.DecodeLeafEntries(format.IndexPage, page, format.PageSize);
        IndexEntry changed = entries[^1];
        if (bookmark)
        {
            changed = changed with { DataRow = 99 };
        }
        else
        {
            changed.Key[^1]++;
        }

        entries[^1] = changed;
        PutPage(format, bytes, allocation.FirstPageNumber, IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, 0, 0, 0, true));
        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, original, allocation));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ChangedIntermediateMaximum_RefusesEvenIfLeafKeysStillRoute(bool bookmark)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(60, 300);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        byte[] root = Page(format, bytes, allocation.RootPageNumber);
        List<DecodedIntermediateEntry> entries = IndexPageCodec.DecodeIntermediateEntries(format.IndexPage, root, format.PageSize);
        DecodedIntermediateEntry changed = entries[^1];
        if (bookmark)
        {
            changed = changed with { Entry = changed.Entry with { DataRow = 99 } };
        }
        else
        {
            changed.Entry.Key[^1]++;
        }

        entries[^1] = changed;
        PutPage(format, bytes, allocation.RootPageNumber, IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, entries, 0, 0, allocation.LastLeafPageNumber));
        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, original, allocation));
    }

    [Theory]
    [InlineData("owner")]
    [InlineData("root")]
    [InlineData("levels")]
    [InlineData("tail")]
    [InlineData("extra")]
    [InlineData("missing")]
    public async Task ContradictoryAllocationOrStructure_Refuses(string damage)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(60, 300);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        switch (damage)
        {
            case "owner":
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)(allocation.FirstPageNumber * format.PageSize)) + 4), 3);
                break;
            case "root":
                allocation = allocation with { RootPageNumber = allocation.FirstPageNumber };
                break;
            case "levels":
                allocation = allocation with { Levels = allocation.Levels + 1 };
                break;
            case "tail":
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(checked((int)(allocation.FirstPageNumber * format.PageSize)) + format.IndexPage.TailPageOffset), checked((uint)allocation.FirstPageNumber));
                break;
            case "extra":
                byte[] enlarged = new byte[bytes.Length + format.PageSize];
                bytes.CopyTo(enlarged, 0);
                PutPage(format, enlarged, bytes.Length / format.PageSize, Page(format, bytes, allocation.FirstPageNumber));
                bytes = enlarged;
                allocation = allocation with { PageCount = allocation.PageCount + 1 };
                break;
            case "missing":
                allocation = allocation with { PageCount = allocation.PageCount - 1 };
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, original, allocation));
    }

    [Theory]
    [InlineData("entries")]
    [InlineData("canonical")]
    [InlineData("pages")]
    [InlineData("reads")]
    [InlineData("working")]
    [InlineData("graph")]
    public async Task ExhaustedAllowance_RefusesBeforeReturningValidation(string limit)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(60, 300);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        CompactRawIndexValidatorBudget budget = limit switch
        {
            "entries" => new() { MaxEntries = 1 },
            "canonical" => new() { MaxCanonicalBytes = 1 },
            "pages" => new() { MaxTreePages = 1 },
            "reads" => new() { MaxReadBytes = 1 },
            "working" => new() { MaxWorkingBytes = 1 },
            "graph" => new() { IndexAudit = new() { MaxGraphSteps = 1 } },
            _ => throw new InvalidOperationException(),
        };
        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(format, destination, original, allocation, budget));
    }

    [Fact]
    public async Task PartialReads_ChargeEveryRequestedByteAndPreserveAllKeys()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        await using var ordinary = new MemoryStream(bytes, writable: false);
        CompactRawIndexValidatorResult baseline = await ValidateAsync(format, ordinary, original, allocation);
        await using var partial = new PartialReadStream(bytes);
        CompactRawIndexValidatorResult result = await ValidateAsync(format, partial, original, allocation);
        Assert.True(result.ReadBytes > baseline.ReadBytes);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await ValidateAsync(format, partial, original, allocation, new() { MaxReadBytes = result.ReadBytes - 1 }));
    }

    [Fact]
    public async Task CancellationIoFailureAndLengthMutation_NeverReturnValidation()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        var failure = new IOException("Injected reopened index read failure.");
        await using var fault = new ReadFailureStream(bytes, failure);
        Assert.Same(failure, await Assert.ThrowsAsync<IOException>(async () => await ValidateAsync(format, fault, original, allocation)));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await cancellation.CancelAsync();
        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ValidateWithTokenAsync(format, destination, original, allocation, null, cancellation.Token));
        await using var changing = new ChangingLengthStream(bytes);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, changing, original, allocation));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(1)]
    public async Task MissingOrExtraOriginalObservation_RefusesCompleteCoverage(int difference)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        await using var destination = new MemoryStream(bytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, destination, Entries(original.Length + difference, 8), allocation));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(600)]
    public async Task DuplicateOpaqueKeys_RetainIncreasingMappedBookmarkTies(int count)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        byte[] key = Enumerable.Repeat((byte)0x80, 300).ToArray();
        IndexEntry[] original = Entries(count, 300).Select(entry => entry with { Key = key }).ToArray();
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        await using var destination = new MemoryStream(bytes, writable: false);
        Assert.Equal(original.Length, (await ValidateAsync(format, destination, original, allocation)).EntryCount);
    }

    [Fact]
    public async Task BorrowedOriginalKeys_AreCopiedBeforeTheirEnumeratorAdvances()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = [new([1], 100, 0), new([2], 100, 1), new([3], 100, 2)];
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        await using var destination = new MemoryStream(bytes, writable: false);
        await using MemoryStream ledger = Ledger(original);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, original.LongLength, 1024, Ct);
        CompactRawIndexValidatorResult result = await CompactRawIndexValidator.ValidateAsync(format, 2, destination, ReusedKeysAsync(Ct), map, allocation, new(), Ct);
        Assert.Equal(3, result.EntryCount);
    }

    [Fact]
    public async Task CancellationDuringReadOrOriginalEnumeration_PreventsSuccess()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        using var reading = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var destination = new CancellingReadStream(bytes, reading);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ValidateWithTokenAsync(format, destination, original, allocation, null, reading.Token));
        using var enumerating = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var reopened = new MemoryStream(bytes, writable: false);
        await using MemoryStream ledger = Ledger(original);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, original.LongLength, 1024, Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactRawIndexValidator.ValidateAsync(format, 2, reopened, CancelInputAsync(original, enumerating, enumerating.Token), map, allocation, new(), enumerating.Token));
    }

    [Fact]
    public async Task IncompleteReopenedPageOrMissingRelocation_Refuses()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry[] original = Entries(4, 8);
        (byte[] bytes, CompactRawIndexBuilderResult allocation) = await BuildAsync(format, original);
        await using var incomplete = new EarlyEndStream(bytes);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await ValidateAsync(format, incomplete, original, allocation));
        await using var destination = new MemoryStream(bytes, writable: false);
        await using MemoryStream ledger = Ledger(original[..^1]);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, original.LongLength - 1, 1024, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawIndexValidator.ValidateAsync(format, 2, destination, InputAsync(original, Ct), map, allocation, new(), Ct));
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task MicrosoftAuthoredStoredKeys_CompareAgainstIndependentOriginalAudit(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        byte[] unchanged = image.ToArray();
        await using var source = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(source, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(file, 2, Ct);
        var original = new List<IndexEntry>();
        await CompactIndexAudit.ScanAsync(
            file.Pages,
            file.Format,
            2,
            roots.First(static root => root > 0),
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry")
                {
                    original.Add(new(record.CanonicalKey!, record.TargetPageNumber!.Value, record.TargetRowIndex!.Value));
                }

                return ValueTask.CompletedTask;
            },
            Ct);
        original.Sort(static (left, right) =>
        {
            int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
            if (key != 0)
            {
                return key;
            }

            int page = left.DataPage.CompareTo(right.DataPage);
            return page != 0 ? page : left.DataRow.CompareTo(right.DataRow);
        });
        Assert.NotEmpty(original);
        await using var staged = new MemoryStream();
        staged.SetLength(image.Length);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        CompactRawIndexBuilderResult allocation = await CompactRawIndexBuilder.BuildAsync(file.Format, 2, InputAsync(original, Ct), staged, first, second, new(), Ct);
        await using var ledger = new MemoryStream();
        foreach (uint bookmark in original.Select(static entry => checked((uint)((entry.DataPage << 8) | entry.DataRow))).Distinct().Order())
        {
            byte[] record = new byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(record, bookmark);
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), bookmark);
            ledger.Write(record);
        }

        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, ledger.Length / 8, 1_000_000, Ct);
        await using var reopened = new MemoryStream(staged.ToArray(), writable: false);
        CompactRawIndexValidatorResult result = await CompactRawIndexValidator.ValidateAsync(file.Format, 2, reopened, InputAsync(original, Ct), map, allocation, new(), Ct);
        Assert.Equal(original.Count, result.EntryCount);
        Assert.Equal(unchanged, image);
    }

    private static ValueTask<CompactRawIndexValidatorResult> ValidateAsync(JetFormat format, Stream destination, IndexEntry[] original, CompactRawIndexBuilderResult allocation, CompactRawIndexValidatorBudget? budget = null)
        => ValidateWithTokenAsync(format, destination, original, allocation, budget, Ct);

    private static async ValueTask<CompactRawIndexValidatorResult> ValidateWithTokenAsync(JetFormat format, Stream destination, IndexEntry[] original, CompactRawIndexBuilderResult allocation, CompactRawIndexValidatorBudget? budget, CancellationToken token)
    {
        await using MemoryStream ledger = Ledger(original);
        CompactRowRelocationMap map = await CompactRowRelocationMap.OpenAsync(ledger, original.LongLength, 1_000_000, token);
        return await CompactRawIndexValidator.ValidateAsync(format, 2, destination, InputAsync(original, token), map, allocation, budget ?? new(), token);
    }

    private static async ValueTask<(byte[] Bytes, CompactRawIndexBuilderResult Allocation)> BuildAsync(JetFormat format, IndexEntry[] original)
    {
        await using var destination = new MemoryStream();
        destination.SetLength(10L * format.PageSize);
        await using var first = new MemoryStream();
        await using var second = new MemoryStream();
        IndexEntry[] relocated = original.Select(static entry => entry with { DataPage = entry.DataPage - 95 }).ToArray();
        CompactRawIndexBuilderResult allocation = await CompactRawIndexBuilder.BuildAsync(format, 2, InputAsync(relocated, Ct), destination, first, second, new(), Ct);
        return (destination.ToArray(), allocation);
    }

    private static IndexEntry[] Entries(int count, int length)
        => Enumerable.Range(0, count).Select(i =>
        {
            byte[] key = new byte[length];
            BinaryPrimitives.WriteInt32BigEndian(key, i);
            key[^1] = 0x80;
            return new IndexEntry(key, 100 + (i / 256), checked((byte)(i % 256)));
        }).ToArray();

    private static MemoryStream Ledger(IEnumerable<IndexEntry> original)
    {
        var stream = new MemoryStream();
        foreach (IndexEntry entry in original)
        {
            byte[] record = new byte[8];
            BinaryPrimitives.WriteUInt32LittleEndian(record, checked((uint)((entry.DataPage << 8) | entry.DataRow)));
            BinaryPrimitives.WriteUInt32LittleEndian(record.AsSpan(4), checked((uint)(((entry.DataPage - 95) << 8) | entry.DataRow)));
            stream.Write(record);
        }

        return stream;
    }

    private static async IAsyncEnumerable<IndexEntry> InputAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private static async IAsyncEnumerable<IndexEntry> ReusedKeysAsync([EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        byte[] key = [0];
        for (byte index = 0; index < 3; index++)
        {
            token.ThrowIfCancellationRequested();
            key[0] = checked((byte)(index + 1));
            yield return new(key, 100, index);
        }
    }

    private static async IAsyncEnumerable<IndexEntry> CancelInputAsync(IEnumerable<IndexEntry> entries, CancellationTokenSource cancellation, [EnumeratorCancellation] CancellationToken token)
    {
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            await cancellation.CancelAsync();
            yield return entry;
        }
    }

    private static byte[] Page(JetFormat format, byte[] bytes, long page)
        => bytes.AsSpan(checked((int)(page * format.PageSize)), format.PageSize).ToArray();

    private static void PutPage(JetFormat format, byte[] bytes, long page, byte[] image)
        => image.CopyTo(bytes, checked((int)(page * format.PageSize)));

    private sealed class PartialReadStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(17, buffer.Length)], cancellationToken);
    }

    private sealed class ReadFailureStream(byte[] bytes, IOException failure) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException<int>(failure);
    }

    private sealed class ChangingLengthStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await base.ReadAsync(buffer, cancellationToken);
            this.SetLength(this.Length - 1);
            return read;
        }
    }

    private sealed class CancellingReadStream(byte[] bytes, CancellationTokenSource cancellation) : MemoryStream(bytes, writable: false)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await base.ReadAsync(buffer, cancellationToken);
            await cancellation.CancelAsync();
            return read;
        }
    }

    private sealed class EarlyEndStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(0);
    }
}
