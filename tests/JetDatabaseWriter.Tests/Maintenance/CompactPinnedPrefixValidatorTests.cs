namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Every original physical byte is pinned except independently reconstructed known pointer changes.</summary>
public sealed class CompactPinnedPrefixValidatorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Validate_ReconstructsCompletePinnedChainAndComparesEntireOpaquePrefix(DatabaseFormat kind)
    {
        Fixture fixture = await CreateAsync(kind);
        byte[] original = (byte[])fixture.Source.Bytes.Clone();
        await using var reopened = new MemoryStream(fixture.PrivateBytes, writable: false);
        reopened.Position = 17;

        CompactPinnedPrefixValidatorResult result = await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, new(), Ct);

        Assert.Equal(fixture.Source.PageCount, result.PinnedPages);
        Assert.Equal(1, result.Definitions);
        Assert.Equal(fixture.Pages.Count, result.DefinitionPages);
        Assert.True(result.PatchedBytes > 0);
        Assert.Equal(17, reopened.Position);
        Assert.False(result.DatabaseValidated);
        Assert.False(result.OwnershipProven);
        Assert.Equal(original, fixture.Source.Bytes);
        Assert.True(fixture.PrivateBytes.Length > original.Length);
        Assert.True(fixture.Pages.Count > 1);
        Assert.NotEmpty(fixture.Expected.LongValues);
        Assert.Contains(fixture.Pages.Skip(1), page => !original.AsSpan(checked((int)(page * fixture.Format.PageSize)), fixture.Format.PageSize).SequenceEqual(fixture.PrivateBytes.AsSpan(checked((int)(page * fixture.Format.PageSize)), fixture.Format.PageSize)));
    }

    [Theory]
    [InlineData("opaque-header")]
    [InlineData("opaque-data")]
    [InlineData("unlisted-definition")]
    [InlineData("root-header")]
    [InlineData("continuation-header")]
    [InlineData("continuation-padding")]
    [InlineData("extra-tdef-byte")]
    [InlineData("wrong-pointer")]
    public async Task Validate_UnapprovedOriginalByteChangesRefuse(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb);
        int root = checked((int)(fixture.Expected.RootPage * fixture.Format.PageSize));
        int continuation = checked((int)(fixture.Pages[^1] * fixture.Format.PageSize));
        int offset = fault switch
        {
            "opaque-header" => 17,
            "opaque-data" => checked((int)(fixture.OpaquePage * fixture.Format.PageSize)) + 19,
            "unlisted-definition" => checked((int)(fixture.OtherRoot * fixture.Format.PageSize)) + fixture.Format.TDef.UsedPages,
            "root-header" => root + 2,
            "continuation-header" => continuation + 2,
            "continuation-padding" => continuation + fixture.Format.PageSize - 1,
            "extra-tdef-byte" => root + fixture.Format.TDef.NumRows,
            "wrong-pointer" => root + fixture.Format.TDef.UsedPages,
            _ => throw new InvalidOperationException(),
        };
        fixture.PrivateBytes[offset] ^= 1;
        await using var reopened = new MemoryStream(fixture.PrivateBytes, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, new(), Ct));
    }

    [Theory]
    [InlineData("overlap")]
    [InlineData("missing-index")]
    [InlineData("missing-long-value")]
    [InlineData("missing-definition")]
    [InlineData("source-body")]
    [InlineData("source-cycle")]
    [InlineData("source-outside")]
    public async Task Validate_OriginalImagesAndCompleteReferenceListsAreIndependentEvidence(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.AceAccdb);
        CompactPinnedTDefReferences expected = fixture.Expected;
        IReadOnlyList<CompactPinnedTDefReferences> patches = [expected];
        switch (fault)
        {
            case "overlap":
                patches = [expected, expected];
                break;
            case "missing-index":
                patches = [expected with { Indexes = [] }];
                break;
            case "missing-long-value":
                patches = [expected with { LongValues = [] }];
                break;
            case "missing-definition":
                patches = [];
                break;
            case "source-body":
                fixture.Source.Bytes[checked((int)(expected.RootPage * fixture.Format.PageSize)) + 100] ^= 1;
                break;
            case "source-cycle":
                BinaryPrimitives.WriteUInt32LittleEndian(fixture.Source.Bytes.AsSpan(checked((int)(fixture.Pages[^1] * fixture.Format.PageSize)) + 4), checked((uint)expected.RootPage));
                break;
            case "source-outside":
                BinaryPrimitives.WriteUInt32LittleEndian(fixture.Source.Bytes.AsSpan(checked((int)(expected.RootPage * fixture.Format.PageSize)) + 4), checked((uint)fixture.Source.PageCount));
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(fixture.PrivateBytes, writable: false);
        if (fault is "missing-index" or "missing-long-value")
        {
            await Assert.ThrowsAsync<ArgumentException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, patches, reopened, new(), Ct));
        }
        else
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, patches, reopened, new(), Ct));
        }
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("definitions")]
    [InlineData("definition-pages")]
    [InlineData("reads")]
    [InlineData("working")]
    public async Task Validate_ExplicitAdmissionLimitsRefuse(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet3Mdb);
        var budget = new CompactPinnedPrefixValidatorBudget
        {
            MaxPages = fault == "pages" ? fixture.Source.PageCount - 1 : 100_000,
            MaxDefinitions = fault == "definitions" ? 0 : 100,
            MaxDefinitionPages = fault == "definition-pages" ? fixture.Pages.Count - 1 : 1000,
            MaxReadBytes = fault == "reads" ? 1 : 16 * 1024 * 1024,
            MaxWorkingBytes = fault == "working" ? 1 : 16 * 1024 * 1024,
        };
        await using var reopened = new MemoryStream(fixture.PrivateBytes, writable: false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, budget, Ct));
    }

    [Fact]
    public async Task Validate_EmptyPatchListRequiresExactPrefixWithoutInterpretingUnknownPages()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using var reopened = new MemoryStream((byte[])fixture.Source.Bytes.Clone(), writable: false);
        CompactPinnedPrefixValidatorResult result = await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [], reopened, new(), Ct);
        Assert.Equal(0, result.Definitions);
        Assert.Equal(0, result.PatchedBytes);
        Assert.Equal(2 * fixture.Source.Bytes.Length, result.ReadBytes);
    }

    [Fact]
    public async Task Validate_DisjointDefinitionEvidenceMayArriveOutsidePhysicalPageOrder()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.AceAccdb);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(fixture.OtherRoot, fixture.Format.PageSize, fixture.Source.ReadPageAsync, PageBuffers.Return, true, Ct);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(fixture.Format, chain.Bytes));
        var other = new CompactPinnedTDefReferences(fixture.OtherRoot, image, new(0, 22), new(1, 22), [], []);
        byte[] patched = CompactTDefReferencePatcher.Patch(fixture.Format, image, other.OwnedPages, other.FreePages, other.Indexes, other.LongValues, 1_000_000, Ct);
        await using var destination = new MemoryStream(fixture.PrivateBytes, writable: true);
        await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [new(other.RootPage, image, patched)], destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);

        CompactPinnedPrefixValidatorResult result = await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [other, fixture.Expected], reopened, new(), Ct);

        Assert.Equal(2, result.Definitions);
        Assert.Equal(fixture.Pages.Count + chain.PageNumbers.Count, result.DefinitionPages);
    }

    [Fact]
    public async Task Validate_PartialReadsShareCumulativeRequestAdmission()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb);
        await using var ordinary = new MemoryStream(fixture.PrivateBytes, writable: false);
        CompactPinnedPrefixValidatorResult complete = await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], ordinary, new(), Ct);
        await using var partial = new ReadStream(fixture.PrivateBytes, "partial", null);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], partial, new() { MaxReadBytes = complete.ReadBytes }, Ct));
        await using var permitted = new ReadStream(fixture.PrivateBytes, "partial", null);
        CompactPinnedPrefixValidatorResult result = await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], permitted, new() { MaxReadBytes = 256 * 1024 * 1024 }, Ct);
        Assert.True(result.ReadBytes > complete.ReadBytes);
    }

    [Theory]
    [InlineData("cancel")]
    [InlineData("io")]
    [InlineData("eof")]
    public async Task Validate_CancellationTruncationAndRealIoReturnNoEvidence(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var reopened = new ReadStream(fixture.PrivateBytes, fault, cancellation);
        if (fault == "cancel")
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, new(), cancellation.Token));
        }
        else if (fault == "io")
        {
            await Assert.ThrowsAsync<IOException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, new(), Ct));
            fixture.Source.FailReads = true;
            await using var ordinary = new MemoryStream(fixture.PrivateBytes, writable: false);
            await Assert.ThrowsAsync<IOException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], ordinary, new(), Ct));
        }
        else
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedPrefixValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Expected], reopened, new(), Ct));
        }
    }

    private static async Task<Fixture> CreateAsync(DatabaseFormat kind)
    {
        await using var stream = new MemoryStream();
        long root;
        long otherRoot;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Pinned", Enumerable.Range(0, 160).Select(static number => new ColumnDefinition("PreservedColumn" + number, typeof(int))).Append(new("Memo", typeof(string), 20_000)).ToArray(), [new IndexDefinition("IX_Pinned", "PreservedColumn0")], Ct);
            await writer.InsertRowsAsync("Pinned", [Enumerable.Range(0, 160).Select(static number => (object?)number).Append(new string('M', 14_000)).ToArray()], Ct);
            root = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Pinned", Ct)).TDefPage;
            await writer.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], cancellationToken: Ct);
            otherRoot = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Other", Ct)).TDefPage;
        }

        var format = JetFormat.ForNewDatabase(kind);
        long opaquePage = stream.Length / format.PageSize;
        byte[] opaque = Enumerable.Range(0, format.PageSize).Select(static index => unchecked((byte)((index * 17) + 31))).ToArray();
        stream.Position = stream.Length;
        await stream.WriteAsync(opaque, Ct);
        var source = new Source(stream.ToArray(), format.PageSize);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(root, format.PageSize, source.ReadPageAsync, PageBuffers.Return, true, Ct);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, chain.Bytes));
        Assert.True(LongValueUsageMaps.TryRead(format, image.CopyBytes(), out List<LongValueUsageMap> maps, out _));
        var expected = new CompactPinnedTDefReferences(root, image, new(0, 20), new(1, 20), [new(21, new(2, 20))], maps.Select(static map => new CompactTDefLongValueReferences(map.ColumnNumber, new(3, 20), new(4, 20))).ToArray());
        byte[] patched = CompactTDefReferencePatcher.Patch(format, image, expected.OwnedPages, expected.FreePages, expected.Indexes, expected.LongValues, 1_000_000, Ct);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(source.Bytes, Ct);
        await CompactTDefChainInstaller.InstallAsync(source, format, [new(root, image, patched)], destination, new(), Ct);
        destination.Position = destination.Length;
        await destination.WriteAsync(new byte[format.PageSize], Ct);
        return new(source, format, expected, destination.ToArray(), chain.PageNumbers, otherRoot, opaquePage);
    }

    private sealed record Fixture(Source Source, JetFormat Format, CompactPinnedTDefReferences Expected, byte[] PrivateBytes, IReadOnlyList<long> Pages, long OtherRoot, long OpaquePage);

    private sealed class Source(byte[] bytes, int pageSize) : IPageSource
    {
        internal byte[] Bytes { get; } = bytes;

        public int PageSize { get; } = pageSize;

        public long PageCount => this.Bytes.Length / this.PageSize;

        public bool IsDisposed => false;

        internal bool FailReads { get; set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (this.FailReads)
            {
                throw new IOException("Injected source failure.");
            }

            byte[] page = ArrayPool<byte>.Shared.Rent(this.PageSize);
            this.Bytes.AsSpan(checked((int)(pageNumber * this.PageSize)), this.PageSize).CopyTo(page);
            return new(page);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }

    private sealed class ReadStream(byte[] bytes, string fault, CancellationTokenSource? cancellation) : MemoryStream(bytes, writable: false)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (fault == "io")
            {
                throw new IOException("Injected destination failure.");
            }

            if (fault == "eof")
            {
                return 0;
            }

            int read = await base.ReadAsync(fault == "partial" ? buffer[..Math.Min(97, buffer.Length)] : buffer, cancellationToken);
            if (fault == "cancel")
            {
                await cancellation!.CancelAsync();
            }

            return read;
        }
    }
}
