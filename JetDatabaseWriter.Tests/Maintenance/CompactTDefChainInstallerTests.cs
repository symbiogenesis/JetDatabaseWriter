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
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Pinned definition installation preserves native chain identities and refuses before private writes.</summary>
public sealed class CompactTDefChainInstallerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Install_PreservesEveryPhysicalHeaderAndReopenedPatchedByte(DatabaseFormat kind, bool wide)
    {
        Fixture fixture = await CreateAsync(kind, wide);
        byte[] original = (byte[])fixture.Source.Bytes.Clone();
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        long position = destination.Position;

        CompactTDefChainResult installed = await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), Ct);

        Assert.Equal(position, destination.Position);
        Assert.Equal(fixture.Pages.Count, installed.Pages);
        Assert.False(installed.DatabaseValidated);
        byte[] actual = destination.ToArray();
        foreach (long page in fixture.Pages)
        {
            int offset = checked((int)(page * fixture.Format.PageSize));
            Assert.Equal(original.AsSpan(offset, 8).ToArray(), actual.AsSpan(offset, 8).ToArray());
        }

        var reopenedSource = new Source(actual, fixture.Format.PageSize);
        LogicalTDefChain independentlyRead = await LogicalTDefChain.ReadRequiredAsync(fixture.Installation.RootPage, fixture.Format.PageSize, reopenedSource.ReadPageAsync, PageBuffers.Return, true, Ct);
        Assert.Equal(fixture.Installation.PatchedBytes.ToArray(), independentlyRead.Bytes);

        await using var reopened = new MemoryStream(actual, writable: false);
        CompactTDefChainResult validated = await CompactTDefChainValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Installation], reopened, new(), Ct);
        Assert.Equal(installed.Pages, validated.Pages);
        Assert.False(validated.DatabaseValidated);
        Assert.Equal(original, fixture.Source.Bytes);
        Assert.Equal(wide, fixture.Pages.Count > 1);
    }

    [Theory]
    [InlineData("root-header")]
    [InlineData("continuation-header")]
    [InlineData("continuation-body")]
    [InlineData("continuation-link")]
    [InlineData("source-image")]
    [InlineData("cycle")]
    [InlineData("out-of-range")]
    [InlineData("declared-length")]
    [InlineData("patched-header")]
    [InlineData("patched-length")]
    [InlineData("patched-declaration")]
    public async Task Install_MismatchedOrDamagedCompleteChainRefusesBeforeFirstWrite(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        byte[] destinationBytes = (byte[])fixture.Source.Bytes.Clone();
        int root = checked((int)(fixture.Installation.RootPage * fixture.Format.PageSize));
        int continuation = checked((int)(fixture.Pages[1] * fixture.Format.PageSize));
        CompactTDefChainInstallation installation = fixture.Installation;
        switch (fault)
        {
            case "root-header":
                destinationBytes[root + 1] ^= 1;
                break;
            case "continuation-header":
                destinationBytes[continuation + 2] ^= 1;
                break;
            case "continuation-body":
                destinationBytes[continuation + 12] ^= 1;
                break;
            case "continuation-link":
                BinaryPrimitives.WriteUInt32LittleEndian(destinationBytes.AsSpan(continuation + 4), checked((uint)installation.RootPage));
                break;
            case "source-image":
                fixture.Source.Bytes[continuation + 12] ^= 1;
                break;
            case "cycle":
                BinaryPrimitives.WriteUInt32LittleEndian(fixture.Source.Bytes.AsSpan(continuation + 4), checked((uint)installation.RootPage));
                break;
            case "out-of-range":
                BinaryPrimitives.WriteUInt32LittleEndian(fixture.Source.Bytes.AsSpan(root + 4), checked((uint)fixture.Source.PageCount));
                break;
            case "declared-length":
                BinaryPrimitives.WriteInt32LittleEndian(fixture.Source.Bytes.AsSpan(root + 8), 0);
                break;
            case "patched-header":
                installation = ChangePatch(installation, 1);
                break;
            case "patched-length":
                installation = installation with { PatchedBytes = installation.PatchedBytes[..^1] };
                break;
            case "patched-declaration":
                installation = ChangePatch(installation, 8);
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var destination = new MemoryStream();
        await destination.WriteAsync(destinationBytes, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [installation], destination, new(), Ct));
        Assert.Equal(destinationBytes, destination.ToArray());
    }

    [Fact]
    public async Task Install_OverlappingRequestedChainsRefuseBeforeFirstWrite()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation, fixture.Installation], destination, new(), Ct));
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("reads")]
    [InlineData("writes")]
    [InlineData("working")]
    public async Task Install_BudgetRefusesBeforeFirstWrite(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        var budget = new CompactTDefChainBudget
        {
            MaxPages = fault == "pages" ? fixture.Pages.Count - 1 : 1024,
            MaxReadBytes = fault == "reads" ? 1 : 16 * 1024 * 1024,
            MaxWriteBytes = fault == "writes" ? 1 : 16 * 1024 * 1024,
            MaxWorkingBytes = fault == "working" ? 1 : 16 * 1024 * 1024,
        };
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, budget, Ct));
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
    }

    [Fact]
    public async Task Install_CancellationAndRealReadFailurePreservePrivateBytes()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await cancelled.CancelAsync();
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), cancelled.Token));
        await using var failing = new FailingStream(fixture.Source.Bytes);
        await Assert.ThrowsAsync<IOException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], failing, new(), Ct));
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
        Assert.Equal(fixture.Source.Bytes, failing.ToArray());
        fixture.Source.FailReads = true;
        await Assert.ThrowsAsync<IOException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), Ct));
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
    }

    [Theory]
    [InlineData("header")]
    [InlineData("payload")]
    [InlineData("padding")]
    [InlineData("link")]
    public async Task Validate_ReopenedPhysicalTamperingRefuses(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.AceAccdb, true);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), Ct);
        byte[] changed = destination.ToArray();
        int page = checked((int)(fixture.Pages[^1] * fixture.Format.PageSize));
        switch (fault)
        {
            case "header":
                changed[page + 2] ^= 1;
                break;
            case "payload":
                changed[page + 8] ^= 1;
                break;
            case "padding":
                changed[page + fixture.Format.PageSize - 1] ^= 1;
                break;
            case "link":
                BinaryPrimitives.WriteUInt32LittleEndian(changed.AsSpan(page + 4), checked((uint)fixture.Installation.RootPage));
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(changed, writable: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactTDefChainValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Installation], reopened, new(), Ct));
    }

    [Theory]
    [InlineData("cancel-read")]
    [InlineData("write")]
    [InlineData("flush")]
    public async Task Install_InterruptedPrivateIoReturnsNoEvidenceAndLeavesSourceUnchanged(string fault)
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet3Mdb, true);
        byte[] original = (byte[])fixture.Source.Bytes.Clone();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        await using var destination = new FaultStream((byte[])original.Clone(), fault, cancellation);
        if (fault == "cancel-read")
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), cancellation.Token));
            Assert.Equal(original, destination.ToArray());
        }
        else
        {
            await Assert.ThrowsAsync<IOException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), Ct));
        }

        Assert.Equal(original, fixture.Source.Bytes);
    }

    [Fact]
    public async Task Install_PartialReadRetriesShareCumulativeAdmissionBeforeWrites()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        long normalReadBytes = ((2L * fixture.Pages.Count) + 1) * fixture.Format.PageSize;
        await using var destination = new PartialReadStream((byte[])fixture.Source.Bytes.Clone());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new() { MaxReadBytes = normalReadBytes }, Ct));
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
    }

    [Fact]
    public async Task Install_UnchangedDefinitionRequiresNoWriteAllowance()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        CompactTDefChainInstallation unchanged = fixture.Installation with { PatchedBytes = fixture.Installation.Original.CopyBytes() };
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        CompactTDefChainResult result = await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [unchanged], destination, new() { MaxWriteBytes = 0 }, Ct);
        Assert.Equal(0, result.WrittenBytes);
        Assert.Equal(fixture.Source.Bytes, destination.ToArray());
    }

    [Fact]
    public async Task Validate_RefusesExhaustedReadAdmissionAndSourceIoFailure()
    {
        Fixture fixture = await CreateAsync(DatabaseFormat.AceAccdb, true);
        await using var destination = new MemoryStream();
        await destination.WriteAsync(fixture.Source.Bytes, Ct);
        await CompactTDefChainInstaller.InstallAsync(fixture.Source, fixture.Format, [fixture.Installation], destination, new(), Ct);
        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactTDefChainValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Installation], reopened, new() { MaxReadBytes = 1 }, Ct));
        fixture.Source.FailReads = true;
        await Assert.ThrowsAsync<IOException>(async () => await CompactTDefChainValidator.ValidateAsync(fixture.Source, fixture.Format, [fixture.Installation], reopened, new(), Ct));
    }

    private static CompactTDefChainInstallation ChangePatch(CompactTDefChainInstallation installation, int offset)
    {
        byte[] bytes = installation.PatchedBytes.ToArray();
        bytes[offset] ^= 1;
        return installation with { PatchedBytes = bytes };
    }

    private static async Task<Fixture> CreateAsync(DatabaseFormat kind, bool wide)
    {
        await using var stream = new MemoryStream();
        long owner;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            int count = wide ? 160 : 1;
            await writer.CreateTableAsync("Pinned", Enumerable.Range(0, count).Select(static index => new ColumnDefinition("ContinuationColumn" + index, typeof(int))).ToArray(), [new IndexDefinition("IX_Pinned", "ContinuationColumn0")], Ct);
            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Pinned", Ct));
            owner = entry.TDefPage;
        }

        var format = JetFormat.ForNewDatabase(kind);
        var source = new Source(stream.ToArray(), format.PageSize);
        LogicalTDefChain chain = await LogicalTDefChain.ReadRequiredAsync(owner, format.PageSize, source.ReadPageAsync, PageBuffers.Return, true, Ct);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(format, chain.Bytes));
        byte[] patched = CompactTDefReferencePatcher.Patch(format, image, new(0, 20), new(1, 20), [new(21, new(2, 20))], [], 1_000_000, Ct);
        return new(source, format, new(owner, image, patched), chain.PageNumbers);
    }

    private sealed record Fixture(Source Source, JetFormat Format, CompactTDefChainInstallation Installation, IReadOnlyList<long> Pages);

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
                throw new IOException("Injected source I/O failure.");
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

    private sealed class PartialReadStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(7, buffer.Length)], cancellationToken);
    }

    private sealed class FaultStream(byte[] bytes, string fault, CancellationTokenSource cancellation) : MemoryStream(bytes)
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int read = await base.ReadAsync(buffer, cancellationToken);
            if (fault == "cancel-read")
            {
                await cancellation.CancelAsync();
            }

            return read;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => fault == "write" ? ValueTask.FromException(new IOException("Injected private write failure.")) : base.WriteAsync(buffer, cancellationToken);

        public override Task FlushAsync(CancellationToken cancellationToken)
            => fault == "flush" ? Task.FromException(new IOException("Injected private flush failure.")) : base.FlushAsync(cancellationToken);
    }

    private sealed class FailingStream(byte[] bytes) : MemoryStream(bytes)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromException<int>(new IOException("Injected private I/O failure."));
    }
}
