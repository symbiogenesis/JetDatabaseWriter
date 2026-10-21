namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Known index ownership remains bounded evidence rather than a reclamation proof.</summary>
public sealed class CompactIndexOwnershipAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task SameOwnerSharedRootsAndPages_RemainObservableAndPinned()
    {
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareRootAsync(2, 3, 0, Visit, Ct);
        await audit.DeclareRootAsync(2, 3, 1, Visit, Ct);
        await audit.DeclareRootAsync(2, 4, 2, Visit, Ct);
        await audit.ObserveAsync(Page(2, 3, 3), Visit, Ct);
        await audit.ObserveAsync(Reference(2, 3, 3, 5, "child"), Visit, Ct);
        await audit.ObserveAsync(Page(2, 3, 5), Visit, Ct);
        await audit.ObserveAsync(Terminal(2, 3), Visit, Ct);
        await audit.ObserveAsync(Page(2, 4, 4), Visit, Ct);
        await audit.ObserveAsync(Reference(2, 4, 4, 5, "tail"), Visit, Ct);
        await audit.ObserveAsync(Page(2, 4, 5), Visit, Ct);
        await audit.ObserveAsync(Terminal(2, 4), Visit, Ct);
        await audit.CompleteAsync(Visit, Ct);

        CompactIndexOwnershipRecord sharedRoot = Assert.Single(records, static record => record.Kind == "shared-root");
        Assert.Equal(1, sharedRoot.DescriptorIndex);
        Assert.Equal(0, sharedRoot.RelatedDescriptorIndex);
        CompactIndexOwnershipRecord sharedPage = Assert.Single(records, static record => record.Kind == "shared-page");
        Assert.Equal(5, sharedPage.PageNumber);
        Assert.Equal(3, sharedPage.RelatedRootPageNumber);
        Assert.Equal(4, sharedPage.RootPageNumber);
        Assert.Equal(2, records.Count(static record => record.Kind == "incoming-reference"));
        CompactIndexOwnershipRecord terminal = Assert.Single(records, static record => record.Kind == "ownership-terminal");
        Assert.True(terminal.DeclaredRootsObserved);
        Assert.False(terminal.ExclusiveOwnershipProven);
        Assert.All(records, static record => Assert.True(record.IsPinned));

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData("root")]
    [InlineData("page")]
    [InlineData("reference-first")]
    [InlineData("reference-last")]
    public async Task CrossOwnerClaims_EmitContradictionBeforeRefusing(string order)
    {
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareRootAsync(2, 3, 0, Visit, Ct);
        if (order != "root")
        {
            await audit.DeclareRootAsync(7, 4, 0, Visit, Ct);
        }

        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            switch (order)
            {
                case "root":
                    await audit.DeclareRootAsync(7, 3, 0, Visit, Ct);
                    break;
                case "page":
                    await audit.ObserveAsync(Page(2, 3, 5), Visit, Ct);
                    await audit.ObserveAsync(Page(7, 4, 5), Visit, Ct);
                    break;
                case "reference-first":
                    await audit.ObserveAsync(Reference(2, 3, 3, 5, "next"), Visit, Ct);
                    await audit.ObserveAsync(Page(7, 4, 5), Visit, Ct);
                    break;
                default:
                    await audit.ObserveAsync(Page(7, 4, 5), Visit, Ct);
                    await audit.ObserveAsync(Reference(2, 3, 3, 5, "child"), Visit, Ct);
                    break;
            }
        });

        Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
        CompactIndexOwnershipRecord contradiction = Assert.Single(records, static record => record.Kind == "ownership-contradiction");
        Assert.NotEqual(contradiction.OwnerTDefPage, contradiction.RelatedOwnerTDefPage);
        Assert.DoesNotContain(records, static record => record.Kind == "ownership-terminal");
        JetOperationException failure = await Assert.ThrowsAsync<JetOperationException>(async () => await audit.CompleteAsync(Visit, Ct));
        Assert.Equal(JetErrorCode.InvalidOperationState, failure.ErrorCode);

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task UnknownIncomingTarget_RemainsExplicitlyUnverifiedAfterCompletion()
    {
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareRootAsync(2, 3, 0, Visit, Ct);
        await audit.ObserveAsync(Page(2, 3, 3), Visit, Ct);
        await audit.ObserveAsync(Reference(2, 3, 3, 5, "next"), Visit, Ct);
        await audit.ObserveAsync(Terminal(2, 3), Visit, Ct);
        await audit.CompleteAsync(Visit, Ct);

        CompactIndexOwnershipRecord unresolved = Assert.Single(records, static record => record.Kind == "unobserved-reference-target");
        Assert.Equal(5, unresolved.PageNumber);
        Assert.Equal("next", unresolved.Role);
        CompactIndexOwnershipRecord terminal = Assert.Single(records, static record => record.Kind == "ownership-terminal");
        Assert.False(terminal.ExclusiveOwnershipProven);
        Assert.NotNull(terminal.Coverage);

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return ValueTask.CompletedTask;
        }
    }

    [Theory]
    [InlineData("pages")]
    [InlineData("roots")]
    [InlineData("references")]
    [InlineData("records")]
    [InlineData("steps")]
    public async Task ExhaustedAllowance_RefusesWithoutTerminal(string limit)
    {
        CompactIndexOwnershipBudget budget = limit switch
        {
            "pages" => new() { MaxTrackedPages = 1 },
            "roots" => new() { MaxRootDescriptors = 1 },
            "references" => new() { MaxReferences = 1 },
            "records" => new() { MaxRecords = 1 },
            _ => new() { MaxGraphSteps = 1 },
        };
        List<CompactIndexOwnershipRecord> records = [];
        using var audit = new CompactIndexOwnershipAudit(20, budget);
        await audit.DeclareRootAsync(2, 3, 0, Visit, Ct);
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            await audit.DeclareRootAsync(2, 3, 1, Visit, Ct);
            await audit.ObserveAsync(Reference(2, 3, 3, 5, "child"), Visit, Ct);
            await audit.ObserveAsync(Reference(2, 3, 3, 5, "tail"), Visit, Ct);
        });
        Assert.DoesNotContain(records, static record => record.Kind == "ownership-terminal");

        ValueTask Visit(CompactIndexOwnershipRecord record, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            records.Add(record);
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public async Task MissingRootTerminal_RefusesCompletion()
    {
        using var audit = new CompactIndexOwnershipAudit(20, new());
        await audit.DeclareRootAsync(2, 3, 0, Ignore, Ct);
        await audit.ObserveAsync(Page(2, 3, 3), Ignore, Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await audit.CompleteAsync(Ignore, Ct));
    }

    [Fact]
    public async Task CancellationAndVisitorFailure_KeepAdmissionUntilDisposal()
    {
        var accounting = new CompactDiscoveryAccounting(new());
        var audit = new CompactIndexOwnershipAudit(20, new() { Accounting = accounting });
        try
        {
            Assert.True(accounting.ActiveWorkingBytes > 0);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await audit.DeclareRootAsync(2, 3, 0, Ignore, cancellation.Token));
        }
        finally
        {
            audit.Dispose();
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
        using var failingAudit = new CompactIndexOwnershipAudit(20, new() { Accounting = accounting });
        await Assert.ThrowsAsync<IOException>(async () => await failingAudit.DeclareRootAsync(2, 3, 0, static (_, _) => throw new IOException("visitor"), Ct));
        Assert.True(accounting.ActiveWorkingBytes > 0);
        failingAudit.Dispose();
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    public async Task NativeCatalogRoots_ComposeRawObservationsWithoutChangingSource(string fixture)
    {
        byte[] image = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        byte[] original = image.ToArray();
        await using var stream = new MemoryStream(image, writable: false);
        await using var file = DatabaseFile.ForReader(stream, image, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        List<long> roots = await IndexLeafChain.ReadRealIndexRootsAsync(file, 2, Ct);
        using var audit = new CompactIndexOwnershipAudit(file.Pages.PageCount, new());
        for (int index = 0; index < roots.Count; index++)
        {
            if (roots[index] != 0)
            {
                await audit.DeclareRootAsync(2, roots[index], index, Ignore, Ct);
                await CompactIndexAudit.ScanAsync(file.Pages, file.Format, 2, roots[index], new(), (record, token) => audit.ObserveAsync(record, Ignore, token), Ct);
            }
        }

        await audit.CompleteAsync(Ignore, Ct);
        Assert.Equal(original, image);
    }

    private static ValueTask Ignore(CompactIndexOwnershipRecord record, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        Assert.True(record.IsPinned);
        return ValueTask.CompletedTask;
    }

    private static CompactIndexRecord Page(long owner, long root, long page)
        => new() { Kind = "index-page", OwnerTDefPage = owner, RootPageNumber = root, PageNumber = page };

    private static CompactIndexRecord Reference(long owner, long root, long source, long target, string role)
        => new() { Kind = "index-reference", OwnerTDefPage = owner, RootPageNumber = root, PageNumber = source, TargetPageNumber = target, Role = role };

    private static CompactIndexRecord Terminal(long owner, long root)
        => new() { Kind = "index-terminal", OwnerTDefPage = owner, RootPageNumber = root, PageNumber = root, NavigableGraphAcyclic = true, EntryEdgeFencesValidated = true };
}
