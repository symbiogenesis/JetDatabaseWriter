namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Exact separator evidence includes complete observed aliases and bookmark tie breakers.</summary>
public sealed class CompactIndexSeparatorAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RedundantNativeTailAlias_StillHasAnExactObservedChildMaximum(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([3], 9, 1), 4)], 0, 0, 7),
            [4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 0, 7),
            [6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 7, 0, true),
            [7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([3], 9, 1)], 6, 0, 0, true),
        });
        CompactIndexRecord terminal = await ScanAsync(pages, format);
        Assert.True(terminal.ObservedChildMaximumSeparatorsValidated);
        Assert.True(terminal.ObservedLeafChainOrdered);
        Assert.False(terminal.TreeCompletenessProven);
    }

    [Theory]
    [InlineData("key")]
    [InlineData("bookmark")]
    [InlineData("empty")]
    [InlineData("aliased-tail")]
    public async Task OrderedFences_WithoutExactLeafCorrespondenceWithholdTheProof(string mismatch)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        IndexEntry separator = new(mismatch == "key" ? [2] : [1], 9, mismatch == "bookmark" ? (byte)1 : (byte)0);
        var images = new Dictionary<long, byte[]>
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(separator, 4)], 0, 0, 0),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, mismatch == "empty" ? [] : [new([1], 9, 0)], enablePrefixCompression: false),
        };
        if (mismatch == "aliased-tail")
        {
            images[4] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 0), 6)], 0, 0, 7);
            images[6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 0)], 0, 7, 0, true);
            images[7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([2], 9, 1)], 6, 0, 0, true);
        }

        CompactIndexRecord terminal = await ScanAsync(new(format.PageSize, images), format);
        Assert.True(terminal.EntryEdgeFencesValidated);
        Assert.False(terminal.ObservedChildMaximumSeparatorsValidated);
        Assert.True(terminal.IsPinned);
    }

    [Fact]
    public async Task EqualKeyBookmarkMaximum_IsIndependentOfOptionalNativeTieOrdering()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(new([1], 9, 2), 4)], 0, 0, 0),
            [4] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new([1], 9, 2), new([1], 9, 0)], enablePrefixCompression: false),
        });
        CompactIndexRecord terminal = await ScanAsync(pages, format);
        Assert.True(terminal.ObservedChildMaximumSeparatorsValidated);
        Assert.False(terminal.BookmarkInclusiveOrderValidated);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeInventory_SeparatorsNameExactObservedChildLeafMaxima(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        int roots = 0;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, async (owner, image, token) =>
        {
            foreach (TDefIndexDescriptor index in image.RealIndexes)
            {
                if (index.RootPage == 0)
                {
                    continue;
                }

                await CompactIndexAudit.ScanAsync(
                    file.Pages,
                    file.Format,
                    owner,
                    index.RootPage,
                    new(),
                    (record, _) =>
                    {
                        if (record.Kind == "index-terminal")
                        {
                            Assert.True(record.ObservedChildMaximumSeparatorsValidated, $"Owner {owner}, root {index.RootPage}");
                            roots++;
                        }

                        return default;
                    },
                    token);
            }
        });
        Assert.True(roots > 0);
    }

    private static async ValueTask<CompactIndexRecord> ScanAsync(MemoryPages pages, JetFormat format)
    {
        CompactIndexRecord? terminal = null;
        await CompactIndexAudit.ScanAsync(
            pages,
            format,
            2,
            3,
            new(),
            (record, _) =>
            {
                if (record.Kind == "index-terminal")
                {
                    terminal = record;
                }

                return default;
            },
            Ct);
        Assert.NotNull(terminal);
        return terminal;
    }

    private sealed class MemoryPages(int pageSize, Dictionary<long, byte[]> images) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => 20;

        public bool IsDisposed => false;

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            images[pageNumber].CopyTo(buffer, 0);
            return ValueTask.FromResult(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
