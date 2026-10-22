namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Native Boolean keys are independently compared with present physical mask values.</summary>
public sealed class CompactBooleanIndexRowAuditTests
{
    /// <summary>Gets native source and compacted specimens in all three formats.</summary>
    public static TheoryData<string> Fixtures =>
    [
        "Jet3-Boolean.mdb", "Jet3-Boolean-compacted.mdb",
        "Jet4-Boolean.mdb", "Jet4-Boolean-compacted.mdb",
        "Ace-Boolean.accdb", "Ace-Boolean-compacted.accdb",
    ];

    /// <summary>Gets source specimens for independent corruption controls.</summary>
    public static TheoryData<string> Sources => ["Jet3-Boolean.mdb", "Jet4-Boolean.mdb", "Ace-Boolean.accdb"];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task FrozenNativeBooleanIndexes_CompareExactKeysBookmarksAndAllActiveIndexes(string fixture)
    {
        byte[] bytes = await ReadFixtureAsync(fixture);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        Assert.Equal(4, image.RealIndexes.Count);
        Dictionary<(long Page, int Row), bool> values = await ReadValuesAsync(file, owner, image, Specimen(fixture).DataPage);
        var parent = new CompactDiscoveryAccounting(new());
        foreach (TDefLogicalIndexDescriptor logical in image.LogicalIndexes)
        {
            int ordinal = logical.Entry.IndexNum2;
            TDefIndexDescriptor index = image.RealIndexes[ordinal];
            Assert.Equal(logical.Name.StartsWith("Asc", StringComparison.Ordinal), index.Columns[0].Ascending);
            Assert.Equal(logical.Name.EndsWith("IgnoreNulls", StringComparison.Ordinal), (index.Slot.Flags & Constants.TableDefinition.IgnoreNullsIndexFlag) != 0);
            Assert.True(CompactIndexRowAudit.SupportsDescriptor(image, ordinal));
            CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), new(), new() { Accounting = parent }, Ct);
            Assert.Equal(6, result.SourceRows);
            Assert.Equal(6, result.IndexedRows);
            Assert.Equal(0, result.ExcludedNullRows);
            Assert.True(result.StoredKeysMatchRows);
            Assert.True(result.ObservedTreeAndBookmarksValidated);
            Assert.False(result.CompleteSourceOwnershipProven);
            Assert.True(result.IsPinned);
            Assert.Equal(0, parent.ActiveWorkingBytes);
            int observed = 0;
            await CompactIndexBookmarkAudit.ScanAsync(
                file.Pages,
                file.Format,
                owner,
                index.RootPage,
                new(),
                new(),
                (record, _) =>
                {
                    if (record.Kind == "leaf-entry")
                    {
                        (long Page, int Row) bookmark = (record.TargetPageNumber!.Value, record.TargetRowIndex!.Value);
                        bool value = values[bookmark];
                        Assert.Equal(NativeKey(value, index.Columns[0].Ascending), record.CanonicalKey);
                        Assert.Equal(Specimen(fixture).DataPage, record.TargetPageNumber);
                        Assert.InRange(record.TargetRowIndex!.Value, (byte)0, (byte)5);
                        Assert.True(record.BookmarkTargetValidated);
                        observed++;
                    }

                    return default;
                },
                Ct);
            Assert.Equal(6, observed);
        }

        Assert.Equal(48, parent.ReadCount(CompactDiscoveryCharge.Fields));
        int covering = image.LogicalIndexes.Single(static index => index.Name == "AscIgnoreNulls").Entry.IndexNum2;
        using (CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, covering, new() { Accounting = parent }, static (_, _) => default, Ct))
        {
            Assert.Equal(6, active.Result.ActiveRows);
            Assert.Equal(4, await active.ValidateIndexesAsync(Ct));
            Assert.False(active.Result.OwnershipProven);
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
        Assert.Equal(bytes, await File.ReadAllBytesAsync(FixturePath(fixture), Ct));
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public async Task WrongMissingDuplicateOrNullKeysAndInvalidBookmarks_Refuse(string fixture)
    {
        byte[] bytes = await ReadFixtureAsync(fixture);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        int ordinal = image.LogicalIndexes.Single(static index => index.Name == "AscIgnoreNulls").Entry.IndexNum2;
        TDefIndexDescriptor index = image.RealIndexes[ordinal];
        List<IndexEntry> original = await IndexLeafChain.ReadEntriesAsync(file, owner, index.RootPage, Ct);
        foreach (string damage in new[] { "wrong-key", "missing-false", "duplicate", "false-as-null", "invalid-bookmark", "opposite-value-bookmark" })
        {
            var entries = new List<IndexEntry>(original);
            switch (damage)
            {
                case "wrong-key":
                    entries[0] = entries[0] with { Key = [0x7F, 0x01] };
                    break;
                case "missing-false":
                    entries.RemoveAt(entries.Count - 1);
                    break;
                case "duplicate":
                    entries[1] = entries[0];
                    break;
                case "false-as-null":
                    entries[^1] = entries[^1] with { Key = [0] };
                    break;
                case "invalid-bookmark":
                    entries[0] = entries[0] with { DataRow = 99 };
                    break;
                default:
                    entries[0] = entries[0] with { DataRow = 0 };
                    break;
            }

            entries.Sort(static (left, right) =>
            {
                int key = left.Key.AsSpan().SequenceCompareTo(right.Key);
                return key != 0 ? key : left.DataRow.CompareTo(right.DataRow);
            });
            var pages = new ChangedPages(file.Pages);
            pages.Changes[index.RootPage] = IndexPageCodec.BuildLeafPage(file.Format.IndexPage, file.Format.PageSize, checked((int)owner), entries, enablePrefixCompression: false);
            var parent = new CompactDiscoveryAccounting(new());
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, file.Format, owner, image, ordinal, new(), new(), new() { Accounting = parent }, Ct));
            Assert.Equal(0, parent.ActiveWorkingBytes);
        }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public async Task PhysicallyAbsentBoolean_DerivesFalseAsANonNullValue(string fixture)
    {
        byte[] bytes = await ReadFixtureAsync(fixture);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        RowLocation? first = null;
        await CompactRawRowReader.ScanAsync(
            file.Pages,
            file.Format,
            owner,
            new(),
            (location, _, _) =>
            {
                first ??= location;
                return default;
            },
            Ct);
        RowLocation target = Assert.IsType<RowLocation>(first);
        byte[] changed = await file.Pages.ReadPageCopyAsync(target.DataPageNumber, Ct);
        changed[target.RowStart] = 1;
        if (!file.Format.IsJet3)
        {
            changed[target.RowStart + 1] = 0;
        }

        var pages = new ChangedPages(file.Pages);
        pages.Changes[target.DataPageNumber] = changed;
        int ordinal = image.LogicalIndexes.Single(static index => index.Name == "AscIgnoreNulls").Entry.IndexNum2;
        var parent = new CompactDiscoveryAccounting(new());
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, file.Format, owner, image, ordinal, new(), new(), new() { Accounting = parent }, Ct);
        Assert.Equal(6, result.IndexedRows);
        Assert.Equal(0, result.ExcludedNullRows);
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("rows")]
    [InlineData("key")]
    [InlineData("fields")]
    [InlineData("working")]
    [InlineData("index-entries")]
    public async Task AdmissionFailure_ReleasesBooleanEvidence(string category)
    {
        byte[] bytes = await ReadFixtureAsync("Jet4-Boolean.mdb");
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var parent = new CompactDiscoveryAccounting(category == "fields" ? new() { MaxFields = 1 } : new());
        var budget = new CompactIndexRowBudget { Accounting = parent, MaxRows = category == "rows" ? 5 : 6, MaxKeyBytes = category == "key" ? 1 : 2, MaxWorkingBytes = category == "working" ? 1 : 128 * 1024 * 1024 };
        var indexBudget = new CompactIndexBudget { MaxEntries = category == "index-entries" ? 1 : 100 };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, 0, new(), indexBudget, budget, Ct));
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SourceFailureAndCancellation_PreserveFailureAndReleaseAdmission(bool cancel)
    {
        byte[] bytes = await ReadFixtureAsync("Jet4-Boolean.mdb");
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        var pages = new ChangedPages(file.Pages);
        var parent = new CompactDiscoveryAccounting(new());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var failure = new IOException("Boolean source failure");
        pages.BeforeRead = () =>
        {
            if (cancel)
            {
                cancellation.Cancel();
            }
            else
            {
                throw failure;
            }
        };
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, file.Format, owner, image, 0, new(), new(), new() { Accounting = parent }, cancellation.Token));
        }
        else
        {
            IOException observed = await Assert.ThrowsAsync<IOException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, file.Format, owner, image, 0, new(), new(), new() { Accounting = parent }, cancellation.Token));
            Assert.Same(failure, observed);
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    private static byte[] NativeKey(bool value, bool ascending)
    {
        string hex = (value, ascending) switch
        {
            (true, true) => "7F00",
            (false, true) => "7FFF",
            (true, false) => "80FF",
            _ => "8000",
        };
        return Convert.FromHexString(hex);
    }

    private static string FixturePath(string name) => Path.Combine(AppContext.BaseDirectory, "Databases", "NativeBooleanIndexes", name);

    private static (string Hash, int DataPage) Specimen(string name) => name switch
    {
        "Jet3-Boolean.mdb" => ("7062F0A54807195FF772095BA80F86E4B475D2E62408020C86F9707A057ADE54", 23),
        "Jet3-Boolean-compacted.mdb" => ("31222827F48795DC5B8D4CDC03AD089819BC2A88C5A8B46C53A5DE2D21D6BA8A", 21),
        "Jet4-Boolean.mdb" => ("89F8B0E8A90618CB87EB10EF4F517AF293B7F4840D4B56FED89F94C88E7FE607", 22),
        "Jet4-Boolean-compacted.mdb" => ("1BEF6777E2BF2CC5C6BEE5F188D991E445666F8994408385BB3535DBEED475CD", 20),
        "Ace-Boolean.accdb" => ("F7A09D655CB21D8E0AD09B4FBC2265FDD67BBAA6BBA5BA600D6B34B680C48C3E", 45),
        "Ace-Boolean-compacted.accdb" => ("C2B0A7712FEA1EF26865BE60EA29EDA01B757A0DE8C787210087C39A3A8C36FB", 43),
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    private static async Task<byte[]> ReadFixtureAsync(string name)
    {
        byte[] bytes = await File.ReadAllBytesAsync(FixturePath(name), Ct);
        Assert.Equal(Specimen(name).Hash, Convert.ToHexString(SHA256.HashData(bytes)));
        return bytes;
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private static async Task<(long Owner, TDefImage Image)> FindAsync(DatabaseFile file)
    {
        (long, TDefImage) found = default;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, (owner, image, _) =>
        {
            if (image.Columns.Count == 2 && image.Columns[0].Name == "Id" && image.Columns[1].Name == "Flag")
            {
                found = (owner, image);
            }

            return default;
        });
        Assert.NotNull(found.Item2);
        return found;
    }

    private static async Task<Dictionary<(long Page, int Row), bool>> ReadValuesAsync(DatabaseFile file, long owner, TDefImage image, int dataPage)
    {
        var values = new Dictionary<(long Page, int Row), bool>();
        await CompactRawRowReader.ScanAsync(
            file.Pages,
            file.Format,
            owner,
            new(),
            async (location, payload, token) =>
            {
                int id = 0;
                bool value = false;
                await CompactRawFieldReader.ScanAsync(
                    file.Format,
                    image,
                    location,
                    payload,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Id")
                        {
                            id = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                        }
                        else
                        {
                            Assert.Equal(CompactRawFieldKind.Boolean, field.Kind);
                            value = field.BoolValue;
                        }

                        return default;
                    },
                    token);
                Assert.Equal(dataPage, location.PageNumber);
                Assert.InRange(id, 1, 6);
                Assert.Equal(id - 1, location.RowIndex);
                Assert.Equal(id is 2 or 5, value);
                values.Add((location.PageNumber, location.RowIndex), value);
            },
            Ct);
        Assert.Equal(6, values.Count);
        return values;
    }

    private sealed class ChangedPages(IPageSource inner) : IPageSource
    {
        public int PageSize => inner.PageSize;

        public long PageCount => inner.PageCount;

        public bool IsDisposed => inner.IsDisposed;

        internal Dictionary<long, byte[]> Changes { get; } = [];

        internal Action? BeforeRead { get; set; }

        public async ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            this.BeforeRead?.Invoke();
            cancellationToken.ThrowIfCancellationRequested();
            if (this.Changes.TryGetValue(pageNumber, out byte[]? changed))
            {
                byte[] copy = ArrayPool<byte>.Shared.Rent(this.PageSize);
                changed.CopyTo(copy, 0);
                return copy;
            }

            return await inner.ReadUncachedPageAsync(pageNumber, cancellationToken);
        }

        public void ThrowIfDisposed() => inner.ThrowIfDisposed();

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => inner.ThrowIfDisposedOrCancelled(cancellationToken);
    }
}
