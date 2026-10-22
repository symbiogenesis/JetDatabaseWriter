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
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueEncoding;
using Xunit;

/// <summary>Stored scalar keys are compared with independent raw-row decoding, including missing entries.</summary>
public sealed class CompactIndexRowAuditTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task IntegerRows_HaveOneMatchingStoredEntryEach(DatabaseFormat kind)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(kind);
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(3, result.SourceRows);
        Assert.Equal(3, result.IndexedRows);
        Assert.Equal(0, result.ExcludedNullRows);
        Assert.True(result.StoredKeysMatchRows);
        Assert.True(result.DiscoveredRowsCovered);
        Assert.False(result.CompleteSourceOwnershipProven);
        Assert.True(result.IsPinned);
    }

    [Theory]
    [InlineData("wrong-key")]
    [InlineData("duplicate")]
    [InlineData("missing")]
    [InlineData("unknown-row")]
    public async Task OrderedValidTree_WithIncorrectRowCorrespondenceRefuses(string damage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        List<IndexEntry> entries = [Entry(10, 0), Entry(20, 1), Entry(30, 2)];
        switch (damage)
        {
            case "wrong-key":
                entries[1] = Entry(21, 1);
                break;
            case "duplicate":
                entries[1] = entries[0];
                break;
            case "missing":
                entries.RemoveAt(1);
                break;
            default:
                entries.Add(new(Entry(40, 0).Key, 5, 0));
                break;
        }

        pages.Images[3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData("separator-key")]
    [InlineData("separator-bookmark")]
    [InlineData("disconnected-leaves")]
    public async Task MatchingLeafRows_RequireComposedRoutingSeparatorAndChainEvidence(string damage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        IndexEntry separator = damage == "separator-key" ? Entry(40, 2) : Entry(30, 1);
        pages.Images[3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(separator, 6)], 0, 0, 0);
        pages.Images[6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: false);
        if (damage == "disconnected-leaves")
        {
            pages.Images[3] = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, 2, [new(Entry(20, 1), 6)], 0, 0, 7);
            pages.Images[6] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1)], enablePrefixCompression: false);
            pages.Images[7] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(30, 2)], enablePrefixCompression: false);
        }

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IgnoreNulls_ExcludesOnlyAllNullCompositeRows(bool ignoreNulls)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        ColumnInfo[] columns = [new() { Name = "A", ColNum = 0, Type = ColumnType.LongIntegerType, Flags = 1, Size = 4 }, new() { Name = "B", ColNum = 1, Type = ColumnType.LongIntegerType, Flags = 1, FixedOff = 4, Size = 4 }];
        TDefImage image = Image(columns, [new(0, true), new(1, false)], ignoreNulls ? Constants.TableDefinition.IgnoreNullsIndexFlag : (byte)0);
        List<IndexEntry> entries = [];
        object?[][] rows = [[null, null], [null, 20], [10, null]];
        for (int row = 0; row < rows.Length; row++)
        {
            if (ignoreNulls && row == 0)
            {
                continue;
            }

            byte[] a = IndexKeyEncoder.EncodeColumnEntry(format, columns[0], rows[row][0]);
            byte[] b = IndexKeyEncoder.EncodeColumnEntry(format, columns[1], rows[row][1], ascending: false);
            entries.Add(new([.. a, .. b], 4, checked((byte)row)));
        }

        entries.Sort(static (left, right) => left.Key.AsSpan().SequenceCompareTo(right.Key));
        var pages = new MemoryPages(format.PageSize, new() { [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: true), [4] = Data(format, image, rows) });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(ignoreNulls ? 1 : 0, result.ExcludedNullRows);
        Assert.Equal(ignoreNulls ? 2 : 3, result.IndexedRows);
    }

    [Theory]
    [InlineData(ColumnType.BigBinaryType)]
    [InlineData(ColumnType.OleType)]
    public async Task UnsupportedKeyTypes_RefuseBeforeReading(ColumnType type)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Opaque", ColNum = 0, Type = type }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, []);
        Assert.False(CompactIndexRowAudit.SupportsDescriptor(image, 0));
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "inline")]
    [InlineData(DatabaseFormat.Jet3Mdb, "single")]
    [InlineData(DatabaseFormat.Jet3Mdb, "chain")]
    [InlineData(DatabaseFormat.Jet4Mdb, "inline")]
    [InlineData(DatabaseFormat.Jet4Mdb, "single")]
    [InlineData(DatabaseFormat.Jet4Mdb, "chain")]
    [InlineData(DatabaseFormat.AceAccdb, "inline")]
    [InlineData(DatabaseFormat.AceAccdb, "single")]
    [InlineData(DatabaseFormat.AceAccdb, "chain")]
    public async Task MemoKeys_CompareCompleteDeclaredPayloadInEveryStorageMode(DatabaseFormat kind, string storage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Memo(kind, storage, "stored memo text");
        Assert.True(CompactIndexRowAudit.SupportsDescriptor(image, 0));
        var accounting = new CompactDiscoveryAccounting(new());
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new() { Accounting = accounting }, Ct);
        Assert.Equal(1, result.IndexedRows);
        Assert.True(result.StoredKeysMatchRows);
        Assert.Equal(1, accounting.ReadCount(CompactDiscoveryCharge.LongValues));
        Assert.True(accounting.ReadCount(CompactDiscoveryCharge.LongValuePayloadBytes) > 0);
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ChainedMemoKeys_PreserveLeadingFeffWithoutTreatingItAsACompressionMarker(DatabaseFormat kind)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Memo(kind, "chain", "\uFEFFstored memo text");
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(1, result.IndexedRows);
    }

    [Theory]
    [InlineData("descriptor")]
    [InlineData("reserved-mode")]
    [InlineData("short-chain")]
    [InlineData("trailing-cycle")]
    [InlineData("trailing-dangling")]
    [InlineData("wrong-key")]
    [InlineData("unicode-odd")]
    [InlineData("compressed-half")]
    public async Task MemoKeys_RefuseDamagedPayloadsAndInvalidTrailingReferences(string damage)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Memo(DatabaseFormat.Jet4Mdb, "chain", "stored memo text");
        TDefImage binary = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.BinaryType }], [new(0, true)], 0);
        if (damage is "descriptor" or "reserved-mode" or "unicode-odd" or "compressed-half")
        {
            byte[] column = damage switch
            {
                "descriptor" => [1, 2, 3],
                "reserved-mode" => [0, 0, 0, 0xC0, 0, 0, 0, 0, 0, 0, 0, 0],
                "unicode-odd" => [.. LongValueDescriptor.Inline(1).ToHeaderBytes(), 0x41],
                _ => [.. LongValueDescriptor.Inline(4).ToHeaderBytes(), 0xFF, 0xFE, 0, 0x41],
            };
            pages.Images[4] = Data(format, binary, [[column]]);
        }
        else if (damage == "wrong-key")
        {
            pages.Images[3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, "different memo"), 4, 0)], enablePrefixCompression: false);
        }
        else if (damage == "short-chain")
        {
            pages.Images[6] = LongValuePage(format, [0, 0, 0, 0]);
        }
        else
        {
            byte[] page = pages.Images[6];
            int start = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart));
            BinaryPrimitives.WriteUInt32LittleEndian(page.AsSpan(start), (damage == "trailing-cycle" ? 5u : 19u) << 8);
        }

        var parent = new CompactDiscoveryAccounting(new());
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new() { Accounting = parent }, Ct));
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("value")]
    [InlineData("payload")]
    [InlineData("raw-bytes")]
    [InlineData("values")]
    [InlineData("reads")]
    [InlineData("chain")]
    [InlineData("storage")]
    [InlineData("simultaneous")]
    [InlineData("key")]
    public async Task MemoKeys_ApplyValueWorkAndSimultaneousStorageLimits(string limit)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Memo(DatabaseFormat.Jet4Mdb, "chain", "stored memo text");
        CompactDiscoveryBudget values = limit switch
        {
            "value" => new() { MaxValueBytes = 1 },
            "payload" => new() { MaxLongValuePayloadBytes = 1 },
            "raw-bytes" => new() { MaxLongValueBytes = 1 },
            "values" => new() { MaxLongValues = 1 },
            "reads" => new() { MaxPageReads = 1 },
            "chain" => new() { MaxChainRows = 1 },
            "storage" => new() { MaxWorkingBytes = 65_536 },
            _ => new(),
        };
        if (limit == "values")
        {
            byte[] header = LongValueDescriptor.Chained(format.EncodeText("stored memo text", compress: false).Length, 5u << 8, 0).ToHeaderBytes();
            TDefImage binary = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.BinaryType }], [new(0, true)], 0);
            pages.Images[4] = Data(format, binary, [[header], [header]]);
            byte[] key = IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, "stored memo text");
            pages.Images[3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0), new(key, 4, 1)], enablePrefixCompression: false);
        }

        var parent = new CompactDiscoveryAccounting(new());
        var budget = new CompactIndexRowBudget { Accounting = parent, LongValues = values, MaxWorkingBytes = limit == "simultaneous" ? 400_000 : 128 * 1024 * 1024, MaxKeyBytes = limit == "key" ? 1 : 65_536 };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), budget, Ct));
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MemoKeys_DistinguishAssignedEmptyFromNullBeforeIgnoreNulls(bool ignoreNulls)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.MemoType }], [new(0, true)], ignoreNulls ? Constants.TableDefinition.IgnoreNullsIndexFlag : (byte)0);
        TDefImage binary = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.BinaryType }], [new(0, true)], 0);
        byte[] header = LongValueDescriptor.Inline(0).ToHeaderBytes();
        byte[] data = Data(format, binary, [[header], [header]]);
        int firstRow = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(format.DataPage.RowsStart));
        data[firstRow - 1] &= 0xFE;
        var entries = new List<IndexEntry> { new(IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, string.Empty), 4, 0) };
        if (!ignoreNulls)
        {
            entries.Add(new(IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, null), 4, 1));
            entries.Sort(static (left, right) => left.Key.AsSpan().SequenceCompareTo(right.Key));
        }

        var pages = new MemoryPages(format.PageSize, new() { [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false), [4] = data });
        var parent = new CompactDiscoveryAccounting(new());
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new() { Accounting = parent }, Ct);
        Assert.Equal(ignoreNulls ? 1 : 2, result.IndexedRows);
        Assert.Equal(ignoreNulls ? 1 : 0, result.ExcludedNullRows);
        Assert.Equal(1, parent.ReadCount(CompactDiscoveryCharge.LongValues));
    }

    [Fact]
    public async Task CompositeMemoKeys_PreserveEachColumnDirection()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        ColumnInfo[] columns = [new() { Name = "Id", ColNum = 0, Type = ColumnType.LongIntegerType, Flags = 1, Size = 4 }, new() { Name = "Body", ColNum = 1, Type = ColumnType.MemoType }];
        TDefImage image = Image(columns, [new(0, true), new(1, false)], 0);
        TDefImage binary = Image([columns[0], columns[1] with { Type = ColumnType.BinaryType }], [new(0, true), new(1, false)], 0);
        string[] text = ["stored first", "stored second"];
        var entries = new List<IndexEntry>();
        object?[][] rows = new object?[2][];
        for (int row = 0; row < rows.Length; row++)
        {
            byte[] payload = format.EncodeText(text[row]);
            byte[] column = [.. LongValueDescriptor.Inline(payload.Length).ToHeaderBytes(), .. payload];
            rows[row] = [row + 1, column];
            byte[] id = IndexKeyEncoder.EncodeColumnEntry(format, columns[0], row + 1);
            byte[] memo = IndexKeyEncoder.EncodeColumnEntry(format, columns[1], text[row], ascending: false);
            entries.Add(new([.. id, .. memo], 4, checked((byte)row)));
        }

        var pages = new MemoryPages(format.PageSize, new() { [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, entries, enablePrefixCompression: false), [4] = Data(format, binary, rows) });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(2, result.IndexedRows);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MemoKeys_PreserveIoFailureAndCancellationWhileReleasingAdmission(bool cancel)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Memo(DatabaseFormat.Jet4Mdb, "chain", "stored memo text");
        var parent = new CompactDiscoveryAccounting(new());
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var failure = new IOException("Memo source failure");
        pages.BeforeRead = page =>
        {
            if (page == 5)
            {
                if (cancel)
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw failure;
                }
            }
        };
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new() { Accounting = parent }, cancellation.Token));
        }
        else
        {
            IOException observed = await Assert.ThrowsAsync<IOException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new() { Accounting = parent }, cancellation.Token));
            Assert.Same(failure, observed);
        }

        Assert.Equal(0, parent.ActiveWorkingBytes);
        Assert.True(parent.ReadCount(CompactDiscoveryCharge.LongValuePayloadBytes) > 0);
    }

    [Theory]
    [InlineData("Jackcess/V2000/testIndexCodesV2000.mdb")]
    [InlineData("Jackcess/V2003/testIndexCodesV2003.mdb")]
    [InlineData("Jackcess/V2007/testIndexCodesV2007.accdb")]
    [InlineData("Jackcess/V2010/testIndexCodesV2010.accdb")]
    public async Task NativeMemoIndexes_CompareCompleteStoredKeysIncludingLongRowSuffixes(string fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        int verified = 0;
        int longKeys = 0;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, async (owner, image, token) =>
        {
            for (int ordinal = 0; ordinal < image.RealIndexes.Count; ordinal++)
            {
                TDefIndexDescriptor index = image.RealIndexes[ordinal];
                if (index.Columns.Count != 1 || image.Columns.Single(column => column.ColNum == index.Columns[0].ColNum).Type != ColumnType.MemoType)
                {
                    continue;
                }

                CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), new(), new(), token);
                Assert.True(result.ObservedTreeAndBookmarksValidated);
                await CompactIndexAudit.ScanAsync(
                    file.Pages,
                    file.Format,
                    owner,
                    index.RootPage,
                    new(),
                    (record, _) =>
                    {
                        if (record.Kind == "leaf-entry" && record.CanonicalKey!.Length == 510)
                        {
                            longKeys++;
                        }

                        return default;
                    },
                    token);
                verified++;
            }
        });
        Assert.True(verified > 0);
        if (fixture.Contains("V2010", StringComparison.Ordinal))
        {
            Assert.True(longKeys > 0);
        }
    }

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeCalculatedColumns_RefusePhysicalIndexesWhileOrdinaryControlSucceeds()
    {
        string fixture = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "Jackcess", "V2010", "calcFieldTestV2010.accdb");
        byte[] bytes = await File.ReadAllBytesAsync(fixture, Ct);
        byte[] originalHash = SHA256.HashData(bytes);
        await using var native = AccessRoundTripSession.CreateEmpty(Path.Combine("jdw-compact-deps-20261022-0bd7253d", "ownership-memo", "native-calculated-index"));
        await File.WriteAllBytesAsync(native.SourcePath, bytes, Ct);
        string path = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(
            $$"""
            $db = $engine.OpenDatabase({{path}}, $true, $false)
            try {
                Write-Output "ENGINE=$($engine.Version)"
                Get-Process -Id $PID | ForEach-Object { $_.Modules } | Where-Object { $_.ModuleName -match 'acedao' } | ForEach-Object {
                    Write-Output "DAO_DLL=$($_.FileName) VERSION=$([Diagnostics.FileVersionInfo]::GetVersionInfo($_.FileName).FileVersion)"
                }
                $table = $db.TableDefs.Item('Table1')
                $ordinal = 0
                foreach ($name in @('FirstName', 'LastFirstLen', 'LastFirst', 'MonthlySalary', 'WeeklySalary')) {
                    $indexName = 'CompactProbe' + $ordinal
                    $ordinal++
                    $index = $table.CreateIndex($indexName)
                    $index.Fields.Append($index.CreateField($name))
                    try {
                        $table.Indexes.Append($index)
                        Write-Output "INDEX_ACCEPTED=$name"
                        $table.Indexes.Delete($indexName)
                    } catch {
                        Write-Output "INDEX_REFUSED=$name CODE=$($_.Exception.HResult -band 0xFFFF)"
                    }
                }
            } finally { $db.Close() }
            """,
            TimeSpan.FromMinutes(1));
        Assert.True(result.ExitCode == 0, $"Native calculated index control failed:\n{result.StdOut}\n{result.StdErr}");
        TestContext.Current.SendDiagnosticMessage(result.StdOut);
        TestContext.Current.SendDiagnosticMessage("ORIGINAL_SHA256=" + Convert.ToHexString(originalHash));
        Assert.Contains("INDEX_ACCEPTED=FirstName", result.StdOut, StringComparison.Ordinal);
        foreach (string field in new[] { "LastFirstLen", "LastFirst", "MonthlySalary", "WeeklySalary" })
        {
            Assert.Contains($"INDEX_REFUSED={field} CODE=3251", result.StdOut, StringComparison.Ordinal);
        }

        Assert.Equal(originalHash, SHA256.HashData(await File.ReadAllBytesAsync(fixture, Ct)));
    }

    [Fact]
    public async Task ComplexReferenceKeys_PreserveRawParentIdentitiesWithoutFollowingItems()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Items", ColNum = 0, Type = ColumnType.ComplexType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: false),
            [4] = Data(format, image, [[10], [20], [30]]),
        });
        Assert.True(CompactIndexRowAudit.SupportsDescriptor(image, 0));
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.Equal(3, result.IndexedRows);
        Assert.True(result.StoredKeysMatchRows);
        Assert.False(result.CompleteSourceOwnershipProven);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ComplexReferenceKeys_RejectMatchingNonpositiveAssignedIdentities(int reference)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "Items", ColNum = 0, Type = ColumnType.ComplexType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(reference, 0)], enablePrefixCompression: false),
            [4] = Data(format, image, [[reference]]),
        });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Fact]
    public async Task DateKeys_RetainSubMillisecondStoredDoublePrecision()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        TDefImage image = Image([new() { Name = "When", ColNum = 0, Type = ColumnType.DateTimeType, Flags = 1, Size = 8 }], [new(0, true)], 0);
        byte[] data = Data(format, image, [[new DateTime(2026, 1, 1)]]);
        int rowStart = BinaryPrimitives.ReadUInt16LittleEndian(data.AsSpan(format.DataPage.RowsStart));
        const double stored = 45_000.123456789;
        BinaryPrimitives.WriteDoubleLittleEndian(data.AsSpan(rowStart + format.RowFields.NumCols), stored);
        byte[] key = IndexKeyEncoder.EncodeEntry(ColumnType.DoubleType, stored);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = data,
        });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.True(result.StoredKeysMatchRows);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TextKeys_RefuseIncompleteCodeUnitsInsteadOfComparingLossyEmptyText(bool compressed)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet4Mdb);
        TDefImage image = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.TextType, Size = 16, ExtraFlags = compressed ? Constants.CompressedUnicodeExtFlagMask : (byte)0 }], [new(0, true)], 0);
        TDefImage binary = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.BinaryType, Size = 16 }], [new(0, true)], 0);
        byte[] key = IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, string.Empty);
        byte[] storedText = compressed ? [0xFF, 0xFE, 0, 0x41] : "A"u8.ToArray();
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = Data(format, binary, [[storedText]]),
        });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UncompressedText_LeadingByteOrderMarkRemainsAStoredCodeUnit(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.TextType, Size = 16 }], [new(0, true)], 0);
        TDefImage binary = Image([new() { Name = "Text", ColNum = 0, Type = ColumnType.BinaryType, Size = 16 }], [new(0, true)], 0);
        byte[] key = IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, "\uFEFFA");
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(key, 4, 0)], enablePrefixCompression: false),
            [4] = Data(format, binary, [[new byte[] { 0xFF, 0xFE, 0x41, 0 }]]),
        });
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), new(), Ct);
        Assert.True(result.StoredKeysMatchRows);
        Assert.True(result.ObservedTreeAndBookmarksValidated);
    }

    [Theory]
    [InlineData("rows")]
    [InlineData("key-bytes")]
    [InlineData("working")]
    public async Task EvidenceBudget_RefusesAndReleasesAggregateAdmission(string limit)
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        var accounting = new CompactDiscoveryAccounting(new());
        CompactIndexRowBudget budget = limit switch
        {
            "rows" => new() { Accounting = accounting, MaxRows = 1 },
            "key-bytes" => new() { Accounting = accounting, MaxKeyBytes = 1 },
            _ => new() { Accounting = accounting, MaxWorkingBytes = 1 },
        };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), budget, Ct));
        Assert.Equal(0, accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task ScalarIndexes_DoNotConsumeOrValidateIrrelevantMemoAdmission()
    {
        (MemoryPages pages, JetFormat format, TDefImage image) = Create(DatabaseFormat.Jet4Mdb);
        var parent = new CompactDiscoveryAccounting(new() { MaxFields = 3 });
        var budget = new CompactIndexRowBudget
        {
            Accounting = parent,
            LongValues = new() { MaxPages = 3, MaxFields = 1, MaxGraphSteps = 1, MaxWorkingBytes = 1 },
        };
        CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(pages, format, 2, image, 0, new(), new(), budget, Ct);
        Assert.Equal(3, result.IndexedRows);
        Assert.Equal(3, parent.ReadCount(CompactDiscoveryCharge.Fields));
        Assert.Equal(0, parent.ReadCount(CompactDiscoveryCharge.LongValues));
        Assert.Equal(0, parent.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("FixedText/NativeFixedTextJet3Cp1252.mdb")]
    [InlineData("FixedText/NativeFixedTextJet4.mdb")]
    [InlineData("NorthwindTraders.accdb")]
    [InlineData("ComplexFields.accdb")]
    [InlineData("mdbtools/nwind.mdb")]
    public async Task NativeSupportedScalarIndexes_MatchTheirIndependentlyDecodedRows(string fixture)
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", fixture), Ct);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: Ct);
        int verified = 0;
        int ambiguousPhysicalScopes = 0;
        await CompactStorageInventory.ScanAsync(file.Pages, file.Format, file.TableDefs, new(), static (_, _) => default, Ct, async (owner, image, token) =>
        {
            for (int ordinal = 0; ordinal < image.RealIndexes.Count; ordinal++)
            {
                if (!CompactIndexRowAudit.SupportsDescriptor(image, ordinal) || image.RealIndexes[ordinal].RootPage == 0)
                {
                    continue;
                }

                CompactIndexRowAuditResult result;
                try
                {
                    result = await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), new(), new(), token);
                }
                catch (JetCorruptDataException exception) when (fixture == "mdbtools/nwind.mdb" && owner == 753 && exception.Message.StartsWith("An independently discovered included row is missing", StringComparison.Ordinal))
                {
                    // Umsätze has historical same-owner DATA outside its active
                    // map. Physical headers alone cannot choose which rows to drop.
                    Assert.Equal(830u, image.Header.Counters.RowCount);
                    Assert.Contains("rows 2879/830", exception.Message, StringComparison.Ordinal);
                    IReadOnlyList<long> mapped = await file.OwnedPages.GetOwnedDataPagesAsync(owner, token);
                    Assert.DoesNotContain(843, mapped);
                    ambiguousPhysicalScopes++;
                    continue;
                }
                catch (JetCorruptDataException exception)
                {
                    string keys = string.Join(", ", image.RealIndexes[ordinal].Columns.Select(key => image.Columns.Single(column => column.ColNum == key.ColNum)).Select(column => $"{column.Name}/{column.Type}/{column.TextSortOrder}"));
                    throw new InvalidDataException($"Native owner {owner}, descriptor {ordinal}: {keys}", exception);
                }

                Assert.True(result.StoredKeysMatchRows);
                Assert.True(result.DiscoveredRowsCovered);
                verified++;
            }
        });
        Assert.True(verified > 0);
        Assert.Equal(fixture == "mdbtools/nwind.mdb", ambiguousPhysicalScopes > 0);
    }

    private static (MemoryPages Pages, JetFormat Format, TDefImage Image) Create(DatabaseFormat kind)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = Image([new() { Name = "Id", ColNum = 0, Type = ColumnType.LongIntegerType, Flags = 1, Size = 4 }], [new(0, true)], 0);
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [Entry(10, 0), Entry(20, 1), Entry(30, 2)], enablePrefixCompression: true),
            [4] = Data(format, image, [[10], [20], [30]]),
        });
        return (pages, format, image);
    }

    private static (MemoryPages Pages, JetFormat Format, TDefImage Image) Memo(DatabaseFormat kind, string storage, string text)
    {
        var format = JetFormat.ForNewDatabase(kind);
        TDefImage image = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.MemoType }], [new(0, true)], 0);
        TDefImage binary = Image([new() { Name = "Body", ColNum = 0, Type = ColumnType.BinaryType }], [new(0, true)], 0);
        byte[] payload = format.EncodeText(text, compress: storage != "chain");
        LongValueDescriptor descriptor = storage switch
        {
            "inline" => LongValueDescriptor.Inline(payload.Length),
            "single" => LongValueDescriptor.SinglePage(payload.Length, 5u << 8, 0),
            _ => LongValueDescriptor.Chained(payload.Length, 5u << 8, 0),
        };
        byte[] column = storage == "inline" ? [.. descriptor.ToHeaderBytes(), .. payload] : descriptor.ToHeaderBytes();
        var pages = new MemoryPages(format.PageSize, new()
        {
            [3] = IndexPageCodec.BuildLeafPage(format.IndexPage, format.PageSize, 2, [new(IndexKeyEncoder.EncodeTextEntry(image.Columns[0].TextSortOrder, text), 4, 0)], enablePrefixCompression: false),
            [4] = Data(format, binary, [[column]]),
        });
        if (storage == "single")
        {
            pages.Images[5] = LongValuePage(format, payload);
        }
        else if (storage == "chain")
        {
            int split = payload.Length / 2;
            byte[] first = new byte[sizeof(uint) + split];
            BinaryPrimitives.WriteUInt32LittleEndian(first, 6u << 8);
            payload.AsSpan(0, split).CopyTo(first.AsSpan(sizeof(uint)));
            pages.Images[5] = LongValuePage(format, first);
            pages.Images[6] = LongValuePage(format, [0, 0, 0, 0, .. payload.AsSpan(split).ToArray()]);
        }

        return (pages, format, image);
    }

    private static byte[] LongValuePage(JetFormat format, byte[] row)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        "LVAL"u8.CopyTo(page.AsSpan(4));
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows), 1);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart), checked((ushort)(format.PageSize - row.Length)));
        row.CopyTo(page, format.PageSize - row.Length);
        return page;
    }

    private static TDefImage Image(ColumnInfo[] columns, List<KeyColumn> keys, byte flags)
        => new([], new() { ColumnCount = columns.Length, VariableColumnCount = columns.Count(static column => !column.IsFixed) }, [.. columns], false, TDefParseIssues.None, null, [new(new(0, 0, flags), 3, keys, [])], []) { TDefPageNumber = 2 };

    private static IndexEntry Entry(int value, byte row) => new(IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, value), 4, row);

    private static byte[] Data(JetFormat format, TDefImage image, object?[][] rows)
    {
        byte[] page = new byte[format.PageSize];
        page[0] = Constants.PageTypes.Data;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff), 2);
        BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.NumRows), checked((ushort)rows.Length));
        int end = format.PageSize;
        for (int row = 0; row < rows.Length; row++)
        {
            object[] values = rows[row].Select(static value => value ?? 0).ToArray();
            byte[] payload = new RowEncoder(format).SerializeRow(image.Definition, values);
            for (int column = 0; column < rows[row].Length; column++)
            {
                if (rows[row][column] is null)
                {
                    int bit = image.Columns[column].ColNum;
                    int nullMaskStart = payload.Length - JetFormat.GetNullMaskSizeBytes(image.Columns.Count);
                    payload[nullMaskStart + (bit / 8)] &= unchecked((byte)~(1 << (bit % 8)));
                }
            }

            end -= payload.Length;
            payload.CopyTo(page, end);
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(format.DataPage.RowsStart + (row * 2)), checked((ushort)end));
        }

        return page;
    }

    private sealed class MemoryPages(int pageSize, Dictionary<long, byte[]> images) : IPageSource
    {
        public int PageSize => pageSize;

        public long PageCount => 20;

        public bool IsDisposed => false;

        internal Dictionary<long, byte[]> Images => images;

        internal Action<long>? BeforeRead { get; set; }

        public ValueTask<byte[]> ReadPageAsync(long pageNumber, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            this.BeforeRead?.Invoke(pageNumber);
            cancellationToken.ThrowIfCancellationRequested();
            byte[] buffer = ArrayPool<byte>.Shared.Rent(pageSize);
            Array.Clear(buffer, 0, buffer.Length);
            if (images.TryGetValue(pageNumber, out byte[]? image))
            {
                image.CopyTo(buffer, 0);
            }

            return ValueTask.FromResult(buffer);
        }

        public void ThrowIfDisposed()
        {
        }

        public void ThrowIfDisposedOrCancelled(CancellationToken cancellationToken) => cancellationToken.ThrowIfCancellationRequested();
    }
}
