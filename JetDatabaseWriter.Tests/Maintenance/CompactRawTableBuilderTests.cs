namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Raw table staging composes native maps without replacing the source or interpreting stored values.</summary>
public sealed class CompactRawTableBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task Build_ReopenedRowsAndIndependentColumnMapsCoverOnlyPrivateAllocations(DatabaseFormat kind, bool empty)
    {
        (byte[] original, long owner) = await CreateAsync(kind, empty);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        LogicalTDefChain chain = await source.Database.TableDefs.ReadTDefChainAsync(owner, Ct);
        Assert.Single(chain.PageNumbers);
        Assert.Empty(image.RealIndexes);
        var session = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();

        CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, format, owner, image, destination, rows, values, [], new(), Ct);

        Assert.False(result.IsValidated);
        Assert.False(result.OwnershipProven);
        Assert.Equal(original, destination.ToArray().AsSpan(0, original.Length).ToArray());
        Assert.Equal(original, sourceStream.ToArray());
        Assert.True(LongValueUsageMaps.TryRead(format, image.CopyBytes(), out List<LongValueUsageMap> oldMaps, out _));
        Assert.Equal(2, result.LongValueMaps.Count);
        Assert.Equal(oldMaps.Select(static map => map.ColumnNumber), result.LongValueMaps.Select(static map => map.ColumnNumber));
        Assert.Equal(empty ? 0 : 3, result.Data.RowCount);
        Assert.Equal(result.AllocationRecords * 12, values.Length);
        Assert.True(empty || result.LongValues.PageCount > 0);
        destination.Position = owner * format.PageSize;
        await destination.WriteAsync(result.PatchedDefinition, Ct);
        await destination.FlushAsync(Ct);
        byte[] staged = destination.ToArray();
        await using var reopenedStream = new MemoryStream(staged, writable: false);
        await using var reopenedRows = new MemoryStream(rows.ToArray(), writable: false);
        await using var reopenedAllocations = new MemoryStream(values.ToArray(), writable: false);
        CompactRawTableAllocationValidatorResult validated = await CompactRawTableAllocationValidator.ValidateAsync(session, owner, image, reopenedStream, reopenedRows, reopenedAllocations, result, new(), 1_000_000, Ct);
        Assert.False(validated.DatabaseValidated);
        Assert.Equal(result.AllocationRecords, validated.AllocationRecords);
        await using AccessReader reopened = await TestDatabase.OpenReaderAsync(reopenedStream, cancellationToken: Ct);
        IReadOnlyList<long> selected = await reopened.Database.OwnedPages.GetOwnedDataPagesAsync(owner, Ct);
        Assert.Equal(Enumerable.Range(0, checked((int)result.Data.PageCount)).Select(index => result.Data.FirstPageNumber + index), selected);
        Assert.Equal(selected, await MembersAsync(format, staged, result.OwnedPages));
        Assert.Equal(FreePages(format, staged, selected), await MembersAsync(format, staged, result.FreePages));
        await ValidateMapAsync(format, reopenedStream, result.OwnedPages, result.Data.PageCount == 0 ? [] : [new(result.Data.FirstPageNumber, result.Data.PageCount)]);
        await ValidateMapAsync(format, reopenedStream, result.FreePages, FreePages(format, staged, selected).Select(static page => new CompactPageRun(page, 1)));

        var allValuePages = new List<long>();
        foreach (CompactRawTableLongValueMaps column in result.LongValueMaps)
        {
            List<long> expected = AllocationPages(values.ToArray(), column.ColumnNumber);
            Assert.Equal(expected, await MembersAsync(format, staged, column.OwnedPages));
            Assert.Equal(FreePages(format, staged, expected), await MembersAsync(format, staged, column.FreePages));
            await ValidateMapAsync(format, reopenedStream, column.OwnedPages, AllocationRuns(values.ToArray(), column.ColumnNumber));
            await ValidateMapAsync(format, reopenedStream, column.FreePages, FreePages(format, staged, expected).Select(static page => new CompactPageRun(page, 1)));
            allValuePages.AddRange(expected);
        }

        Assert.Equal(Enumerable.Range(0, checked((int)result.LongValues.PageCount)).Select(index => result.LongValues.FirstPageNumber + index), allValuePages.Order());
        Assert.Equal(allValuePages.Count, allValuePages.Distinct().Count());
        int observed = 0;
        await foreach (object[] row in reopened.Rows("Items", cancellationToken: Ct))
        {
            Assert.Equal(observed, Assert.IsType<int>(row[0]));
            Assert.Equal(Text(observed), Assert.IsType<string>(row[1]));
            Assert.Equal(Binary(observed), Assert.IsType<byte[]>(row[2]));
            observed++;
        }

        Assert.Equal(empty ? 0 : 3, observed);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("allocation")]
    [InlineData("read")]
    [InlineData("output")]
    [InlineData("working")]
    public async Task Build_ExhaustedAggregateAllowanceReturnsNoCompletionAndPreservesSource(string limit)
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, false);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        CompactRawTableBuilderBudget budget = limit switch
        {
            "allocation" => new() { MaxAllocationLedgerBytes = 0 },
            "read" => new() { MaxReadBytes = 0 },
            "output" => new() { MaxOutputBytes = 1 },
            _ => new() { MaxWorkingBytes = 1 },
        };

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, destination, rows, values, [], budget, Ct));

        Assert.Equal(original, sourceStream.ToArray());
        Assert.Equal(original, destination.ToArray().AsSpan(0, original.Length).ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_AliasedLedgersRefuseBeforeStaging()
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.AceAccdb, false);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var ledger = new MemoryStream();

        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, destination, ledger, ledger, [], new(), Ct));

        Assert.Equal(original, destination.ToArray());
        Assert.Equal(0, ledger.Length);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Build_AggregateAppendLimitIncludesEveryMapAfterDataAndLongValues(bool pages)
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, false);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
        await using var complete = new MemoryStream();
        await complete.WriteAsync(original, Ct);
        await using var completeRows = new MemoryStream();
        await using var completeValues = new MemoryStream();
        CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, complete, completeRows, completeValues, [], new(), Ct);
        long limit = complete.Length - original.Length - source.Database.Format.PageSize;
        Assert.True(limit > (result.Data.PageCount + result.LongValues.PageCount) * source.Database.Format.PageSize);
        CompactRawTableBuilderBudget budget = pages ? new() { MaxPages = limit / source.Database.Format.PageSize } : new() { MaxOutputBytes = limit };
        await using var partial = new MemoryStream();
        await partial.WriteAsync(original, Ct);
        await using var partialRows = new MemoryStream();
        await using var partialValues = new MemoryStream();

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, partial, partialRows, partialValues, [], budget, Ct));

        Assert.Equal(original.Length + limit, partial.Length);
        Assert.Equal(original, partial.ToArray().AsSpan(0, original.Length).ToArray());
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Fact]
    public async Task Build_ManyMapRecordsRequireStorageAdmissionBeforeParsingOrStaging()
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        Assert.True(LongValueUsageMaps.TryRead(format, image.CopyBytes(), out List<LongValueUsageMap> maps, out _));
        int trailer = maps[0].OwnedOffset - 2;
        int end = trailer + (Constants.TableDefinition.MaxColumns * 10) + 10;
        byte[] bytes = new byte[end];
        image.Bytes[..trailer].CopyTo(bytes);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(8), end - 8);
        for (int column = 0; column < Constants.TableDefinition.MaxColumns; column++)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(trailer + (column * 10)), checked((ushort)column));
        }

        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(end - 10), ushort.MaxValue);
        TDefImage manyMaps = Assert.IsType<TDefImage>(TDefCodec.Parse(format, bytes));
        var session = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();

        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawTableBuilder.BuildAsync(session, format, owner, manyMaps, destination, rows, values, [], new() { MaxWorkingBytes = 300_000 }, Ct));

        Assert.Equal(original, destination.ToArray());
        Assert.Equal(0, rows.Length);
        Assert.Equal(0, values.Length);
        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Build_WorkingAllowanceIncludesRetainedMapResultsBeforeStaging(bool oneByteShort)
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        Assert.True(LongValueUsageMaps.TryRead(format, image.CopyBytes(), out List<LongValueUsageMap> maps, out _));
        long working = (2L * image.LogicalByteLength) + format.PageSize + 131_072
            + (256L * Math.Min(Constants.TableDefinition.MaxColumns, image.LogicalByteLength / 10)) + (256L * maps.Count);
        var session = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        var budget = new CompactRawTableBuilderBudget { MaxWorkingBytes = working - (oneByteShort ? 1 : 0) };
        if (oneByteShort)
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawTableBuilder.BuildAsync(session, format, owner, image, destination, rows, values, [], budget, Ct));
            Assert.Equal(original, destination.ToArray());
            Assert.Equal(0, rows.Length);
            Assert.Equal(0, values.Length);
        }
        else
        {
            CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, format, owner, image, destination, rows, values, [], budget, Ct);
            Assert.Equal(maps.Count, result.LongValueMaps.Count);
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Validate_WorkingAllowanceIncludesLedgerAndNestedRawComparison(bool oneByteShort)
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, true);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        JetFormat format = source.Database.Format;
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, format, owner, image, destination, rows, values, [], new(), Ct);
        long working = (3L * format.PageSize) + 65_536 + 4096;
        var budget = new CompactRawDataLongValueValidatorBudget { MaxWorkingBytes = working - (oneByteShort ? 1 : 0) };
        if (oneByteShort)
        {
            await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactRawTableAllocationValidator.ValidateAsync(session, owner, image, destination, rows, values, result, budget, 0, Ct));
        }
        else
        {
            CompactRawTableAllocationValidatorResult validated = await CompactRawTableAllocationValidator.ValidateAsync(session, owner, image, destination, rows, values, result, budget, 0, Ct);
            Assert.Equal(0, validated.AllocationRecords);
        }

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory]
    [InlineData("column")]
    [InlineData("swap")]
    [InlineData("range")]
    [InlineData("missing")]
    [InlineData("extra")]
    [InlineData("truncated")]
    public async Task Validate_ChangedColumnAllocationLedgerRefusesDespiteIntactRawRowsAndValues(string fault)
    {
        (byte[] original, long owner) = await CreateAsync(DatabaseFormat.Jet4Mdb, false);
        await using var sourceStream = new MemoryStream(original, writable: false);
        await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct);
        TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
        var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
        await using var destination = new MemoryStream();
        await destination.WriteAsync(original, Ct);
        await using var rows = new MemoryStream();
        await using var values = new MemoryStream();
        CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, destination, rows, values, [], new(), Ct);
        byte[] records = values.ToArray();
        Assert.True(result.AllocationRecords >= 2);
        switch (fault)
        {
            case "column":
                BinaryPrimitives.WriteInt32LittleEndian(records, 123);
                break;
            case "swap":
                int first = BinaryPrimitives.ReadInt32LittleEndian(records);
                int second = BinaryPrimitives.ReadInt32LittleEndian(records.AsSpan(12));
                Assert.NotEqual(first, second);
                BinaryPrimitives.WriteInt32LittleEndian(records, second);
                BinaryPrimitives.WriteInt32LittleEndian(records.AsSpan(12), first);
                break;
            case "range":
                BinaryPrimitives.WriteInt32LittleEndian(records.AsSpan(4), 3);
                break;
            case "missing":
                records = records[..^12];
                result = result with { AllocationRecords = result.AllocationRecords - 1 };
                break;
            case "extra":
                records = [.. records, .. records.AsSpan(0, 12)];
                result = result with { AllocationRecords = result.AllocationRecords + 1 };
                break;
            case "truncated":
                records = records[..^1];
                break;
            default:
                throw new InvalidOperationException();
        }

        await using var reopened = new MemoryStream(destination.ToArray(), writable: false);
        await using var reopenedRows = new MemoryStream(rows.ToArray(), writable: false);
        await using var changedLedger = new MemoryStream(records, writable: false);
        await CompactRawDataLongValueValidator.ValidateAsync(session, owner, owner, image, reopened, reopenedRows, result.Data, result.LongValues, new(), Ct);

        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactRawTableAllocationValidator.ValidateAsync(session, owner, image, reopened, reopenedRows, changedLedger, result, new(), 1_000_000, Ct));

        Assert.Equal(0, session.Accounting.ActiveWorkingBytes);
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Build_Dao120ReadsAndCompactsPinnedTableAssembly(DatabaseFormat kind) => VerifyNativeAsync(kind);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task Build_Dao36ReadsAndCompactsPinnedTableAssembly() => VerifyNativeAsync(DatabaseFormat.Jet3Mdb);

    private static async Task VerifyNativeAsync(DatabaseFormat kind)
    {
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactRawTable", databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        (byte[] original, long owner) = await CreateAsync(kind, false);
        string control = native.CreateDatabasePath("control");
        await File.WriteAllBytesAsync(control, original, Ct);
        await using (var sourceStream = new MemoryStream(original, writable: false))
        await using (AccessReader source = await TestDatabase.OpenReaderAsync(sourceStream, cancellationToken: Ct))
        await using (var destination = new MemoryStream())
        await using (var rows = new MemoryStream())
        await using (var values = new MemoryStream())
        {
            TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(owner, Ct));
            Assert.Single((await source.Database.TableDefs.ReadTDefChainAsync(owner, Ct)).PageNumbers);
            var session = new CompactDiscoverySession(source.Database.Pages, source.Database.Format, new());
            await destination.WriteAsync(original, Ct);
            CompactRawTableBuilderResult result = await CompactRawTableBuilder.BuildAsync(session, source.Database.Format, owner, image, destination, rows, values, [], new(), Ct);
            destination.Position = owner * source.Database.Format.PageSize;
            await destination.WriteAsync(result.PatchedDefinition, Ct);
            await File.WriteAllBytesAsync(native.SourcePath, destination.ToArray(), Ct);
        }

        // The original global allocation and storage stay pinned. This checks native read/compact only;
        // mutable native acceptance requires separate global allocation assembly.
        string sourcePath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compactedPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        string controlPath = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control);
        string script = $$"""
            function Assert-Contents([string]$path) {
                $db=$engine.OpenDatabase($path,$false,$true)
                try {
                    $rs=$db.OpenRecordset('SELECT * FROM Items ORDER BY Id',2)
                    try {
                        $count=0
                        while(!$rs.EOF) {
                            $textLength=if($count -eq 0){10}else{10000+$count}
                            $expected=(''+[char](97+$count))*$textLength
                            $binaryLength=if($count -eq 0){7}else{9000+$count}
                            $actualId=$rs.Fields.Item('Id').Value
                            $rawMemo=$rs.Fields.Item('Memo').Value
                            [string]$actualMemo=$rawMemo
                            if($actualId -ne $count -or $actualMemo -cne $expected) { throw "Native raw table value mismatch path=$path row=$count id=$actualId memoLength=$($actualMemo.Length) expectedLength=$textLength actualType=$($rawMemo.GetType().FullName) expectedType=$($expected.GetType().FullName) codes=$([string]::Join(',', [int[]][char[]]($actualMemo.Substring(0,[Math]::Min(8,$actualMemo.Length)))))" }
                            [byte[]]$bytes=$rs.Fields.Item('Binary').Value
                            if($bytes.Length -ne $binaryLength) { throw 'Native raw table binary length mismatch' }
                            foreach($value in $bytes) { if($value -ne $count+1) { throw 'Native raw table binary byte mismatch' } }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne 3) { throw 'Native raw table row count mismatch' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Assert-Contents {{controlPath}}
            Assert-Contents {{sourcePath}}
            $engine.CompactDatabase({{sourcePath}},{{compactedPath}})
            Assert-Contents {{compactedPath}}
            """;
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 table read/compact failed:\n{result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO table read/compact failed:\n{result.StdOut}\n{result.StdErr}");
        }
    }

    private static string Text(int id) => new((char)('a' + id), id == 0 ? 10 : 10_000 + id);

    private static byte[] Binary(int id) => Enumerable.Repeat(checked((byte)(id + 1)), id == 0 ? 7 : 9_000 + id).ToArray();

    private static async Task<(byte[] Bytes, long Owner)> CreateAsync(DatabaseFormat kind, bool empty)
    {
        await using var stream = new MemoryStream();
        long owner;
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("Items", [new("Id", typeof(int)), new("Memo", typeof(string), 20_000), new("Binary", typeof(byte[]))], Ct);
            if (!empty)
            {
                await writer.InsertRowsAsync("Items", Enumerable.Range(0, 3).Select(static id => new object?[] { id, Text(id), Binary(id) }), Ct);
            }

            CatalogEntry entry = Assert.IsType<CatalogEntry>(await writer.Services.Catalog.GetCatalogEntryAsync("Items", Ct));
            owner = entry.TDefPage;
        }

        return (stream.ToArray(), owner);
    }

    private static List<long> AllocationPages(byte[] bytes, int column)
        => AllocationRuns(bytes, column).SelectMany(static run => Enumerable.Range(checked((int)run.FirstPageNumber), checked((int)run.PageCount)).Select(static page => (long)page)).ToList();

    private static List<CompactPageRun> AllocationRuns(byte[] bytes, int column)
    {
        var result = new List<CompactPageRun>();
        for (int offset = 0; offset < bytes.Length; offset += 12)
        {
            if (BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)) == column)
            {
                int first = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + 4));
                int count = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset + 8));
                result.Add(new(first, count));
            }
        }

        return result;
    }

    private static async Task ValidateMapAsync(JetFormat format, Stream destination, CompactUsageMapBuilderResult allocation, IEnumerable<CompactPageRun> runs)
    {
        CompactUsageMapValidatorResult result = await CompactUsageMapValidator.ValidateAsync(format, RunsAsync(runs, Ct), destination, allocation, new(), Ct);
        Assert.False(result.DatabaseValidated);
        Assert.Equal(allocation.ListedPages, result.ListedPages);
    }

    private static async IAsyncEnumerable<CompactPageRun> RunsAsync(IEnumerable<CompactPageRun> runs, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (CompactPageRun run in runs)
        {
            token.ThrowIfCancellationRequested();
            yield return run;
        }
    }

    private static IEnumerable<long> FreePages(JetFormat format, byte[] bytes, IEnumerable<long> pages)
        => pages.Where(page => BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(checked((int)(page * format.PageSize)) + 2)) >= 3
            && BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(checked((int)(page * format.PageSize)) + format.DataPage.NumRows)) < Constants.DataPage.MaxRowsPerPage);

    private static async Task<List<long>> MembersAsync(JetFormat format, byte[] bytes, CompactUsageMapBuilderResult map)
    {
        byte[] page = bytes.AsSpan(checked((int)(map.Pointer.PageNumber * (long)format.PageSize)), format.PageSize).ToArray();
        Assert.True(UsageMap.TryGetRowBound(page, format.DataPage, format.PageSize, map.Pointer.RowIndex, out RowBound bound));
        var result = new List<long>();
        Assert.True(await UsageMap.TryEnumeratePagesAsync(
            page,
            bound,
            format.PageSize,
            bytes.Length / format.PageSize,
            1,
            true,
            (number, _) => new(bytes.AsSpan(checked((int)(number * format.PageSize)), format.PageSize).ToArray()),
            static _ => { },
            result,
            Ct));
        return result;
    }
}
