namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Real disk-backed private database assembly closes outputs before independently reopening every comparison.</summary>
public sealed class CompactPinnedDatabaseBuilderTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task CompleteDiscoveredPlan_BuildsAndIndependentlyReopens(DatabaseFormat kind)
        => WithFixtureAsync(kind, async fixture =>
        {
            CompactPinnedDatabaseBuilderResult result = await BuildAsync(fixture, new());
            Assert.Equal(fixture.Tables.Count, result.Validation.Tables);
            Assert.True(result.Validation.DestinationInventoryCompared);
            Assert.True(result.Validation.CatalogObjects >= fixture.Tables.Count);
            Assert.Equal(fixture.Source.Database.Pages.PageCount, result.Validation.Prefix.PinnedPages);
            Assert.True(result.Validation.Allocations.DestinationPages > result.Plan.SourcePages);
            Assert.True(result.ReadBytes > 0);
            Assert.True(result.WrittenBytes > fixture.Original.Length);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.ReclamationAuthorized);
            Assert.False(result.PublicationAuthorized);
            Assert.Contains((CompactPinnedDatabaseFile.Destination, 0L), fixture.Opened);
            Assert.Contains((CompactPinnedDatabaseFile.AllocationLedger, 0L), fixture.Opened);
            await using AccessReader reopened = await TestDatabase.OpenReaderAsync(fixture.PathFor(CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct);
            int count = 0;
            await foreach (object[] row in reopened.Rows("Items", cancellationToken: Ct))
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal(new string((char)('a' + id), 3000), row[1]);
                count++;
            }

            Assert.Equal(3, count);
        });

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task RecoveredCatalogOwnerAliasesUserTable_RowsStageOnceAndActiveInventoryCompletes(DatabaseFormat kind)
        => WithFixtureAsync(kind, async fixture =>
        {
            CatalogEntry user = Assert.IsType<CatalogEntry>(await fixture.Source.GetCatalogEntryAsync("Items", Ct));
            JetFormat format = fixture.Source.Database.Format;
            byte[] damaged = fixture.Original.ToArray();
            int catalogPage = Enumerable.Range(3, (damaged.Length / format.PageSize) - 3).First(page =>
                damaged[page * format.PageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadInt32LittleEndian(damaged.AsSpan((page * format.PageSize) + format.DataPage.TDefOff)) == 2);
            int ownerOffset = (catalogPage * format.PageSize) + format.DataPage.TDefOff;
            BinaryPrimitives.WriteInt32LittleEndian(damaged.AsSpan(ownerOffset), checked((int)user.TDefPage));
            byte[] unchanged = damaged.ToArray();
            await using var original = new MemoryStream(damaged, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, format, fixture.Tables, new(fixture.CreateAsync, fixture.OpenAsync), new(), Ct);
            Assert.Equal(fixture.Tables.Count, result.Validation.Tables);
            Assert.True(result.Validation.DestinationInventoryCompared);
            Assert.False(result.DatabaseValidated);
            Assert.Equal(unchanged, original.ToArray());
            await using AccessReader reopened = await TestDatabase.OpenReaderAsync(fixture.PathFor(CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct);
            Assert.Equal(await fixture.Source.ListTablesAsync(Ct), await reopened.ListTablesAsync(Ct));
            Assert.Equal(user.TDefPage, Assert.IsType<CatalogEntry>(await reopened.GetCatalogEntryAsync("Items", Ct)).TDefPage);
            int rows = 0;
            await foreach (object[] row in reopened.Rows("Items", cancellationToken: Ct))
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal(rows, id);
                Assert.Equal(new string((char)('a' + id), 3000), row[1]);
                rows++;
            }

            Assert.Equal(3, rows);
            TDefImage catalog = Assert.IsType<TDefImage>(await reopened.Database.TableDefs.ReadImageAsync(2, Ct));
            var reader = new CatalogRowReader(format, reopened.Database.TableDefs, reopened.Database.OwnedPages);
            List<CatalogRow> identities = await reader.GetCatalogRowsAsync(catalog.Definition, Ct);
            Assert.Single(identities, row => row.Name == "Items");
            Assert.Equal(identities.Count, identities.Select(static row => row.Id).Distinct().Count());
        });

    [Fact]
    public Task MissingTable_RefusesBeforeCreatingAnyOutput()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.Tables.RemoveAt(fixture.Tables.Count - 1);
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(fixture, new()));
            Assert.Empty(fixture.Created);
        });

    [Theory]
    [InlineData("read")]
    [InlineData("write")]
    [InlineData("pages")]
    [InlineData("working")]
    public Task WholeBuildAdmission_IsCumulative(string kind)
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            CompactPinnedDatabaseBuilderResult result = await BuildAsync(fixture, new());
            fixture.ClearOutputs();
            CompactPinnedDatabaseBuilderBudget budget = kind switch
            {
                "read" => new() { MaxReadBytes = result.ReadBytes - 1 },
                "write" => new() { MaxWriteBytes = result.WrittenBytes - 1 },
                "pages" => new() { MaxPages = result.Validation.Allocations.DestinationPages - 1 },
                _ => new() { MaxWorkingBytes = 8192 },
            };
            await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(fixture, budget));
        });

    [Fact]
    public Task ReopenedInventoryLimits_AreForwardedToValidation()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
            await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(fixture, new()
            {
                Validation = new() { Inventory = new() { MaxObjects = 1 } },
            })));

    [Fact]
    public Task ActiveCatalogLimits_AreForwardedToValidation()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
            await Assert.ThrowsAsync<JetLimitationException>(async () => await BuildAsync(fixture, new()
            {
                Validation = new() { ActiveCatalog = new() { MaxRows = 1 } },
            })));

    [Fact]
    public Task ActiveRowsWithoutObserver_RefuseBeforeCreatingOutput()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            long owner = Assert.IsType<CatalogEntry>(await fixture.Source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
            int slot = fixture.Tables.FindIndex(table => table.Owner == owner);
            fixture.Tables[slot] = fixture.Tables[slot] with { ActiveIndexOrdinal = 0 };
            await Assert.ThrowsAsync<ArgumentException>(async () => await BuildAsync(fixture, new()));
            Assert.Empty(fixture.Created);
        });

    [Fact]
    public Task ActiveTableLimits_AreForwardedToIndependentValidation()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            long owner = Assert.IsType<CatalogEntry>(await fixture.Source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
            int slot = fixture.Tables.FindIndex(table => table.Owner == owner);
            fixture.Tables[slot] = fixture.Tables[slot] with { ActiveIndexOrdinal = 0 };
            var workspace = new CompactPinnedDatabaseWorkspace(fixture.CreateAsync, fixture.OpenAsync, static (_, _) => ValueTask.CompletedTask);
            await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                fixture.Source.Database.Pages,
                fixture.Source.Database.Format,
                fixture.Tables,
                workspace,
                new() { Validation = new() { ActiveTables = new() { MaxActiveRows = 1 } } },
                Ct));
            Assert.Contains((CompactPinnedDatabaseFile.Destination, 0L), fixture.Opened);
        });

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task ActiveRows_RefuseSecondarySeparatorThatDoesNotDescribeItsRow(DatabaseFormat kind)
        => WithFixtureAsync(
            kind,
            async fixture =>
        {
            long owner = Assert.IsType<CatalogEntry>(await fixture.Source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
            int slot = fixture.Tables.FindIndex(table => table.Owner == owner);
            CompactPinnedDatabaseBuildTable table = fixture.Tables[slot];
            int primary = table.Definition.LogicalIndexes.Single(static index => index.Name == "PrimaryKey").Entry.IndexNum2;
            int secondary = table.Definition.LogicalIndexes.Single(static index => index.Name == "Secondary").Entry.IndexNum2;
            fixture.Tables[slot] = table with { ActiveIndexOrdinal = primary };
            TDefIndexDescriptor descriptor = table.Definition.RealIndexes[secondary];
            List<IndexEntry> leaves = await IndexLeafChain.ReadEntriesAsync(fixture.Source.Database, owner, descriptor.RootPage, Ct);
            Assert.Equal(3, leaves.Count);
            JetFormat format = fixture.Source.Database.Format;
            byte[] original = fixture.Original;
            byte[] damaged = new byte[original.Length + format.PageSize];
            original.CopyTo(damaged, 0);
            long leafPage = original.Length / format.PageSize;
            original.AsSpan(checked((int)descriptor.RootPage * format.PageSize), format.PageSize).CopyTo(damaged.AsSpan(original.Length));
            IndexEntry maximum = leaves[^1];
            IndexEntry wrong = new(maximum.Key, leaves[0].DataPage, leaves[0].DataRow);
            byte[] root = IndexPageCodec.BuildIntermediatePage(format.IndexPage, format.PageSize, checked((int)owner), [new(wrong, checked((int)leafPage))], 0, 0, 0);
            root.CopyTo(damaged, checked((int)descriptor.RootPage * format.PageSize));
            byte[] unchanged = damaged.ToArray();
            await using var sourceBytes = new MemoryStream(damaged, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(sourceBytes, cancellationToken: Ct);
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages,
                format,
                fixture.Tables,
                new(fixture.CreateAsync, fixture.OpenAsync, static (_, _) => ValueTask.CompletedTask),
                new(),
                Ct));
            Assert.DoesNotContain((CompactPinnedDatabaseFile.Rows, owner), fixture.Created);
            Assert.Equal(unchanged, sourceBytes.ToArray());
        },
            secondaryIndex: true);

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public Task ActiveRows_RetainAndReportUnassignedSameOwnerCopies(DatabaseFormat kind)
        => WithFixtureAsync(kind, async fixture =>
        {
            long owner = Assert.IsType<CatalogEntry>(await fixture.Source.GetCatalogEntryAsync("Items", Ct)).TDefPage;
            int slot = fixture.Tables.FindIndex(table => table.Owner == owner);
            fixture.Tables[slot] = fixture.Tables[slot] with { ActiveIndexOrdinal = 0 };
            JetFormat format = fixture.Source.Database.Format;
            byte[] extended = new byte[fixture.Original.Length + format.PageSize];
            fixture.Original.CopyTo(extended, 0);
            int originalPage = Enumerable.Range(3, (fixture.Original.Length / format.PageSize) - 3).First(page =>
                fixture.Original[page * format.PageSize] == Constants.PageTypes.Data
                && BinaryPrimitives.ReadInt32LittleEndian(fixture.Original.AsSpan((page * format.PageSize) + format.DataPage.TDefOff)) == owner);
            ReadOnlyMemory<byte> copy = fixture.Original.AsMemory(originalPage * format.PageSize, format.PageSize);
            copy.CopyTo(extended.AsMemory(fixture.Original.Length));
            int unassigned = BinaryPrimitives.ReadUInt16LittleEndian(copy.Span[format.DataPage.NumRows..]);
            Assert.True(unassigned > 0);
            var reported = new HashSet<RowLocation>();
            var workspace = new CompactPinnedDatabaseWorkspace(fixture.CreateAsync, fixture.OpenAsync, (record, token) =>
            {
                token.ThrowIfCancellationRequested();
                if (record.Kind == "unassigned-row")
                {
                    Assert.True(record.IsPinned);
                    Assert.Equal(owner, record.OwnerTDefPage);
                    RowLocation location = Assert.IsType<RowLocation>(record.Location);
                    Assert.Equal(fixture.Original.Length / format.PageSize, location.DataPageNumber);
                    Assert.False(record.RawBytes.IsEmpty);
                    reported.Add(location);
                }

                return ValueTask.CompletedTask;
            });
            await using var original = new MemoryStream(extended, writable: false);
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            CompactPinnedDatabaseBuilderResult result = await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                source.Database.Pages, format, fixture.Tables, workspace, new(), Ct);
            Assert.Equal(1, result.Validation.ActiveTables);
            Assert.Equal(unassigned, result.Validation.UnassignedRows);
            Assert.Equal(unassigned, reported.Count);
            Assert.False(result.DatabaseValidated);
            Assert.False(result.ReclamationAuthorized);
            Assert.Equal(extended, original.ToArray());
            byte[] staged = await File.ReadAllBytesAsync(fixture.PathFor(CompactPinnedDatabaseFile.Destination, 0), Ct);
            Assert.Equal(copy.ToArray(), staged.AsSpan(fixture.Original.Length, format.PageSize).ToArray());
            await using AccessReader reopened = await TestDatabase.OpenReaderAsync(fixture.PathFor(CompactPinnedDatabaseFile.Destination, 0), cancellationToken: Ct);
            int activeRows = 0;
            await foreach (object[] row in reopened.Rows("Items", cancellationToken: Ct))
            {
                Assert.Equal(activeRows, Assert.IsType<int>(row[0]));
                activeRows++;
            }

            Assert.Equal(3, activeRows);
        });

    [Fact]
    public Task ChangedClosedDestination_RefusesDuringIndependentReopen()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.BeforeOpen = (role, _) =>
            {
                if (role == CompactPinnedDatabaseFile.Destination)
                {
                    using var changed = new FileStream(fixture.PathFor(role, 0), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                    changed.Position = 16;
                    int value = changed.ReadByte();
                    changed.Position = 16;
                    changed.WriteByte((byte)(value ^ 1));
                }
            };
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await BuildAsync(fixture, new()));
        });

    [Fact]
    public Task FactoryFailure_ClosesEveryTransferredOutputWithoutOpeningValidation()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            fixture.BeforeCreate = (role, _) =>
            {
                if (role == CompactPinnedDatabaseFile.LongValues)
                {
                    throw new IOException("Injected sidecar creation failure.");
                }
            };
            await Assert.ThrowsAsync<IOException>(async () => await BuildAsync(fixture, new()));
            Assert.Empty(fixture.Opened);
            foreach ((CompactPinnedDatabaseFile role, long owner) in fixture.Created)
            {
                await using var check = new FileStream(fixture.PathFor(role, owner), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
        });

    [Fact]
    public Task CancellationAfterCreation_ClosesOutputAndReturnsNoResult()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            fixture.BeforeCreate = (role, _) =>
            {
                if (role == CompactPinnedDatabaseFile.Rows)
                {
                    cancellation.Cancel();
                }
            };
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await BuildAsync(fixture, new(), cancellation.Token));
            Assert.Empty(fixture.Opened);
            foreach ((CompactPinnedDatabaseFile role, long owner) in fixture.Created)
            {
                await using var check = new FileStream(fixture.PathFor(role, owner), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
        });

    [Fact]
    public Task AliasedTransferredSidecar_RefusesAndClosesTheOriginalOwner()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            Stream? rowOutput = null;
            var workspace = new CompactPinnedDatabaseWorkspace(
                async (role, owner, token) =>
                {
                    if (role == CompactPinnedDatabaseFile.LongValues)
                    {
                        return rowOutput!;
                    }

                    Stream stream = await fixture.CreateAsync(role, owner, token);
                    if (role == CompactPinnedDatabaseFile.Rows)
                    {
                        rowOutput = stream;
                    }

                    return stream;
                },
                fixture.OpenAsync);
            await Assert.ThrowsAsync<ArgumentException>(async () => await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                fixture.Source.Database.Pages, fixture.Source.Database.Format, fixture.Tables, workspace, new(), Ct));
            Assert.NotNull(rowOutput);
            Assert.False(rowOutput.CanRead);
            Assert.Empty(fixture.Opened);
        });

    [Fact]
    public Task CancelledIndependentReopen_ClosesTheTransferredReader()
        => WithFixtureAsync(DatabaseFormat.Jet4Mdb, async fixture =>
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            Stream? reader = null;
            var workspace = new CompactPinnedDatabaseWorkspace(
                fixture.CreateAsync,
                async (role, owner, token) =>
                {
                    Stream stream = await fixture.OpenAsync(role, owner, token);
                    if (role == CompactPinnedDatabaseFile.Destination)
                    {
                        reader = stream;
                        await cancellation.CancelAsync();
                    }

                    return stream;
                });
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactPinnedDatabaseBuilder.BuildAndValidateAsync(
                fixture.Source.Database.Pages, fixture.Source.Database.Format, fixture.Tables, workspace, new(), cancellation.Token));
            Assert.NotNull(reader);
            Assert.False(reader.CanRead);
            Assert.DoesNotContain((CompactPinnedDatabaseFile.AllocationLedger, 0L), fixture.Opened);
        });

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task ClosedAssembly_Dao120ReadsMutatesEnforcesAndCompacts(DatabaseFormat kind)
        => VerifyNativeAsync(kind);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task ClosedAssembly_Dao36ReadsMutatesEnforcesAndCompacts()
        => VerifyNativeAsync(DatabaseFormat.Jet3Mdb);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public Task FreshJet3Assembly_Dao36ReadsMutatesEnforcesAndCompacts()
        => WithFixtureAsync(DatabaseFormat.Jet3Mdb, fixture => VerifyNativeAsync(fixture, DatabaseFormat.Jet3Mdb));

    private static Task VerifyNativeAsync(DatabaseFormat kind)
        => WithFixtureAsync(kind, fixture => VerifyNativeAsync(fixture, kind), nativeJet3: true);

    private static async Task VerifyNativeAsync(Fixture fixture, DatabaseFormat kind)
    {
        await BuildAsync(fixture, new());
        await using var native = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.CompactBuilder", databaseExtension: kind == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string control = native.CreateDatabasePath("control");
        await File.WriteAllBytesAsync(control, fixture.Original, Ct);
        File.Copy(fixture.PathFor(CompactPinnedDatabaseFile.Destination, 0), native.SourcePath);
        string script = NativeScript(native, control);
        if (kind == DatabaseFormat.Jet3Mdb)
        {
            PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, native.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO3.6 complete private assembly failed:\n{result.StandardOutput}\n{result.StandardError}");
        }
        else
        {
            AccessRoundTripEnvironment.CompactResult result = native.RunDaoEngineScript(script, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO complete private assembly failed:\n{result.StdOut}\n{result.StdErr}");
        }
    }

    private static string NativeScript(AccessRoundTripSession native, string control)
    {
        string original = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(control);
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(native.CompactedPath);
        return $$"""
            function Check-Contents([string]$path, [bool]$mutated) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('Items',1)
                    try {
                        $rs.Index=$db.TableDefs.Item('Items').Indexes.Item(0).Name
                        $rs.MoveFirst()
                        $count=0
                        while(!$rs.EOF) {
                            $id=[int]$rs.Fields.Item('Id').Value
                            $expectedId=if($mutated -and $count -eq 2){3}else{$count}
                            if($id -ne $expectedId) { throw "Native private assembly identity mismatch: $id" }
                            $expected=if($mutated -and $id -eq 0){'updated'}elseif($id -eq 3){'d'*4000}else{(''+[char](97+$id))*3000}
                            if(![string]::Equals([string]$rs.Fields.Item('Memo').Value,$expected,[StringComparison]::Ordinal)) { throw 'Native private assembly Memo mismatch' }
                            $count++
                            $rs.MoveNext()
                        }
                        if($count -ne 3) { throw "Native private assembly row count mismatch: $count" }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            function Change-Contents([string]$path) {
                $db=$engine.OpenDatabase($path)
                try {
                    $rs=$db.OpenRecordset('Items',1)
                    try {
                        $rs.Index=$db.TableDefs.Item('Items').Indexes.Item(0).Name
                        $rs.Seek('=',0)
                        if($rs.NoMatch) { throw 'Native private assembly missing update target' }
                        $rs.Edit()
                        $rs.Fields.Item('Memo').Value='updated'
                        $rs.Update()
                        $rs.AddNew()
                        $rs.Fields.Item('Id').Value=3
                        $rs.Fields.Item('Memo').Value='d'*4000
                        $rs.Update()
                        $rs.Seek('=',2)
                        if($rs.NoMatch) { throw 'Native private assembly missing deletion target' }
                        $rs.Delete()
                        $rejected=$false
                        try {
                            $rs.AddNew()
                            $rs.Fields.Item('Id').Value=0
                            $rs.Fields.Item('Memo').Value='duplicate'
                            $rs.Update()
                        } catch {
                            if(@($engine.Errors | ForEach-Object { $_.Number }) -notcontains 3022) { throw }
                            $rejected=$true
                            $rs.CancelUpdate()
                        }
                        if(!$rejected) { throw 'Native private assembly accepted duplicate primary key' }
                    } finally { $rs.Close() }
                } finally { $db.Close() }
            }
            Check-Contents {{original}} $false
            Check-Contents {{source}} $false
            Change-Contents {{original}}
            Check-Contents {{original}} $true
            Change-Contents {{source}}
            Check-Contents {{source}} $true
            $engine.CompactDatabase({{source}},{{compacted}})
            Check-Contents {{compacted}} $true
            Write-Output 'PINNED_DATABASE_NATIVE_OK'
            """;
    }

    private static ValueTask<CompactPinnedDatabaseBuilderResult> BuildAsync(Fixture fixture, CompactPinnedDatabaseBuilderBudget budget, CancellationToken? token = null)
        => CompactPinnedDatabaseBuilder.BuildAndValidateAsync(fixture.Source.Database.Pages, fixture.Source.Database.Format, fixture.Tables, new(fixture.CreateAsync, fixture.OpenAsync), budget, token ?? Ct);

    private static async Task WithFixtureAsync(DatabaseFormat kind, Func<Fixture, Task> action, bool nativeJet3 = false, bool secondaryIndex = false)
    {
        string directory = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter.CompactBuilder." + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            await using var original = new MemoryStream();
            if (nativeJet3 && kind == DatabaseFormat.Jet3Mdb)
            {
                await original.WriteAsync(await File.ReadAllBytesAsync(Path.Combine(AppContext.BaseDirectory, "Databases", "Jet3", "NativeDescriptorMetadata.mdb"), Ct), Ct);
            }

            await using (AccessWriter writer = nativeJet3 && kind == DatabaseFormat.Jet3Mdb
                ? await AccessWriter.OpenAsync(original, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct)
                : await AccessWriter.CreateDatabaseAsync(original, kind, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
            {
                if (secondaryIndex)
                {
                    await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000), new("Other", typeof(int))], [new IndexDefinition("Secondary", "Other")], Ct);
                    await writer.InsertRowsAsync("Items", Enumerable.Range(0, 3).Select(id => new object?[] { id, new string((char)('a' + id), 3000), id * 10 }), Ct);
                }
                else
                {
                    await writer.CreateTableAsync("Items", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Memo", typeof(string), 20_000)], Ct);
                    await writer.InsertRowsAsync("Items", Enumerable.Range(0, 3).Select(id => new object?[] { id, new string((char)('a' + id), 3000) }), Ct);
                }
            }

            byte[] originalBytes = original.ToArray();
            await using AccessReader source = await TestDatabase.OpenReaderAsync(original, cancellationToken: Ct);
            TDefImage catalog = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(2, Ct));
            var reader = new CatalogRowReader(source.Database.Format, source.Database.TableDefs, source.Database.OwnedPages);
            List<CatalogRow> entries = await reader.GetCatalogRowsAsync(catalog.Definition, Ct);
            var tables = new List<CompactPinnedDatabaseBuildTable>();
            foreach (CatalogRow entry in entries.Where(static entry => entry.ObjectType == Constants.SystemObjects.UserTableType).OrderBy(static entry => entry.TDefPage))
            {
                TDefImage image = Assert.IsType<TDefImage>(await source.Database.TableDefs.ReadImageAsync(entry.TDefPage, Ct));
                var indexes = new CompactPinnedTableIndexSource[image.RealIndexes.Count];
                for (int slot = 0; slot < indexes.Length; slot++)
                {
                    TDefIndexDescriptor descriptor = image.RealIndexes[slot];
                    Assert.True(UsageMap.TryReadPointer(image.CopyBytes(), descriptor.Slot.FirstDpOffset - 4, out UsageMapPointer map));
                    List<IndexEntry> stored = await IndexLeafChain.ReadEntriesAsync(source.Database, entry.TDefPage, descriptor.RootPage, Ct);
                    indexes[slot] = new(descriptor.RootPage, map, EntriesAsync(stored, Ct));
                }

                tables.Add(new(entry.TDefPage, image, indexes));
            }

            try
            {
                await action(new(directory, source, tables, originalBytes));
            }
            finally
            {
                Assert.Equal(originalBytes, original.ToArray());
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async IAsyncEnumerable<IndexEntry> EntriesAsync(IEnumerable<IndexEntry> entries, [EnumeratorCancellation] CancellationToken token)
    {
        await Task.CompletedTask;
        foreach (IndexEntry entry in entries)
        {
            token.ThrowIfCancellationRequested();
            yield return entry;
        }
    }

    private sealed class Fixture(string directory, AccessReader source, List<CompactPinnedDatabaseBuildTable> tables, byte[] original)
    {
        internal AccessReader Source { get; } = source;

        internal List<CompactPinnedDatabaseBuildTable> Tables { get; } = tables;

        internal byte[] Original { get; } = original;

        internal HashSet<(CompactPinnedDatabaseFile Role, long Owner)> Created { get; } = [];

        internal HashSet<(CompactPinnedDatabaseFile Role, long Owner)> Opened { get; } = [];

        internal Action<CompactPinnedDatabaseFile, long>? BeforeCreate { get; set; }

        internal Action<CompactPinnedDatabaseFile, long>? BeforeOpen { get; set; }

        internal string PathFor(CompactPinnedDatabaseFile role, long owner) => Path.Combine(directory, $"{role}-{owner}.bin");

        internal ValueTask<Stream> CreateAsync(CompactPinnedDatabaseFile role, long owner, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            this.BeforeCreate?.Invoke(role, owner);
            this.Created.Add((role, owner));
#pragma warning disable CA2000 // This factory transfers stream ownership to the coordinator, which closes it on every exit.
            return ValueTask.FromResult<Stream>(new FileStream(this.PathFor(role, owner), FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None));
#pragma warning restore CA2000
        }

        internal ValueTask<Stream> OpenAsync(CompactPinnedDatabaseFile role, long owner, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            this.BeforeOpen?.Invoke(role, owner);
            this.Opened.Add((role, owner));
#pragma warning disable CA2000 // This factory transfers stream ownership to the coordinator, which closes it on every exit.
            return ValueTask.FromResult<Stream>(new FileStream(this.PathFor(role, owner), FileMode.Open, FileAccess.Read, FileShare.Read));
#pragma warning restore CA2000
        }

        internal void ClearOutputs()
        {
            foreach ((CompactPinnedDatabaseFile role, long owner) in this.Created)
            {
                File.Delete(this.PathFor(role, owner));
            }

            this.Created.Clear();
            this.Opened.Clear();
        }
    }
}
