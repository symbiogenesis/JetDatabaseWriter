namespace JetDatabaseWriter.Tests.Architecture;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Transactions;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>
/// Guards how both facades are composed. Each facade owns a
/// <see cref="DatabaseFile"/> and builds its collaborators once in a
/// composition root (<see cref="ReaderServices"/> / <see cref="WriterServices"/>)
/// that hands every collaborator the database file plus the specific siblings
/// it uses, and the writer's <see cref="Pager"/> only to the writer services
/// that write pages. No collaborator may hold or receive a facade,
/// <see cref="AccessBase"/>, or a composition root; each collaborator graph
/// must be acyclic; the facade must not be reachable from its services at
/// runtime; the reader's graph neither holds nor names anything that writes;
/// each facade sets up its page reads by how it was opened; and the facades
/// expose no internal members.
/// </summary>
public sealed class ServiceGraphTests
{
    private const BindingFlags DeclaredMembers =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static readonly Assembly Library = typeof(AccessReader).Assembly;

    private static readonly Type[] Facades = [typeof(AccessBase), typeof(AccessReader), typeof(AccessWriter)];

    /// <summary>
    /// Collaborators that open a separate <see cref="AccessReader"/> of their own:
    /// the readers of an Access-file linked table's source database. None of
    /// them receives or holds the reader that owns it. The writer reads its own
    /// file through its own <see cref="DatabaseFile"/>, never through a reader.
    /// </summary>
    private static readonly Type[] OpensSeparateReader = [typeof(LinkedTableReader), typeof(LinkedTableManager)];

    /// <summary>
    /// The types a graph writes through: the writer's <see cref="Pager"/> and
    /// its journal lease, the transaction journal, the byte-range locks and the
    /// in-place TDEF write-backs. Only the writer's graph may hold or name them.
    /// </summary>
    private static readonly Type[] WriteCapabilities =
        [typeof(Pager), typeof(Pager.JournalGate), typeof(PageJournal), typeof(JetByteRangeLock), typeof(TDefWriter)];

    public static TheoryData<Type> CompositionRoots => [typeof(ReaderServices), typeof(WriterServices)];

    [Fact]
    public void Collaborators_DoNotReferenceFacadesOrCompositionRoots()
    {
        Type[] forbidden = [.. Facades, typeof(ReaderServices), typeof(WriterServices)];
        var violations = new List<string>();

        foreach (Type type in Library.GetTypes())
        {
            // The facades themselves (and their compiler-generated state machines
            // and closures) are the only legitimate holders of these types.
            if (Facades.Any(facade => IsNestedWithin(type, facade)))
            {
                continue;
            }

            Type[] allowed = OpensSeparateReader.Any(opener => IsNestedWithin(type, opener))
                ? [typeof(AccessReader)]
                : [];

            foreach (FieldInfo field in type.GetFields(DeclaredMembers))
            {
                foreach (Type mentioned in forbidden.Where(f => Mentions(field.FieldType, f) && !allowed.Contains(f)))
                {
                    violations.Add($"{type.FullName}.{field.Name} : {mentioned.Name}");
                }
            }

            IEnumerable<MethodBase> methods = type.GetConstructors(DeclaredMembers).Cast<MethodBase>()
                .Concat(type.GetMethods(DeclaredMembers));
            foreach (MethodBase method in methods)
            {
                foreach (ParameterInfo parameter in method.GetParameters())
                {
                    foreach (Type mentioned in forbidden.Where(f => Mentions(parameter.ParameterType, f) && !allowed.Contains(f)))
                    {
                        violations.Add($"{type.FullName}.{method.Name}({parameter.Name}) : {mentioned.Name}");
                    }
                }
            }
        }

        Assert.Empty(violations);
    }

    /// <summary>
    /// A ratchet on the composite: only the types listed here may still take
    /// the whole <see cref="DatabaseFile"/> through a constructor or a method
    /// parameter, static helpers and delegates included, or hold it in a
    /// field, and the list may only shrink. New code takes the part it needs
    /// instead: the <see cref="JetFormat"/> profile, an <see cref="IPageSource"/>,
    /// the <see cref="TableDefReader"/> or <see cref="OwnedDataPages"/>.
    /// A type narrowed to its parts is removed from the list in the same
    /// commit. The facades, the composition roots and the composite itself are
    /// exempt.
    /// </summary>
    [Fact]
    public void Collaborators_TakingDatabaseFile_OnlyShrink()
    {
        string[] allowed =
        [
            "JetDatabaseWriter.Catalog.CatalogArtifactWriter",
            "JetDatabaseWriter.Catalog.CatalogReader",
            "JetDatabaseWriter.Catalog.CatalogRowReader",
            "JetDatabaseWriter.Catalog.CatalogWriter",
            "JetDatabaseWriter.Catalog.ColumnPropertyReader",
            "JetDatabaseWriter.Catalog.TableCatalog",
            "JetDatabaseWriter.ComplexColumns.ComplexColumnManager",
            "JetDatabaseWriter.ComplexColumns.ComplexColumnReader",
            "JetDatabaseWriter.ComplexColumns.ComplexReferenceSeedReader",
            "JetDatabaseWriter.Indexes.IndexBTreeEditor",
            "JetDatabaseWriter.Indexes.IndexCatalogReader",
            "JetDatabaseWriter.Indexes.IndexMaintainer",
            "JetDatabaseWriter.Indexes.UniqueIndexChecker",
            "JetDatabaseWriter.Pages.DataPageInserter",
            "JetDatabaseWriter.Pages.PageAllocator",
            "JetDatabaseWriter.Pages.ReaderPageCache",
            "JetDatabaseWriter.Relationships.LinkedTableManager",
            "JetDatabaseWriter.Relationships.RelationshipCatalogStore",
            "JetDatabaseWriter.Relationships.RelationshipChildRowLocator",
            "JetDatabaseWriter.Relationships.RelationshipEnforcer",
            "JetDatabaseWriter.Relationships.RelationshipManager",
            "JetDatabaseWriter.Relationships.RelationshipPageReader",
            "JetDatabaseWriter.Relationships.RelationshipSeekPlanner",
            "JetDatabaseWriter.Schema.AccessObjectName",
            "JetDatabaseWriter.Schema.AutoNumberMaintainer",
            "JetDatabaseWriter.Schema.TDefPageBuilder",
            "JetDatabaseWriter.Tables.IndexRowReader",
            "JetDatabaseWriter.Tables.SchemaReader",
            "JetDatabaseWriter.Tables.TableDataWriter",
            "JetDatabaseWriter.Tables.TableReader",
            "JetDatabaseWriter.Tables.TableRowStore",
            "JetDatabaseWriter.Tables.TableSchemaEditor",
            "JetDatabaseWriter.Tables.TableSnapshotReader",
            "JetDatabaseWriter.Transactions.TransactionLifecycle",
            "JetDatabaseWriter.ValueDecoding.DirectRowDecoder`1",
            "JetDatabaseWriter.ValueDecoding.LongValueDecoder",
            "JetDatabaseWriter.ValueDecoding.PartialColumnReader",
            "JetDatabaseWriter.ValueDecoding.RowDecodePlan",
            "JetDatabaseWriter.ValueDecoding.RowDecoder",
            "JetDatabaseWriter.ValueEncoding.LongValueEncoder",
            "JetDatabaseWriter.ValueEncoding.RowEncoder",
        ];

        Type[] exempt = [.. Facades, typeof(ReaderServices), typeof(WriterServices), typeof(DatabaseFile)];
        string[] actual =
        [
            .. Library.GetTypes()
                .Where(type => !exempt.Contains(type) && !IsCompilerGenerated(type) && TakesOrHoldsDatabaseFile(type))
                .Select(type => type.FullName!)
                .Order(StringComparer.Ordinal),
        ];

        string[] added = [.. actual.Except(allowed, StringComparer.Ordinal)];
        string[] narrowed = [.. allowed.Except(actual, StringComparer.Ordinal)];
        Assert.True(
            added.Length == 0,
            $"These types newly take or hold DatabaseFile; take JetFormat, IPageSource, TableDefReader or OwnedDataPages instead: {string.Join(", ", added)}");
        Assert.True(
            narrowed.Length == 0,
            $"These types no longer take DatabaseFile; remove them from the allow-list: {string.Join(", ", narrowed)}");
    }

    [Theory]
    [MemberData(nameof(CompositionRoots))]
    public void CollaboratorGraph_IsAcyclic(Type compositionRoot)
    {
        ArgumentNullException.ThrowIfNull(compositionRoot);

        var state = new Dictionary<Type, bool>();
        string? cycle = FindCycle(compositionRoot, state, new Stack<Type>());

        Assert.Null(cycle);
        Assert.DoesNotContain(state.Keys, type => Facades.Contains(type));
    }

    [Fact]
    public void ReaderCompositionRoot_ReachesEveryReaderService()
    {
        var state = new Dictionary<Type, bool>();
        _ = FindCycle(typeof(ReaderServices), state, new Stack<Type>());

        Type[] expected =
        [
            typeof(DatabaseFile),
            typeof(ReaderPageCache),
            typeof(LongValueDecoder),
            typeof(RowDecoder),
            typeof(TableCatalog),
            typeof(CatalogRowReader),
            typeof(CatalogReader),
            typeof(ColumnPropertyReader),
            typeof(ComplexColumnReader),
            typeof(ComplexItemReader),
            typeof(LinkedTableReader),
            typeof(TableReader),
            typeof(IndexRowReader),
            typeof(SchemaReader),
        ];

        foreach (Type service in expected)
        {
            Assert.Contains(service, state.Keys);
        }
    }

    /// <summary>
    /// The reader's graph cannot write: no type it holds or is built from is a
    /// write capability, and none declares an instance field, property, method
    /// or constructor whose signature names one, so nothing it reaches, the
    /// <see cref="DatabaseFile"/> it shares with the writer included, hands
    /// out a <see cref="Pager"/>. Static members are not scanned: the writer's
    /// composition root takes its pager from the static
    /// <see cref="DatabaseFile.ForWriter"/>, which opens a file of its own.
    /// </summary>
    [Fact]
    public void ReaderCompositionRoot_ReachesNoWriteCapability()
    {
        var state = new Dictionary<Type, bool>();
        _ = FindCycle(typeof(ReaderServices), state, new Stack<Type>());

        string[] violations =
        [
            .. state.Keys.Where(type => WriteCapabilities.Contains(type)).Select(type => type.FullName!),
            .. state.Keys.SelectMany(type => InstanceSignatures(type)
                .Where(signature => WriteCapabilities.Any(capability => Mentions(signature.Type, capability)))
                .Select(signature => $"{type.FullName}.{signature.Member} : {signature.Type.Name}")),
        ];

        Assert.Contains(typeof(DatabaseFile), state.Keys);
        Assert.Empty(violations);
    }

    [Fact]
    public void WriterCompositionRoot_ReachesEveryWriterWorkflow()
    {
        var state = new Dictionary<Type, bool>();
        _ = FindCycle(typeof(WriterServices), state, new Stack<Type>());

        Type[] expected =
        [
            typeof(DatabaseFile),
            typeof(Pager),
            typeof(TDefWriter),
            typeof(TableCatalog),
            typeof(TableDataWriter),
            typeof(TableSchemaEditor),
            typeof(TableRowStore),
            typeof(RelationshipManager),
            typeof(RelationshipEnforcer),
            typeof(ComplexColumnManager),
            typeof(CatalogWriter),
            typeof(CatalogArtifactWriter),
            typeof(CatalogOwnedMapPolicy),
            typeof(IndexMaintainer),
            typeof(TransactionLifecycle),
            typeof(TableSnapshotReader),
            typeof(RowDecoder),
            typeof(CatalogReader),
            typeof(ColumnPropertyReader),
        ];

        foreach (Type service in expected)
        {
            Assert.Contains(service, state.Keys);
        }
    }

    [Fact]
    public async Task OpenWriter_SnapshotReadsGoThroughTheWritersDatabaseFile()
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        HashSet<object> reachable = ReachableLibraryObjects(FacadeServices(writer));
        object database = FacadeDatabase(writer);

        // Every database file and page cache the writer's services hold is the
        // writer's own: no second file is opened to read rows back, and no
        // page cache keeps pages between calls.
        Assert.Single(reachable, item => item is DatabaseFile);
        Assert.Contains(database, reachable);
        Assert.All(reachable.OfType<ReaderPageCache>(), cache => Assert.Null(ReadField(cache, "pageCache")));
        Assert.DoesNotContain(reachable, item => item is AccessReader);
    }

    /// <summary>
    /// The writer's services write through one pager, the one its database
    /// file reads through, so a write is seen by the next read and a
    /// transaction's journal covers every write.
    /// </summary>
    [Fact]
    public async Task OpenWriter_PageWritesGoThroughTheWritersOnePager()
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync("Items", [2], TestContext.Current.CancellationToken);

        HashSet<object> reachable = ReachableLibraryObjects(FacadeServices(writer));

        Pager pager = Assert.IsType<Pager>(Assert.Single(reachable, item => item is Pager));
        Assert.Same(FacadeDatabase(writer).Pages, pager);
        _ = Assert.Single(reachable, item => item is TDefWriter);
    }

    /// <summary>
    /// One owned-map policy decides for the whole writer: the data-page
    /// inserter asks it, the catalog artifact writer registers the maps it
    /// creates with it, and the transaction lifecycle captures and restores
    /// it, so a rollback forgets what the transaction taught it.
    /// </summary>
    [Fact]
    public async Task OpenWriter_ServicesShareOneOwnedMapPolicy()
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        HashSet<object> reachable = ReachableLibraryObjects(FacadeServices(writer));

        _ = Assert.Single(reachable, item => item is IOwnedMapPolicy);
    }

    [Fact]
    public async Task OpenReader_FacadeIsNotReachableFromItsServices()
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        // Warm the caches so lazily built state is part of the walk.
        _ = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        _ = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);

        HashSet<object> reachable = ReachableLibraryObjects(FacadeServices(reader));

        Assert.DoesNotContain(reader, reachable);
        Assert.Contains(FacadeDatabase(reader), reachable);

        // The reader's graph cannot write: no pager, journal, byte-range lock
        // or TDEF writer.
        Assert.DoesNotContain(reachable, item => WriteCapabilities.Contains(item.GetType()));
        Assert.IsNotType<Pager>(FacadeDatabase(reader).Pages);
    }

    [Fact]
    public async Task OpenWriter_FacadeIsNotReachableFromItsServices()
    {
        await using MemoryStream stream = await CreateDatabaseAsync();
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);

        await writer.InsertRowAsync("Items", [2], TestContext.Current.CancellationToken);

        HashSet<object> reachable = ReachableLibraryObjects(FacadeServices(writer));

        Assert.DoesNotContain(writer, reachable);
        Assert.Contains(FacadeDatabase(writer), reachable);
    }

    /// <summary>
    /// How each facade sets up its page reads at open, read from its own
    /// <see cref="DatabaseFile"/>. A reader opened by path reads positionally
    /// unless <see cref="PageReadOptimizationMode.Disabled"/> (never in the
    /// netstandard2.1 build) and reads inline on pool threads, because its
    /// handle is synchronous. A reader over a caller's stream does neither,
    /// even in <see cref="PageReadOptimizationMode.Enabled"/>: the caller may
    /// still use the stream's position, and its handle may be overlapped. A
    /// writer never reads inline, before a write, after one or inside a
    /// transaction, because its handle is overlapped. <see cref="ReaderHarness"/>,
    /// which other tests open in the reader's place, sets up a path and a
    /// stream the same way.
    /// </summary>
    /// <param name="mode">The readers' page-read optimization mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Auto)]
    [InlineData(PageReadOptimizationMode.Enabled)]
    [InlineData(PageReadOptimizationMode.Disabled)]
    public async Task OpenFacades_PageReadSetup_FollowsHowEachWasOpened(PageReadOptimizationMode mode)
    {
        string path = Path.Combine(Path.GetTempPath(), $"ServiceGraphPageReads_{Guid.NewGuid():N}.accdb");
        try
        {
            await using (MemoryStream created = await CreateDatabaseAsync())
            {
                await File.WriteAllBytesAsync(path, created.ToArray(), TestContext.Current.CancellationToken);
            }

            var readerOptions = new AccessReaderOptions { PageReadOptimizationMode = mode, UseLockFile = false };
            bool positional = mode != PageReadOptimizationMode.Disabled && !LibraryTarget.IsNetStandard;
            await using (AccessReader reader = await AccessReader.OpenAsync(path, readerOptions, TestContext.Current.CancellationToken))
            await using (ReaderHarness harness = await ReaderHarness.OpenAsync(path, readerOptions, TestContext.Current.CancellationToken))
            {
                DatabaseFile db = FacadeDatabase(reader);
                Assert.Equal(positional, db.UsesRandomAccessPageReads);
                Assert.True(db.ReadsInlineOnThreadPool, $"{mode}: a path-opened reader reads inline on pool threads.");
                Assert.Equal(positional, harness.Database.UsesRandomAccessPageReads);
                Assert.True(harness.Database.ReadsInlineOnThreadPool, $"{mode}: ReaderHarness opens a path as the reader does.");
            }

            await using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, FileOptions.Asynchronous | FileOptions.RandomAccess))
            {
                await using (AccessReader reader = await AccessReader.OpenAsync(stream, readerOptions, leaveOpen: true, TestContext.Current.CancellationToken))
                {
                    DatabaseFile db = FacadeDatabase(reader);
                    Assert.False(db.UsesRandomAccessPageReads, $"{mode}: a reader over a caller's stream reads positionally.");
                    Assert.False(db.ReadsInlineOnThreadPool, $"{mode}: a reader over a caller's stream reads inline.");
                }

                await using ReaderHarness harness = await ReaderHarness.OpenAsync(stream, readerOptions, leaveOpen: true, TestContext.Current.CancellationToken);
                Assert.False(harness.Database.UsesRandomAccessPageReads);
                Assert.False(harness.Database.ReadsInlineOnThreadPool);
            }

            foreach (bool transactional in (bool[])[false, true])
            {
                await using AccessWriter writer = await AccessWriter.OpenAsync(
                    path,
                    new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = transactional },
                    TestContext.Current.CancellationToken);
                DatabaseFile db = FacadeDatabase(writer);
                Assert.False(db.ReadsInlineOnThreadPool);
                await writer.InsertRowAsync("Items", [transactional ? 3 : 2], TestContext.Current.CancellationToken);
                Assert.False(db.ReadsInlineOnThreadPool);

                await using JetTransaction transaction = await writer.BeginTransactionAsync(TestContext.Current.CancellationToken);
                Assert.False(db.ReadsInlineOnThreadPool);
                await transaction.RollbackAsync(TestContext.Current.CancellationToken);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>
    /// The facades carry no members for tests to reach into, and none for the
    /// writer either: the writer reads its own file through its own
    /// <see cref="DatabaseFile"/> instead of a reader opened over it.
    /// </summary>
    /// <param name="facade">The facade type.</param>
    [Theory]
    [InlineData(typeof(AccessBase))]
    [InlineData(typeof(AccessReader))]
    [InlineData(typeof(AccessWriter))]
    public void Facade_ExposesNoInternalMembers(Type facade)
    {
        ArgumentNullException.ThrowIfNull(facade);

        IEnumerable<string> fields = facade.GetFields(DeclaredMembers)
            .Where(field => field.IsAssembly || field.IsFamilyOrAssembly)
            .Select(field => field.Name);
        IEnumerable<string> methods = facade.GetMethods(DeclaredMembers).Cast<MethodBase>()
            .Concat(facade.GetConstructors(DeclaredMembers))
            .Where(method => (method.IsAssembly || method.IsFamilyOrAssembly) && !method.IsDefined(typeof(CompilerGeneratedAttribute)))
            .Select(method => method.IsSpecialName && method.Name.Length > 4 && method.Name[3] == '_' ? method.Name[4..] : method.Name);
        IEnumerable<string> nestedTypes = facade.GetNestedTypes(DeclaredMembers)
            .Where(nested => (nested.IsNestedAssembly || nested.IsNestedFamORAssem) && !nested.IsDefined(typeof(CompilerGeneratedAttribute)))
            .Select(nested => nested.Name);

        string[] actual = [.. fields.Concat(methods).Concat(nestedTypes).Distinct(StringComparer.Ordinal).Order(StringComparer.Ordinal)];

        Assert.Empty(actual);
    }

    /// <summary>
    /// Returns the service graph a reader or writer built at open, from the
    /// facade's private <c>services</c> field. This class checks how the
    /// facades are composed, so it is the one place tests reflect into them;
    /// every other test opens <see cref="ReaderHarness"/> or <see cref="WriterHarness"/>.
    /// </summary>
    /// <param name="facade">The reader or writer.</param>
    /// <exception cref="MissingFieldException">Thrown when the facade no longer declares the field, or it is null.</exception>
    private static object FacadeServices(AccessBase facade) => ReadField(facade, "services")
        ?? throw new MissingFieldException(facade.GetType().FullName, "services");

    /// <summary>Returns the database file a reader or writer owns, from <see cref="AccessBase"/>'s private <c>Database</c> property.</summary>
    /// <param name="facade">The reader or writer.</param>
    /// <exception cref="MissingMemberException">Thrown when <see cref="AccessBase"/> no longer declares the property.</exception>
    private static DatabaseFile FacadeDatabase(AccessBase facade)
    {
        PropertyInfo property = typeof(AccessBase).GetProperty("Database", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMemberException(typeof(AccessBase).FullName, "Database");
        return (DatabaseFile)property.GetValue(facade)!;
    }

    /// <summary>Reads a private instance field declared on <paramref name="instance"/>'s type or a base type.</summary>
    /// <param name="instance">The object to read from.</param>
    /// <param name="fieldName">The field name.</param>
    /// <exception cref="MissingFieldException">Thrown when no type in <paramref name="instance"/>'s hierarchy declares the field.</exception>
    private static object? ReadField(object instance, string fieldName)
    {
        for (Type? type = instance.GetType(); type is not null; type = type.BaseType)
        {
            FieldInfo? field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            if (field is not null)
            {
                return field.GetValue(instance);
            }
        }

        throw new MissingFieldException(instance.GetType().FullName, fieldName);
    }

    private static async ValueTask<MemoryStream> CreateDatabaseAsync()
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream,
            DatabaseFormat.AceAccdb,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Items", [1], TestContext.Current.CancellationToken);
        }

        stream.Position = 0;
        return stream;
    }

    /// <summary>
    /// Walks every object reachable from <paramref name="root"/> through the
    /// instance fields of library types and the targets of delegates. Framework
    /// collections are not entered: no library type keeps a facade in one.
    /// </summary>
    /// <param name="root">The object to start from.</param>
    private static HashSet<object> ReachableLibraryObjects(object root)
    {
        var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var pending = new Stack<object>();
        pending.Push(root);
        while (pending.Count > 0)
        {
            object current = pending.Pop();
            if (!visited.Add(current))
            {
                continue;
            }

            if (current is Delegate callback)
            {
                foreach (Delegate single in callback.GetInvocationList())
                {
                    if (single.Target is not null)
                    {
                        pending.Push(single.Target);
                    }
                }

                continue;
            }

            for (Type? type = current.GetType(); type is not null && type.Assembly == Library; type = type.BaseType)
            {
                foreach (FieldInfo field in type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.DeclaredOnly))
                {
                    if (!field.FieldType.IsValueType && field.GetValue(current) is { } value)
                    {
                        pending.Push(value);
                    }
                }
            }
        }

        return visited;
    }

    /// <summary>
    /// Whether <paramref name="type"/> declares an instance constructor or a
    /// method, instance or static, with a <see cref="DatabaseFile"/>
    /// parameter, or an instance field of that type (a primary constructor's
    /// captured parameter included). A delegate type counts through its
    /// <c>Invoke</c> method. Compiler-generated methods, such as local
    /// functions, are skipped.
    /// </summary>
    /// <param name="type">The library type.</param>
    private static bool TakesOrHoldsDatabaseFile(Type type)
    {
        const BindingFlags instanceMembers = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;
        return type.GetConstructors(instanceMembers).SelectMany(constructor => constructor.GetParameters()).Any(parameter => Mentions(parameter.ParameterType, typeof(DatabaseFile)))
            || type.GetMethods(DeclaredMembers)
                .Where(method => !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
                .SelectMany(method => method.GetParameters())
                .Any(parameter => Mentions(parameter.ParameterType, typeof(DatabaseFile)))
            || type.GetFields(instanceMembers).Any(field => Mentions(field.FieldType, typeof(DatabaseFile)));
    }

    /// <summary>
    /// Yields each type named by the signature of an instance member that
    /// <paramref name="type"/> declares: field and property types, method
    /// return and parameter types (accessors and indexers included), and
    /// constructor parameter types, each with the member it came from.
    /// </summary>
    /// <param name="type">The library type.</param>
    private static IEnumerable<(string Member, Type Type)> InstanceSignatures(Type type)
    {
        const BindingFlags instanceMembers = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly;
        foreach (FieldInfo field in type.GetFields(instanceMembers))
        {
            yield return (field.Name, field.FieldType);
        }

        foreach (PropertyInfo property in type.GetProperties(instanceMembers))
        {
            yield return (property.Name, property.PropertyType);
        }

        foreach (MethodInfo method in type.GetMethods(instanceMembers))
        {
            yield return (method.Name, method.ReturnType);
            foreach (ParameterInfo parameter in method.GetParameters())
            {
                yield return ($"{method.Name}({parameter.Name})", parameter.ParameterType);
            }
        }

        foreach (ConstructorInfo constructor in type.GetConstructors(instanceMembers))
        {
            foreach (ParameterInfo parameter in constructor.GetParameters())
            {
                yield return ($"{constructor.Name}({parameter.Name})", parameter.ParameterType);
            }
        }
    }

    private static bool IsCompilerGenerated(Type type)
    {
        for (Type? current = type; current is not null; current = current.DeclaringType)
        {
            if (current.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            {
                return true;
            }
        }

        return false;
    }

    private static bool Mentions(Type candidate, Type target)
    {
        if (candidate == target)
        {
            return true;
        }

        if (candidate.HasElementType)
        {
            return Mentions(candidate.GetElementType()!, target);
        }

        return candidate.IsGenericType && candidate.GetGenericArguments().Any(argument => Mentions(argument, target));
    }

    private static bool IsNestedWithin(Type type, Type container)
    {
        for (Type? current = type; current is not null; current = current.DeclaringType)
        {
            if (current == container)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Depth-first walk over "holds or is constructed from" edges between
    /// library classes. Returns a rendering of the first cycle found, or
    /// <see langword="null"/>. <paramref name="state"/> records every visited
    /// type (<see langword="true"/> once fully explored).
    /// </summary>
    /// <param name="type">The type to explore.</param>
    /// <param name="state">Visited types; <see langword="false"/> while on the current path.</param>
    /// <param name="path">The current depth-first path.</param>
    private static string? FindCycle(Type type, Dictionary<Type, bool> state, Stack<Type> path)
    {
        if (state.TryGetValue(type, out bool done))
        {
            return done
                ? null
                : string.Join(" -> ", path.Reverse().SkipWhile(t => t != type).Select(t => t.Name).Append(type.Name));
        }

        state[type] = false;
        path.Push(type);
        foreach (Type dependency in DependenciesOf(type))
        {
            string? cycle = FindCycle(dependency, state, path);
            if (cycle is not null)
            {
                return cycle;
            }
        }

        _ = path.Pop();
        state[type] = true;
        return null;
    }

    private static IEnumerable<Type> DependenciesOf(Type type)
    {
        // JetTransaction is the public handle TransactionLifecycle hands out;
        // the handle calls back into its owner to commit or roll back, the
        // same owner/handle pairing as DbConnection and DbTransaction. It is
        // not a collaborator, so the walk stops there.
        if (type == typeof(JetTransaction))
        {
            return [];
        }

        IEnumerable<Type> fieldTypes = type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(field => field.FieldType);
        IEnumerable<Type> constructorTypes = type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .SelectMany(constructor => constructor.GetParameters())
            .Select(parameter => parameter.ParameterType);

        return fieldTypes.Concat(constructorTypes)
            .Where(dependency => dependency.IsClass && dependency.Assembly == Library && dependency != type)
            .Distinct();
    }
}
