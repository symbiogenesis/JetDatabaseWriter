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
using JetDatabaseWriter.Relationships;
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
/// it uses. No collaborator may hold or receive a facade, <see cref="AccessBase"/>,
/// or a composition root; each collaborator graph must be acyclic; the facade
/// must not be reachable from its services at runtime; and the facades expose
/// no internal members.
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

    [Fact]
    public void WriterCompositionRoot_ReachesEveryWriterWorkflow()
    {
        var state = new Dictionary<Type, bool>();
        _ = FindCycle(typeof(WriterServices), state, new Stack<Type>());

        Type[] expected =
        [
            typeof(DatabaseFile),
            typeof(TableCatalog),
            typeof(TableDataWriter),
            typeof(TableSchemaEditor),
            typeof(TableRowStore),
            typeof(RelationshipManager),
            typeof(RelationshipEnforcer),
            typeof(ComplexColumnManager),
            typeof(CatalogWriter),
            typeof(CatalogArtifactWriter),
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

        HashSet<object> reachable = ReachableLibraryObjects(FacadeInternals.ReadPrivateField(writer, "services")!);
        object database = FacadeInternals.Database(writer);

        // Every database file and page cache the writer's services hold is the
        // writer's own: no second file is opened to read rows back, and no
        // page cache keeps pages between calls.
        Assert.Single(reachable, item => item is DatabaseFile);
        Assert.Contains(database, reachable);
        Assert.All(reachable.OfType<ReaderPageCache>(), cache => Assert.Null(FacadeInternals.ReadPrivateField(cache, "pageCache")));
        Assert.DoesNotContain(reachable, item => item is AccessReader);
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

        HashSet<object> reachable = ReachableLibraryObjects(FacadeInternals.Services(reader));

        Assert.DoesNotContain(reader, reachable);
        Assert.Contains(FacadeInternals.Database(reader), reachable);
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

        HashSet<object> reachable = ReachableLibraryObjects(FacadeInternals.ReadPrivateField(writer, "services")!);

        Assert.DoesNotContain(writer, reachable);
        Assert.Contains(FacadeInternals.Database(writer), reachable);
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
