namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Relationships;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>
/// Guards the writer's composition. Collaborators depend on the
/// <see cref="AccessBase"/> page I/O and format context and on each other
/// through constructor parameters; none of them may reach the
/// <see cref="AccessWriter"/> facade or its composition root, and the
/// collaborator graph must stay acyclic.
/// </summary>
public sealed class WriterServiceGraphTests
{
    private const BindingFlags DeclaredMembers =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static readonly Assembly Library = typeof(AccessWriter).Assembly;

    [Fact]
    public void Collaborators_DoNotReferenceFacadeOrCompositionRoot()
    {
        Type[] forbidden = [typeof(AccessWriter), typeof(WriterServices)];
        var violations = new List<string>();

        foreach (Type type in Library.GetTypes())
        {
            // The facade itself (and its compiler-generated state machines and
            // closures) is the only legitimate holder of these types.
            if (IsNestedWithin(type, typeof(AccessWriter)))
            {
                continue;
            }

            foreach (FieldInfo field in type.GetFields(DeclaredMembers))
            {
                if (forbidden.Contains(field.FieldType))
                {
                    violations.Add($"{type.FullName}.{field.Name} : {field.FieldType.Name}");
                }
            }

            IEnumerable<MethodBase> methods = type.GetConstructors(DeclaredMembers).Cast<MethodBase>()
                .Concat(type.GetMethods(DeclaredMembers));
            foreach (MethodBase method in methods)
            {
                foreach (ParameterInfo parameter in method.GetParameters())
                {
                    if (forbidden.Contains(parameter.ParameterType))
                    {
                        violations.Add($"{type.FullName}.{method.Name}({parameter.Name}) : {parameter.ParameterType.Name}");
                    }
                }
            }
        }

        Assert.Empty(violations);
    }

    [Fact]
    public void CollaboratorGraph_IsAcyclic()
    {
        var state = new Dictionary<Type, bool>();
        var path = new Stack<Type>();
        string? cycle = FindCycle(typeof(WriterServices), state, path);

        Assert.Null(cycle);
        Assert.DoesNotContain(typeof(AccessWriter), state.Keys);
    }

    [Fact]
    public void CompositionRoot_ReachesEveryWriterWorkflow()
    {
        var state = new Dictionary<Type, bool>();
        _ = FindCycle(typeof(WriterServices), state, new Stack<Type>());

        Type[] expected =
        [
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
        ];

        foreach (Type service in expected)
        {
            Assert.Contains(service, state.Keys);
        }
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
