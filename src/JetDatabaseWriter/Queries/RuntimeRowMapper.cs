namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.ValueDecoding;

/// <summary>
/// Maps an <c>object?[]</c> row (keyed by column headers) onto a new instance of a
/// runtime-resolved POCO type. Binds columns to properties through <see cref="EntityMap"/>
/// and converts values through <see cref="ValueCoercer"/>, the same rules the generic row
/// mapper uses, but for a <see cref="Type"/> only known at runtime — as needed when eagerly
/// loading a related entity discovered from a navigation property.
/// </summary>
[RequiresUnreferencedCode("LINQ queries and Include discover entity types and members at runtime. Use typed row readers for trimmed applications.")]
[RequiresDynamicCode("LINQ queries and Include construct generic types at runtime. Use typed row readers for NativeAOT applications.")]
internal static class RuntimeRowMapper
{
    private static readonly BoundedCache<Type, Func<IList>> ListFactories = new(64);

    private delegate Func<object?[], object> MapperFactory(IReadOnlyList<string> headers, IReadOnlyList<Type>? sourceTypes);

    /// <summary>
    /// Gets the generic row mapper for a runtime-resolved entity type, reusing its bounded
    /// shape cache. Binding the factory as a delegate keeps user exceptions unwrapped.
    /// </summary>
    /// <param name="type">The target POCO type (must have a parameterless constructor).</param>
    /// <param name="headers">Column headers aligned with the decoded row values.</param>
    /// <returns>The compiled row mapper.</returns>
    /// <exception cref="JetOperationException">The generic row mapper factory cannot be reflected.</exception>
    public static Func<object?[], object> Build(Type type, IReadOnlyList<string> headers)
    {
        MethodInfo build = typeof(RowMapper<>).MakeGenericType(type).GetMethod(
            nameof(RowMapper<>.Build), [typeof(IReadOnlyList<string>), typeof(IReadOnlyList<Type>)])
            ?? throw new JetOperationException(JetErrorCode.InvalidOperationState, "The generic row mapper factory could not be reflected.");
#if NET6_0_OR_GREATER
        MapperFactory factory = build.CreateDelegate<MapperFactory>();
#else
        var factory = (MapperFactory)build.CreateDelegate(typeof(MapperFactory));
#endif
        return factory(headers, null);
    }

    internal static IList CreateList(Type elementType) =>
        ListFactories.GetOrCreate(elementType, static et =>
        {
            Type listType = typeof(List<>).MakeGenericType(et);
            return Expression.Lambda<Func<IList>>(Expression.Convert(Expression.New(listType), typeof(IList))).Compile();
        })();
}
