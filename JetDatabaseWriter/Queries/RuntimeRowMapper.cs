namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Mapping;

/// <summary>
/// Maps an <c>object?[]</c> row (keyed by column headers) onto a new instance of a
/// runtime-resolved POCO type. Binds columns to properties through <see cref="EntityMap"/>
/// and converts values through <see cref="ValueCoercer"/>, the same rules the generic row
/// mapper uses, but for a <see cref="Type"/> only known at runtime — as needed when eagerly
/// loading a related entity discovered from a navigation property.
/// </summary>
internal static class RuntimeRowMapper
{
    private static readonly ConcurrentDictionary<Type, Func<object>> InstanceFactories = new();
    private static readonly ConcurrentDictionary<Type, Func<IList>> ListFactories = new();

    /// <summary>
    /// Creates an instance of <paramref name="type"/> and assigns each column to the
    /// property <see cref="EntityMap"/> maps it to.
    /// </summary>
    /// <param name="type">The target POCO type (must have a parameterless constructor).</param>
    /// <param name="headers">Column headers aligned with <paramref name="row"/>.</param>
    /// <param name="row">The decoded row values.</param>
    /// <returns>The populated instance.</returns>
    /// <exception cref="InvalidCastException">A value cannot be converted to the type of the property its column maps to.</exception>
    public static object Map(Type type, IReadOnlyList<string> headers, object?[] row)
    {
        object instance = CreateInstance(type);
        var map = EntityMap.For(type);

        int count = Math.Min(headers.Count, row.Length);
        for (int i = 0; i < count; i++)
        {
            if (map.FindByColumn(headers[i])?.Property is not PropertyInfo property)
            {
                continue;
            }

            object? value = row[i];
            if (value is null or DBNull)
            {
                continue;
            }

            object? coerced = ValueCoercer.Coerce(value, property.PropertyType, headers[i], property);
            if (coerced is not null)
            {
                property.SetValue(instance, coerced);
            }
        }

        return instance;
    }

    internal static object CreateInstance(Type type) =>
        InstanceFactories.GetOrAdd(type, static t =>
        {
            // Activator is banned in this project, so construction goes through a
            // compiled new T() expression factory (as the generic row mapper does).
            if (t.IsAbstract || t.IsInterface || t.GetConstructor(Type.EmptyTypes) is null)
            {
                throw new InvalidOperationException($"Type '{t}' must be a concrete class with a parameterless constructor.");
            }

            return Expression.Lambda<Func<object>>(Expression.Convert(Expression.New(t), typeof(object))).Compile();
        })();

    internal static IList CreateList(Type elementType) =>
        ListFactories.GetOrAdd(elementType, static et =>
        {
            Type listType = typeof(List<>).MakeGenericType(et);
            return Expression.Lambda<Func<IList>>(Expression.Convert(Expression.New(listType), typeof(IList))).Compile();
        })();
}
