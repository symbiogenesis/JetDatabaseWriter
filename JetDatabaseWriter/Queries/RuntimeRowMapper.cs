namespace JetDatabaseWriter.Queries;

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using JetDatabaseWriter.Mapping;
using JetDatabaseWriter.Models;

/// <summary>
/// Maps an <c>object?[]</c> row (keyed by column headers) onto a new instance of a
/// runtime-resolved POCO type. Binds columns to properties through <see cref="EntityMap"/>,
/// the same rule the generic row mapper uses, but for a <see cref="Type"/> only known at runtime — as
/// needed when eagerly loading a related entity discovered from a navigation property.
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

            object? coerced = Coerce(value, property.PropertyType);
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

    private static object? Coerce(object value, Type targetType)
    {
        Type underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (underlying.IsInstanceOfType(value))
        {
            return value;
        }

        if (underlying == typeof(Hyperlink) && value is string hyperlinkText)
        {
            return Hyperlink.Parse(hyperlinkText);
        }

        if (underlying == typeof(string) && value is Hyperlink hyperlink)
        {
            return hyperlink.ToString();
        }

        try
        {
            return Convert.ChangeType(value, underlying, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
        {
            return null;
        }
    }
}
