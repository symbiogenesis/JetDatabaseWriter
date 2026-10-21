namespace JetDatabaseWriter.Models;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Lower or upper bound for an index-key range query.
/// </summary>
/// <remarks>
/// <see cref="Values"/> contains one or more leading index-key column
/// values in index order. Use <see cref="Inclusive"/> or <see cref="Exclusive"/>
/// to build common bounds concisely.
/// </remarks>
public sealed class IndexKeyBound
{
    /// <summary>
    /// Initializes a new instance of the <see cref="IndexKeyBound"/> class.
    /// </summary>
    /// <param name="values">One or more leading index-key values.</param>
    /// <param name="isInclusive">Whether the bound includes matching keys.</param>
    public IndexKeyBound(IReadOnlyList<object?> values, bool isInclusive = true)
    {
        Guard.NotNull(values, nameof(values));

        if (values.Count == 0)
        {
            throw new ArgumentException("At least one key value is required.", nameof(values));
        }

        object?[] copy = new object?[values.Count];
        for (int i = 0; i < values.Count; i++)
        {
            copy[i] = values[i];
        }

        this.Values = copy;
        this.IsInclusive = isInclusive;
    }

    /// <summary>Gets the leading index-key values for this bound.</summary>
    public IReadOnlyList<object?> Values { get; }

    /// <summary>Gets a value indicating whether this bound includes matching keys.</summary>
    public bool IsInclusive { get; }

    /// <summary>Creates an inclusive index-key bound.</summary>
    /// <param name="values">One or more leading index-key values.</param>
    /// <returns>An inclusive key bound.</returns>
    public static IndexKeyBound Inclusive(params object?[] values) => new(values, isInclusive: true);

    /// <summary>Creates an exclusive index-key bound.</summary>
    /// <param name="values">One or more leading index-key values.</param>
    /// <returns>An exclusive key bound.</returns>
    public static IndexKeyBound Exclusive(params object?[] values) => new(values, isInclusive: false);
}
