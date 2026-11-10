namespace JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;

using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Metadata;

/// <summary>
/// Holds stable model-key snapshots with their ordered EF equality semantics.
/// </summary>
internal readonly struct JetKeyTuple : IEquatable<JetKeyTuple>
{
    private readonly ValueComparer[] comparers;

    /// <summary>
    /// Initializes a new instance of the <see cref="JetKeyTuple"/> struct.
    /// </summary>
    /// <param name="properties">The ordered properties supplying key comparison and snapshots.</param>
    /// <param name="values">The model key values.</param>
    public JetKeyTuple(IReadOnlyList<IProperty> properties, object?[] values)
    {
        this.comparers = new ValueComparer[properties.Count];
        this.Values = new object?[properties.Count];
        for (int i = 0; i < properties.Count; i++)
        {
            ValueComparer comparer = GetComparer(properties[i]);
            this.comparers[i] = comparer;
            this.Values[i] = comparer.Snapshot(values[i]);
        }
    }

    /// <summary>Gets the snapshotted model key values.</summary>
    public object?[] Values { get; }

    /// <summary>Gets the comparison used by the principal key for a property.</summary>
    /// <param name="property">The key or foreign-key property.</param>
    /// <returns>The corresponding EF key comparer.</returns>
    public static ValueComparer GetComparer(IProperty property)
        => (property.FindFirstPrincipal() ?? property).GetKeyValueComparer();

    /// <summary>Checks whether key equality comes from the property's default type mapping.</summary>
    /// <param name="property">The key or foreign-key property.</param>
    /// <returns>Whether the comparer has the admitted default equality semantics.</returns>
    public static bool HasDefaultComparer(IProperty property)
    {
        IProperty principal = property.FindFirstPrincipal() ?? property;
        ValueComparer comparer = principal.GetKeyValueComparer();
        return comparer.IsDefault() || ReferenceEquals(comparer, principal.GetTypeMapping().KeyComparer);
    }

    /// <inheritdoc/>
    public bool Equals(JetKeyTuple other)
    {
        if (this.Values.Length != other.Values.Length)
        {
            return false;
        }

        for (int i = 0; i < this.Values.Length; i++)
        {
            if (!this.comparers[i].Equals(this.Values[i], other.Values[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is JetKeyTuple other && this.Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        HashCode hash = default;
        for (int i = 0; i < this.Values.Length; i++)
        {
            hash.Add(this.comparers[i].GetHashCode(this.Values[i]));
        }

        return hash.ToHashCode();
    }
}
