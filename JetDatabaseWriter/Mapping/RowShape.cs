namespace JetDatabaseWriter.Mapping;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Schema.Models;

/// <summary>Immutable identity of the ordered columns consumed by a materializer.</summary>
internal sealed class RowShape : IEquatable<RowShape>
{
    private readonly ColumnShape[] columns;
    private readonly int hashCode;

    /// <summary>Initializes a new instance of the <see cref="RowShape"/> class.</summary>
    /// <param name="headers">The ordered column names.</param>
    /// <param name="types">The projected source types.</param>
    /// <param name="layout">The physical column descriptors, when available.</param>
    internal RowShape(IReadOnlyList<string> headers, IReadOnlyList<Type>? types, IReadOnlyList<ColumnInfo>? layout = null)
    {
        this.columns = new ColumnShape[headers.Count];
        var hash = new HashCode();
        for (int i = 0; i < headers.Count; i++)
        {
            ColumnInfo? column = layout?[i];
            this.columns[i] = new ColumnShape(headers[i], types != null && i < types.Count ? types[i] : null, column?.Type, column?.ColNum, column?.VarIdx, column?.FixedOff, column?.Size, column?.Flags, column?.ExtraFlags, column?.CalculatedResultType, column?.NumericScale, column?.IsAutoNumber);
            hash.Add(this.columns[i]);
        }

        this.hashCode = hash.ToHashCode();
    }

    /// <inheritdoc/>
    public bool Equals(RowShape? other)
    {
        if (other == null || this.hashCode != other.hashCode || this.columns.Length != other.columns.Length)
        {
            return false;
        }

        for (int i = 0; i < this.columns.Length; i++)
        {
            if (this.columns[i] != other.columns[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is RowShape other && this.Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => this.hashCode;

    private sealed record ColumnShape(string Name, Type? ClrType, Enums.ColumnType? Type, int? ColNum, int? VarIdx, int? FixedOff, int? Size, byte? Flags, byte? ExtraFlags, Enums.ColumnType? CalculatedResultType, byte? NumericScale, bool? IsAutoNumber);
}
