namespace JetDatabaseWriter.Indexes;

using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;

internal sealed class IndexQueryCriteria
{
    public static readonly IndexQueryCriteria All = new(IndexQueryKind.All, null, null, null);

    private IndexQueryCriteria(
        IndexQueryKind kind,
        IReadOnlyList<object?>? values,
        IndexKeyBound? lower,
        IndexKeyBound? upper)
    {
        this.Kind = kind;
        this.Values = values;
        this.Lower = lower;
        this.Upper = upper;
    }

    public IndexQueryKind Kind { get; }

    public IReadOnlyList<object?>? Values { get; }

    public IndexKeyBound? Lower { get; }

    public IndexKeyBound? Upper { get; }

    public bool IsFiltered => this.Kind != IndexQueryKind.All;

    public static IndexQueryCriteria Exact(IReadOnlyList<object?> values) =>
        new(IndexQueryKind.Exact, CopyValues(values, nameof(values)), null, null);

    public static IndexQueryCriteria KeyPrefix(IReadOnlyList<object?> values) =>
        new(IndexQueryKind.KeyPrefix, CopyValues(values, nameof(values)), null, null);

    public static IndexQueryCriteria Range(IndexKeyBound? lower, IndexKeyBound? upper) =>
        new(IndexQueryKind.Range, null, lower, upper);

    private static object?[] CopyValues(IReadOnlyList<object?> values, string paramName)
    {
        Guard.NotNull(values, paramName);
        object?[] copy = new object?[values.Count];
        for (int i = 0; i < values.Count; i++)
        {
            copy[i] = values[i];
        }

        return copy;
    }
}
