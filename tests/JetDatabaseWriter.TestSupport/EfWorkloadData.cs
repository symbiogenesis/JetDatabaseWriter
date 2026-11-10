namespace JetDatabaseWriter.TestSupport;

using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>Builds deterministic data shared by EF workload benchmarks and their correctness tests.</summary>
internal static class EfWorkloadData
{
    /// <summary>The physical parent table.</summary>
    internal const string ParentTable = "EfWorkloadParents";

    /// <summary>The physical child table.</summary>
    internal const string ChildTable = "EfWorkloadChildren";

    /// <summary>The number of children belonging to each parent.</summary>
    internal const int ChildrenPerParent = 3;

    /// <summary>The number of parents in each measured page or write batch.</summary>
    internal const int PageSize = 10;

    /// <summary>The stored owned-reference value.</summary>
    internal const string DetailText = "owned-detail";

    /// <summary>Creates a stable GUID key for a child ordinal.</summary>
    /// <param name="ordinal">The one-based child ordinal.</param>
    /// <returns>The deterministic key.</returns>
    internal static Guid ChildKey(int ordinal) => new(ordinal, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    /// <summary>Builds the parent rows without depending on a particular library target.</summary>
    /// <param name="parentCount">The number of parents to insert.</param>
    /// <returns>The named parent values.</returns>
    internal static IEnumerable<Dictionary<string, object?>> ParentRows(int parentCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(parentCount);
        return Enumerable.Range(1, parentCount).Select(static id => new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Id"] = id,
            ["Score"] = id * 2,
            ["Name"] = "Parent" + id.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["Note"] = DetailText,
        });
    }

    /// <summary>Builds the children belonging to the deterministic parent rows.</summary>
    /// <param name="parentCount">The number of parents.</param>
    /// <returns>The named child values.</returns>
    internal static IEnumerable<Dictionary<string, object?>> ChildRows(int parentCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(parentCount);
        return Enumerable.Range(1, checked(parentCount * ChildrenPerParent)).Select(static ordinal => new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["Id"] = ChildKey(ordinal),
            ["ParentId"] = ((ordinal - 1) / ChildrenPerParent) + 1,
        });
    }

    /// <summary>Checks the complete first parent page rather than only its aggregate checksum.</summary>
    /// <param name="rows">The materialized parent values and child counts.</param>
    /// <param name="withChildren">Whether this page includes child collections.</param>
    /// <exception cref="InvalidOperationException">A key, scalar, owned value or child count differs.</exception>
    internal static void VerifyPage(IEnumerable<(int Id, int Score, string? Note, int ChildCount)> rows, bool withChildren)
    {
        int count = 0;
        foreach ((int id, int score, string? note, int childCount) in rows)
        {
            count++;
            if (id != count || score != id * 2 || note != DetailText || (withChildren && childCount != ChildrenPerParent))
            {
                throw new InvalidOperationException($"The EF workload page differs at parent {count}.");
            }
        }

        if (count != PageSize)
        {
            throw new InvalidOperationException($"The EF workload page contains {count} parents; expected {PageSize}.");
        }
    }
}
