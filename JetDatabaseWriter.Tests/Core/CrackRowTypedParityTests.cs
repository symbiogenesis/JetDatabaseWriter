namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Pins parity between the Phase 2 typed-row cracker
/// (<c>AccessReader.CrackRowTypedAsync</c>) and the legacy
/// string→<c>ConvertRowToTyped</c> path used by the public
/// <see cref="AccessReader.Rows(string, IProgress{long}, System.Threading.CancellationToken)"/>
/// API. The typed cracker MUST produce the same boxed CLR values as the
/// legacy path, modulo the three documented Phase-1 divergences (negative
/// short overflow, decimal.MaxValue mantissa, sub-second DateTime precision)
/// — none of which are exercised by the NorthwindTraders sample tables
/// chosen here.
/// </summary>
public sealed class CrackRowTypedParityTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    [Theory]
    [InlineData("Order Details")]
    [InlineData("Customers")]
    [InlineData("Products")]
    [InlineData("Employees")]
    public async Task TypedCracker_MatchesLegacyRowsForNorthwindTable(string tableName)
    {
        var reader = await db.GetReaderAsync(TestDatabases.NorthwindTraders, TestContext.Current.CancellationToken);

        var legacy = new List<object?[]>();
        await foreach (object[] row in reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken))
        {
            legacy.Add(row);
        }

        var typed = new List<object?[]>();
        await foreach (object?[] row in reader.EnumerateRowsTypedNewPathAsync(tableName, TestContext.Current.CancellationToken))
        {
            typed.Add(row);
        }

        Assert.Equal(legacy.Count, typed.Count);
        for (int r = 0; r < legacy.Count; r++)
        {
            object?[] a = legacy[r];
            object?[] b = typed[r];
            Assert.Equal(a.Length, b.Length);
            for (int c = 0; c < a.Length; c++)
            {
                AssertCellEqual(a[c], b[c], $"row {r}, col {c}");
            }
        }
    }

    private static void AssertCellEqual(object? legacy, object? typed, string context)
    {
        // Both DBNull? Equal.
        bool legacyNull = legacy is null or DBNull;
        bool typedNull = typed is null or DBNull;
        if (legacyNull && typedNull)
        {
            return;
        }

        Assert.False(legacyNull ^ typedNull, $"{context}: null mismatch (legacy={legacy}, typed={typed})");

        // Floating-point: compare via invariant string repr to avoid a strict
        // bit-pattern equality check on values that originate from the same
        // 8-byte payload but go through different formatting paths in the
        // legacy round-trip.
        if (legacy is float lf && typed is float tf)
        {
            Assert.Equal(lf, tf);
            return;
        }

        if (legacy is double ld && typed is double td)
        {
            Assert.Equal(ld, td);
            return;
        }

        Assert.Equal(legacy, typed);
    }
}
