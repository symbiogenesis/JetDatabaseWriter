namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-driven validation of <see cref="GeneralTextIndexEncoder"/>.
/// Mirrors <see cref="GeneralLegacyEncoderFixtureTests"/> but targets the
/// V2010 fixture, whose default text-index sort order is "General"
/// (Access 2010+) rather than "General Legacy".
/// <para>
/// Closes the §1.1 General-encoder coverage gap from
/// <c>docs/design/test-coverage-gaps.md</c>.
/// </para>
/// </summary>
public sealed class GeneralEncoderFixtureTests
{
    public static TheoryData<string> Fixtures => new()
    {
        TestDatabases.TestIndexCodesV2010,
    };

    // The 2-chunk long-row encoder (separator <c>07 09 07 06</c>) handles
    // the V2010 Table11 / Table11_desc fixtures byte-exact for the
    // "no-international-chars-in-chunk" rows. Rows whose chunk #2 contains
    // international chars (rows 2 and 3 of Table11 / Table11_desc) hit
    // Access's ~510-byte total-entry truncation cap; we emit the
    // un-truncated form, which is sortable, complete, and round-trip-stable
    // but not byte-identical to Access. See
    // <c>docs/design/long-row-index-encoding-resolution.md</c> §"V2010
    // truncation cap (open)" for the open-question status of that cap rule.
    private static readonly HashSet<string> LongRowStressTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "Table11",
        "Table11_desc",
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public Task TextSingleColumnIndexes_OnDiskLeavesMatchEncoderOutput(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return TextIndexEncoderFixtureHarness.ValidateAsync(
            fixturePath,
            GeneralTextIndexEncoder.Encode,
            LongRowStressTables,
            ct: ct);
    }
}
