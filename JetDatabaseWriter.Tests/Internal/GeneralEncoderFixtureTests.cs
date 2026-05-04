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

    // The 2-chunk long-row encoder (separator <c>07 09 07 06</c>) plus the
    // V2010 hard 510-byte entry-length cap together produce byte-exact
    // matches against Access-authored Table11 / Table11_desc fixtures.
    // Empty skip set: every long-row leaf validates byte-for-byte.
    private static readonly HashSet<string> LongRowStressTables = new(StringComparer.OrdinalIgnoreCase);

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
