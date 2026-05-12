namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

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
    public static TheoryData<string> Fixtures =>
    [
        TestDatabases.TestIndexCodesV2010,
    ];

    // V2010 "General" sort-order long-row entries are pinned at 510 bytes
    // and end with a 2-byte suffix whose algorithm has so far resisted
    // reverse-engineering — see <c>docs/format-probe/format-probe-long-row-index-encoding.md</c>
    // ("V2010: Continuous encoding with 510-byte cap" / "The 2-byte suffix").
    // Bytes [0..507] match byte-exact; the unknown suffix at [508..509] is
    // covered by <see cref="GeneralEncoderLongRowPrefixTests"/>.
    // FIXME: remove the two table entries when the suffix is solved.
    // for details and rationale, see:
    //  <c>docs/design/test-coverage-gaps.md</c> ("§1.1 General-encoder coverage gap").
    //  <c>docs/format-probe/format-probe-long-row-index-encoding.md</c> ("V2010: Continuous encoding with 510-byte cap" / "The 2-byte suffix").
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
