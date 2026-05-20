namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-driven validation of <see cref="GeneralTextIndexEncoder"/>.
/// Mirrors <see cref="GeneralLegacyEncoderFixtureTests"/> but targets the
/// V2010 fixture, whose default text-index sort order is "General"
/// (Access 2010+) rather than "General Legacy".
/// </summary>
public sealed class GeneralEncoderFixtureTests
{
    public static TheoryData<string> Fixtures =>
    [
        TestDatabases.TestIndexCodesV2010,
    ];

    // V2010 "General" sort-order long-row entries are pinned at 510 bytes
    // and end with a 2-byte ACE suffix that is narrowed by the DAO lab but
    // not yet implemented. Bytes [0..507] match byte-exact; the proprietary
    // suffix at [508..509] is covered by <see cref="GeneralEncoderLongRowPrefixTests"/>.
    // FIXME: remove the two table entries when the suffix contribution tables land.
    // Details: <c>docs/format-probe/format-probe-long-row-index-encoding.md</c>.
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
