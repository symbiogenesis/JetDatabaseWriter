namespace JetDatabaseWriter.Tests.Internal;

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
/// Fixture-driven validation of <see cref="GeneralLegacyTextIndexEncoder"/>.
/// For each Jackcess <c>testIndexCodes*</c> fixture, walks every Text/Memo
/// single-column index B-tree on disk, then re-encodes each indexed value
/// with our encoder and asserts byte equality against the on-disk leaf keys.
/// <para>
/// This is the closest analogue in this repository of Jackcess's own
/// <c>IndexCodesTest</c>: the upstream test reads index entries from these
/// same fixtures via reflection on private cursor state and compares them to
/// keys it constructs by re-running its encoder. The fixtures were authored
/// by Microsoft Access itself, so byte-exact agreement here proves the
/// encoder's per-codepoint tables AND the framing/extra/unprintable/crazy
/// state machine match the on-disk Access format.
/// </para>
/// <para>
/// V1997 / V2010 fixtures are validated by the sibling
/// <see cref="General97EncoderFixtureTests"/> /
/// <see cref="GeneralEncoderFixtureTests"/> classes, which use the
/// per-format encoder appropriate to the fixture's default sort order.
/// </para>
/// </summary>
public sealed class GeneralLegacyEncoderFixtureTests
{
    public static TheoryData<string> Fixtures => new()
    {
        // Scope:
        //  • V1997 covered by General97EncoderFixtureTests (Jet3 default sort
        //    order is "General 97", a different state machine).
        //  • V2010 covered by GeneralEncoderFixtureTests (Access 2010+ default
        //    sort order is "General", which uses different per-codepoint
        //    tables — see GeneralIndexCodes in Jackcess).
        TestDatabases.TestIndexCodesV2000,
        TestDatabases.TestIndexCodesV2003,
        TestDatabases.TestIndexCodesV2007,
    };

    // Tables previously skipped because their indexed Memo values exceed
    // the single-chunk indexed-text cap (127 chars). The encoder now emits
    // the 2-chunk long-row layout reverse-engineered from the Access-authored
    // fixtures (separator <c>08 07 08 04</c> for General Legacy, single
    // unified extras/unprintable/crazy block, descending-pass complement
    // applied to the joined chunks). See
    // <c>docs/design/long-row-index-encoding.md</c>.
    private static readonly HashSet<string> LongRowStressTables = new(StringComparer.OrdinalIgnoreCase);

    [Theory]
    [MemberData(nameof(Fixtures))]
    public Task TextSingleColumnIndexes_OnDiskLeavesMatchEncoderOutput(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        return TextIndexEncoderFixtureHarness.ValidateAsync(
            fixturePath,
            GeneralLegacyTextIndexEncoder.Encode,
            LongRowStressTables,
            ct: ct);
    }
}
