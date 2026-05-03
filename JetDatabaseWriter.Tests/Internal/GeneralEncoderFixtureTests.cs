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

    // V2010 long-row stress tables — Jackcess's IndexCodesTest skips them
    // with the same TODO ("long rows not handled completely yet in V2010").
    // Repo-memory note: /memories/repo/long-row-index-todo.md.
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
