namespace JetDatabaseWriter.Tests.Internal;

using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Fixture-driven validation of <see cref="General97TextIndexEncoder"/>.
/// Mirrors <see cref="GeneralLegacyEncoderFixtureTests"/> but targets the
/// V1997 (Jet3) fixture, whose default text-index sort order is
/// "General 97".
/// <para>
/// Closes the §1.1 General-97 encoder coverage gap from
/// <c>docs/design/test-coverage-gaps.md</c>.
/// </para>
/// </summary>
public sealed class General97EncoderFixtureTests
{
    public static TheoryData<string> Fixtures => new()
    {
        TestDatabases.TestIndexCodesV1997,
    };

    [Theory]
    [MemberData(nameof(Fixtures))]
    public Task TextSingleColumnIndexes_OnDiskLeavesMatchEncoderOutput(string fixturePath)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        return TextIndexEncoderFixtureHarness.ValidateAsync(
            fixturePath,
            General97TextIndexEncoder.Encode,
            ct: ct);
    }
}
