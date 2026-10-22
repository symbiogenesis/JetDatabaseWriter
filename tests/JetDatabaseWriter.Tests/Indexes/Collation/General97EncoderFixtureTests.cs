namespace JetDatabaseWriter.Tests.Indexes.Collation;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Collation;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Fixture-driven validation of <see cref="General97TextIndexEncoder"/>.
/// Mirrors <see cref="GeneralLegacyEncoderFixtureTests"/> but targets the
/// V1997 (Jet3) fixture, whose default text-index sort order is
/// "General 97".
/// </summary>
public sealed class General97EncoderFixtureTests
{
    public static TheoryData<string> Fixtures =>
    [
        TestDatabases.TestIndexCodesV1997,
    ];

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

    [Fact]
    public async Task NativeExpansionBeforeAccent_CountsExpandedSignificantPositions()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "mdbtools", "nwind.mdb"), cancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: cancellationToken);
        byte[] expected = Convert.FromHexString("7F736D78777E6675116D66616670766F6A7777666D68757276766F60756C776611606802222222222260");
        byte[]? observed = null;
        await CompactIndexAudit.ScanAsync(
            file.Pages,
            file.Format,
            550,
            587,
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry" && record.TargetPageNumber == 584 && record.TargetRowIndex == 11)
                {
                    Assert.Null(observed);
                    observed = record.CanonicalKey;
                }

                return default;
            },
            cancellationToken);
        Assert.Equal(expected, observed);
        Assert.Equal(expected, General97TextIndexEncoder.Encode("Plutzer Lebensmittelgroßmärkte AG", true));
    }

    [Fact]
    public async Task NativeEmbeddedNul_ContributesItsStoredInlineWeightInCompositeKey()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Databases", "mdbtools", "nwind.mdb"), cancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using var file = DatabaseFile.ForReader(stream, bytes, ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: true, cancellationToken: cancellationToken);
        byte[] expected = [0x7F, 0x6D, 0x72, 0x62, 0x6C, 0x10, 0, 0x7F, 0x80, 0, 0, 0, 0x80, 0x7F, 0xFF, 0xFF, 0xFE];
        byte[]? observed = null;
        await CompactIndexAudit.ScanAsync(
            file.Pages,
            file.Format,
            220,
            414,
            new(),
            (record, _) =>
            {
                if (record.Kind == "leaf-entry" && record.TargetPageNumber == 225 && record.TargetRowIndex == 0)
                {
                    Assert.Null(observed);
                    observed = record.CanonicalKey;
                }

                return default;
            },
            cancellationToken);
        Assert.Equal(expected, observed);

        TDefImage? definition = await file.TableDefs.ReadImageAsync(220, cancellationToken);
        Assert.NotNull(definition);
        bool rawNameObserved = false;
        await CompactRawRowReader.ScanAsync(
            file.Pages,
            file.Format,
            220,
            new(),
            async (location, payload, token) =>
            {
                if (location.PageNumber == 225 && location.RowIndex == 0)
                {
                    await CompactRawFieldReader.ScanAsync(
                        file.Format,
                        definition,
                        location,
                        payload,
                        (field, _) =>
                        {
                            if (field.Column.Name == "Name")
                            {
                                Assert.Equal("Lock\0"u8.ToArray(), field.RawBytes.ToArray());
                                rawNameObserved = true;
                            }

                            return default;
                        },
                        token);
                }
            },
            cancellationToken);
        Assert.True(rawNameObserved);
        byte[] encoded = [.. General97TextIndexEncoder.Encode("Lock\0", true), .. IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, 0), .. IndexKeyEncoder.EncodeEntry(ColumnType.LongIntegerType, 1, ascending: false)];
        Assert.Equal(expected, encoded);
    }
}
