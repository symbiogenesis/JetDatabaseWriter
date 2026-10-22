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
using JetDatabaseWriter.TestSupport;
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

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [InlineData(0)]
    [InlineData(126)]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task NativeExpressionComparison_IgnoresNulDespiteStoredIndexWeight(int prefixLength)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.General97Nul", databaseExtension: ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string script = $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',32)
            try {
                $query=$db.CreateQueryDef('', 'PARAMETERS p TEXT(255), q TEXT(255); SELECT [p]=[q] AS Eq, [p]<[q] AS Lt, Len([p]) AS PLength, Len([q]) AS QLength;')
                try {
                    $prefix='a'*{{prefixLength}}
                    $query.Parameters.Item('p').Value=$prefix+[char]0+'b'
                    $query.Parameters.Item('q').Value=$prefix+'b'
                    $rows=$query.OpenRecordset()
                    try {
                        if ($rows.Fields.Item('Eq').Value -ne -1 -or $rows.Fields.Item('Lt').Value -ne 0 -or $rows.Fields.Item('PLength').Value -ne ({{prefixLength}}+2) -or $rows.Fields.Item('QLength').Value -ne ({{prefixLength}}+1)) { throw 'Native Jet3 NUL expression comparison differs.' }
                    } finally { $rows.Close() }
                } finally { $query.Close() }
            } finally { $db.Close() }
            Write-Output 'GENERAL97_NUL_EXPRESSION_OK'
            """;
        PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(1));
        Assert.False(result.TimedOut, result.StandardOutput + result.StandardError);
        Assert.True(result.OutputComplete, result.StandardOutput + result.StandardError);
        Assert.Equal(0, result.ExitCode);
        Assert.Contains("GENERAL97_NUL_EXPRESSION_OK", result.StandardOutput, StringComparison.Ordinal);
        string prefix = new('a', prefixLength);
        var collation = new JetTextCollation(new TextSortOrder(0x0409, 0, false));
        Assert.Equal(0, collation.Compare(prefix + "\0b", prefix + "b"));
        Assert.NotEqual(General97TextIndexEncoder.Encode(prefix + "\0b", true), General97TextIndexEncoder.Encode(prefix + "b", true));
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
