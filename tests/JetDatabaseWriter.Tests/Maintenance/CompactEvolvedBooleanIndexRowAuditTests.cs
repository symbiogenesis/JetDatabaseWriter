namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema.Models;
using Xunit;

/// <summary>Native appended Boolean columns preserve False for old absent slots regardless of defaults.</summary>
public sealed class CompactEvolvedBooleanIndexRowAuditTests
{
    /// <summary>Gets independent native default and compact controls with pinned specimen hashes.</summary>
    public static TheoryData<string, string> Fixtures =>
    [
        new("Ace-LateDefaultFalse-compacted.accdb", "81B31622868DBBDC2FDEFA6FCFB18BEE483FD246BBAEB833A77B552E741ABF19"),
        new("Ace-LateDefaultFalse-indexed.accdb", "211CA67DBBC5A4FE839FB9D73C7D4DF251C96811CD0CF1518EF000FB6607EF0A"),
        new("Ace-LateDefaultTrue-compacted.accdb", "D80C82547ADF73F10810FCBA7BB623583349A313B61AD3E657143F2787710EA8"),
        new("Ace-LateDefaultTrue-indexed.accdb", "81A6504CFADB7A939E8B092D87DE634A5364F6A92AE72E80C59A417C162702AB"),
        new("Jet3-LateDefaultFalse-compacted.mdb", "A9E7B14D467E171A3D423F5374135F750935B333238A175E99F0249F21D0E325"),
        new("Jet3-LateDefaultFalse-indexed.mdb", "C41A9AD264DA318150B230C12441E8DCF7C2A87198FFB157EC5BA838BB51A30E"),
        new("Jet3-LateDefaultTrue-compacted.mdb", "9E30CA34071C423AC8CE06B0606969683653ABE2A028E4E3292E106F0A842D05"),
        new("Jet3-LateDefaultTrue-indexed.mdb", "EF4739E32B6CD11E62855399F012183DFE8535E2E521463C2B40DBB63D43ED36"),
        new("Jet4-LateDefaultFalse-compacted.mdb", "9A93969007196ADA77B6867A2690D0EB89AEB4887A1C0A78E290F147F0C1FD03"),
        new("Jet4-LateDefaultFalse-indexed.mdb", "EB2D2EE3F76B461E1321C3D57410151B32508C7446D953578A6A17D8AD5A124B"),
        new("Jet4-LateDefaultTrue-compacted.mdb", "8BF6C3C2DFCC0A08FD4075E24A53FC2CB791C8AF22D755B290D1CF8227A9D259"),
        new("Jet4-LateDefaultTrue-indexed.mdb", "127B3782D67D0FD8FBB2C7F15D980692220D52CF2F055ED0C86342806825E85E"),
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task NativeSchemaEvolution_AuditsAbsentFalseWithoutChangingRawObservations(string fixture, string hash)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        byte[] bytes = await ReadFixtureAsync(fixture, hash);
        await using DatabaseFile file = Open(bytes);
        (long owner, TDefImage image) = await FindAsync(file);
        bool compacted = fixture.Contains("compacted", StringComparison.Ordinal);
        var locations = new Dictionary<(long Page, int Row), int>();
        int absent = 0;
        await CompactRawRowReader.ScanAsync(
            file.Pages,
            file.Format,
            owner,
            new(),
            async (location, payload, token) =>
            {
                int id = 0;
                await CompactRawFieldReader.ScanAsync(
                    file.Format,
                    image,
                    location,
                    payload,
                    (field, _) =>
                    {
                        if (field.Column.Name == "Id")
                        {
                            id = BinaryPrimitives.ReadInt32LittleEndian(field.RawBytes.Span);
                        }
                        else if (field.Column.Name == "LateFlag")
                        {
                            Assert.Equal(2, field.Column.ColNum);
                            bool missing = !compacted && id <= 6;
                            Assert.Equal(missing ? CompactRawFieldKind.Absent : CompactRawFieldKind.Boolean, field.Kind);
                            Assert.Equal(0, field.RawBytes.Length);
                            if (missing)
                            {
                                absent++;
                            }
                            else
                            {
                                Assert.Equal(ExpectedValue(fixture, id), field.BoolValue);
                            }
                        }

                        return default;
                    },
                    token);
                Assert.InRange(id, 1, 10);
                Assert.Equal(!compacted && id <= 6 ? 2 : 3, file.Format.RowFields.ReadNumCols(payload.Span, 0));
                locations.Add((location.PageNumber, location.RowIndex), id);
            },
            Ct);
        Assert.Equal(compacted ? 0 : 6, absent);
        Assert.Equal(10, locations.Count);
        Assert.Equal(8, image.RealIndexes.Count);
        var accounting = new CompactDiscoveryAccounting(new());
        foreach (TDefLogicalIndexDescriptor logical in image.LogicalIndexes.Where(static index => index.Name.StartsWith("Late", StringComparison.Ordinal)))
        {
            int ordinal = logical.Entry.IndexNum2;
            TDefIndexDescriptor index = image.RealIndexes[ordinal];
            CompactIndexRowAuditResult result = await CompactIndexRowAudit.ValidateAsync(file.Pages, file.Format, owner, image, ordinal, new(), new(), new() { Accounting = accounting }, Ct);
            Assert.Equal(10, result.IndexedRows);
            Assert.Equal(0, result.ExcludedNullRows);
            Assert.False(result.CompleteSourceOwnershipProven);
            int observed = 0;
            await CompactIndexBookmarkAudit.ScanAsync(
                file.Pages,
                file.Format,
                owner,
                index.RootPage,
                new(),
                new(),
                (record, _) =>
                {
                    if (record.Kind == "leaf-entry")
                    {
                        int id = locations[(record.TargetPageNumber!.Value, record.TargetRowIndex!.Value)];
                        bool expected = ExpectedValue(fixture, id);
                        byte[] key = index.Columns[0].Ascending
                            ? [0x7F, expected ? (byte)0 : byte.MaxValue]
                            : [0x80, expected ? byte.MaxValue : (byte)0];
                        Assert.Equal(key, record.CanonicalKey);
                        Assert.True(record.BookmarkTargetValidated);
                        observed++;
                    }

                    return default;
                },
                Ct);
            Assert.Equal(10, observed);
            Assert.Equal(0, accounting.ActiveWorkingBytes);
        }

        int covering = image.LogicalIndexes.Single(static index => index.Name == "LateAscIgnoreNulls").Entry.IndexNum2;
        using (CompactActiveTableRows active = await CompactActiveTableRows.ValidateAsync(file.Pages, file.Format, owner, image, covering, new() { Accounting = accounting }, static (_, _) => default, Ct))
        {
            Assert.Equal(10, active.Result.ActiveRows);
            Assert.Equal(8, await active.ValidateIndexesAsync(Ct));
            Assert.False(active.Result.OwnershipProven);
        }

        Assert.Equal(0, accounting.ActiveWorkingBytes);
        Assert.Equal(bytes, await File.ReadAllBytesAsync(FixturePath(fixture), Ct));
    }

    [Theory]
    [MemberData(nameof(Fixtures))]
    public async Task NativeSchemaEvolution_PublicReadsAgreeWithNativeFalseForOldRows(string fixture, string hash)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        byte[] bytes = await ReadFixtureAsync(fixture, hash);
        foreach (bool strict in new[] { false, true })
        {
            await using var stream = new MemoryStream(bytes, writable: false);
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
            List<object[]> rows = await reader.Rows("BoolItems", cancellationToken: Ct).ToListAsync(Ct);
            List<string[]> strings = await reader.RowsAsStrings("BoolItems", cancellationToken: Ct).ToListAsync(Ct);
            List<EvolvedRow> pocos = await reader.Rows<EvolvedRow>("BoolItems", cancellationToken: Ct).ToListAsync(Ct);
            using DataTable table = await reader.ReadTableAsync("BoolItems", cancellationToken: Ct);
            using DataTable stringTable = await reader.ReadTableAsStringsAsync("BoolItems", cancellationToken: Ct);
            Assert.Equal(10, rows.Count);
            Assert.Equal(10, strings.Count);
            Assert.Equal(10, pocos.Count);
            Assert.Equal(10, table.Rows.Count);
            Assert.Equal(10, stringTable.Rows.Count);
            foreach (object[] row in rows)
            {
                int id = Assert.IsType<int>(row[0]);
                Assert.Equal(ExpectedValue(fixture, id), Assert.IsType<bool>(row[2]));
            }

            foreach (string[] row in strings)
            {
                int id = int.Parse(row[0], CultureInfo.InvariantCulture);
                Assert.Equal(ExpectedValue(fixture, id).ToString(CultureInfo.InvariantCulture), row[2]);
            }

            foreach (EvolvedRow row in pocos)
            {
                Assert.Equal(ExpectedValue(fixture, row.Id), row.LateFlag);
            }

            foreach (DataRow row in table.Rows)
            {
                Assert.Equal(ExpectedValue(fixture, (int)row["Id"]), Assert.IsType<bool>(row["LateFlag"]));
            }

            foreach (DataRow row in stringTable.Rows)
            {
                int id = int.Parse((string)row["Id"], CultureInfo.InvariantCulture);
                Assert.Equal(ExpectedValue(fixture, id).ToString(CultureInfo.InvariantCulture), row["LateFlag"]);
            }
        }
    }

    private static bool ExpectedValue(string fixture, int id)
        => id == 9 || (id == 7 && fixture.Contains("DefaultTrue", StringComparison.Ordinal));

    private static string FixturePath(string name) => Path.Combine(AppContext.BaseDirectory, "Databases", "NativeBooleanIndexes", "SchemaEvolution", name);

    private static async Task<byte[]> ReadFixtureAsync(string name, string hash)
    {
        byte[] bytes = await File.ReadAllBytesAsync(FixturePath(name), Ct);
        Assert.Equal(hash, Convert.ToHexString(SHA256.HashData(bytes)));
        return bytes;
    }

    private static DatabaseFile Open(byte[] bytes)
        => DatabaseFile.ForReader(new MemoryStream(bytes, writable: false), bytes.AsSpan(0, Math.Min(4096, bytes.Length)).ToArray(), ReadOnlyMemory<char>.Empty, string.Empty, leaveOpen: false);

    private static async Task<(long Owner, TDefImage Image)> FindAsync(DatabaseFile file)
    {
        TDefImage catalog = Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(2, Ct));
        var reader = new CatalogRowReader(file.Format, file.TableDefs, file.OwnedPages);
        long owner = Assert.Single(await reader.GetCatalogRowsAsync(catalog.Definition, Ct), static entry => entry.Name == "BoolItems").TDefPage;
        return (owner, Assert.IsType<TDefImage>(await file.TableDefs.ReadImageAsync(owner, Ct)));
    }

#pragma warning disable CA1812 // Public POCO expression materialization constructs this nullable projection target.
    private sealed class EvolvedRow
    {
        public int Id { get; set; }

        public bool? LateFlag { get; set; }
    }
#pragma warning restore CA1812
}
