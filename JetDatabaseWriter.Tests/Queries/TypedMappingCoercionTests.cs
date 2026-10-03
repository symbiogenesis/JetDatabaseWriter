namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>
/// Every typed read maps a cell to its property through the same conversion: an enum property
/// binds to a Long Integer or Text column, a <see cref="Guid"/> property to a Text or 16-byte
/// Binary column, and a value the property cannot hold throws <see cref="InvalidCastException"/>
/// naming the column and the property. <c>Rows&lt;T&gt;</c>, <c>ReadTableAsync&lt;T&gt;</c>,
/// <c>Query&lt;T&gt;</c>, <c>Rows&lt;T&gt;(predicate)</c>, <c>FromIndex&lt;T&gt;</c> and the
/// entities <c>Include</c> loads must agree; <c>Include</c> used to leave such a property at
/// its default without an error. <c>Include</c> converts only the related rows a root reaches,
/// so a value in a row the query never returns does not fail it.
/// </summary>
/// <param name="db">Caches the fixture files.</param>
public sealed class TypedMappingCoercionTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Table = "Coerce";
    private const string ItemTable = "CoerceItem";

    private static readonly Guid TokenOne = new("6f9619ff-8b86-d011-b42d-00c04fc964ff");
    private static readonly Guid TokenTwo = new("0f8fad5b-d9cb-469f-a165-70867728950e");
    private static readonly Guid RawOne = new("7c9e6679-7425-40de-944b-e07fc1f90ae7");
    private static readonly Guid RawTwo = new("a1b2c3d4-e5f6-4789-8abc-def012345678");

    /// <summary>The values an enum property can hold.</summary>
    internal enum Shade
    {
        /// <summary>No shade.</summary>
        None = 0,

        /// <summary>Red.</summary>
        Red = 1,

        /// <summary>Green.</summary>
        Green = 2,

        /// <summary>Blue.</summary>
        Blue = 3,
    }

    /// <summary>Gets the three formats.</summary>
    public static TheoryData<DatabaseFormat> AllFormats =>
    [
        DatabaseFormat.Jet3Mdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    /// <summary>Gets the formats with index seeks.</summary>
    public static TheoryData<DatabaseFormat> SeekFormats =>
    [
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.AceAccdb,
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task TypedReads_EnumAndGuidProperties_MapFromIntegralTextAndBinaryColumns(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);

        string[] expected = ExpectedRoots();
        Assert.Equal(expected, Describe(await reader.Rows<CoercedRow>(Table, cancellationToken: Ct).ToListAsync(Ct)));
        Assert.Equal(expected, Describe(await reader.ReadTableAsync<CoercedRow>(Table, cancellationToken: Ct)));
        Assert.Equal(expected, Describe(await reader.Query<CoercedRow>(Table).ToListAsync(Ct)));
        Assert.Equal(expected, Describe(await reader.Rows<CoercedRow>(Table, r => r.Id >= 1, cancellationToken: Ct).ToListAsync(Ct)));
    }

    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task TypedReads_ValueOverflowingTheProperty_ThrowNamingColumnAndProperty(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);

        AssertOverflow(await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.Rows<OverflowRow>(Table, cancellationToken: Ct).ToListAsync(Ct)));
        AssertOverflow(await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.ReadTableAsync<OverflowRow>(Table, cancellationToken: Ct)));
        AssertOverflow(await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.Query<OverflowRow>(Table).ToListAsync(Ct)));
        AssertOverflow(await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.Rows<OverflowRow>(Table, r => r.Id >= 1, cancellationToken: Ct).ToListAsync(Ct)));
    }

    [Theory]
    [MemberData(nameof(SeekFormats))]
    public async Task FromIndex_MapsLikeTheOtherTypedReads(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);
        string primaryKey = (await reader.ListIndexesAsync(Table, Ct)).Single(i => i.Kind == IndexKind.PrimaryKey).Name;

        Assert.Equal(ExpectedRoots(), Describe(await reader.FromIndex<CoercedRow>(Table, primaryKey).ToRowsAsync(Ct).ToListAsync(Ct)));
        AssertOverflow(await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.FromIndex<OverflowRow>(Table, primaryKey).ToRowsAsync(Ct).ToListAsync(Ct)));
    }

    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task Include_MapsTheLoadedEntitiesLikeTheOtherTypedReads(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);

        // Collection navigation: the items are materialized by the include loader.
        List<ItemParent> parents = await reader.Query<ItemParent>(Table).Include(p => p.Items).ToListAsync(Ct);
        string[] items = [.. parents.SelectMany(p => p.Items).OrderBy(i => i.Id).Select(i => $"{i.Id}|{i.CoerceId}|{i.Shade}|{i.Token}")];
        Assert.Equal(
            [$"10|1|Blue|{TokenTwo}", $"11|1|Red|{TokenOne}", $"12|2|Green|{TokenOne}"],
            items);

        // Reference navigation: the owners are materialized by the include loader.
        List<OwnedItem> owned = await reader.Query<OwnedItem>(ItemTable).Include(i => i.Owner).ToListAsync(Ct);
        Assert.Equal(3, owned.Count);
        Assert.All(owned, i => Assert.NotNull(i.Owner));
        Assert.Equal(
            ExpectedRoots(),
            Describe([.. owned.Select(i => i.Owner!).DistinctBy(o => o.Id)]));
    }

    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task Include_ValueOverflowingTheProperty_ThrowsNamingColumnAndProperty(DatabaseFormat format)
    {
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);

        InvalidCastException collection = await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.Query<ItemParent>(Table).Include(p => p.OverflowItems).ToListAsync(Ct));
        Assert.Contains("'Big'", collection.Message, StringComparison.Ordinal);
        Assert.Contains($"'{nameof(OverflowItem)}.{nameof(OverflowItem.Small)}'", collection.Message, StringComparison.Ordinal);

        InvalidCastException reference = await Assert.ThrowsAsync<InvalidCastException>(
            async () => await reader.Query<OwnedItem>(ItemTable).Include(i => i.OverflowOwner).ToListAsync(Ct));
        AssertOverflow(reference);
    }

    [Theory]
    [MemberData(nameof(AllFormats))]
    public async Task Include_ValueOverflowingThePropertyInARowNoRootReaches_IsNotMapped(DatabaseFormat format)
    {
        // CoerceItem row 10 and Coerce row 1 hold 300 in Big, which a byte property cannot hold,
        // and no root below reaches either. One key against a table of two or three rows is too
        // large a share for the include loader's seek, so every format scans the related table.
        await using MemoryStream ms = await this.CreateAsync(format);
        await using AccessReader reader = await OpenReaderAsync(ms);

        ItemParent parent = Assert.Single(
            await reader.Query<ItemParent>(Table).Where(p => p.Id == 2).Include(p => p.OverflowItems).ToListAsync(Ct));
        Assert.Equal(["12|5"], parent.OverflowItems.Select(i => $"{i.Id}|{i.Small}"));

        OwnedItem owned = Assert.Single(
            await reader.Query<OwnedItem>(ItemTable).Where(i => i.Id == 12).Include(i => i.OverflowOwner).ToListAsync(Ct));
        Assert.Equal("2|5", $"{owned.OverflowOwner?.Id}|{owned.OverflowOwner?.Small}");

        // A ThenInclude descends only into the owners the roots reference, so owner 1's items
        // are not loaded either.
        OwnedItem held = Assert.Single(
            await reader.Query<OwnedItem>(ItemTable).Where(i => i.Id == 12).Include(i => i.Holder).ThenInclude(h => h!.OverflowItems).ToListAsync(Ct));
        Assert.Equal(["12|5"], held.Holder?.OverflowItems.Select(i => $"{i.Id}|{i.Small}"));
    }

    private static string[] ExpectedRoots() =>
    [
        $"1|Green|Blue|{TokenOne}|{RawOne}",
        $"2|{(Shade)7}|Red|{TokenTwo}|{RawTwo}",
    ];

    private static string[] Describe(IEnumerable<CoercedRow> rows) =>
        [.. rows.OrderBy(r => r.Id).Select(r => $"{r.Id}|{r.Shade}|{r.ShadeName}|{r.Token}|{r.RawToken}")];

    private static void AssertOverflow(InvalidCastException ex)
    {
        // The column is "Big"; the property bound to it through [Column] is "Small".
        Assert.Contains("'Big'", ex.Message, StringComparison.Ordinal);
        Assert.Contains($".{nameof(OverflowRow.Small)}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(int).ToString(), ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(byte).ToString(), ex.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(ex.InnerException);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream ms)
    {
        ms.Position = 0;
        return AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
    }

    private async Task<MemoryStream> CreateAsync(DatabaseFormat format)
    {
        MemoryStream ms = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(ms, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            Assert.Equal(format, writer.DatabaseFormat);
            await writer.CreateTableAsync(
                Table,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Shade", typeof(int)),
                    new ColumnDefinition("ShadeName", typeof(string), maxLength: 20),
                    new ColumnDefinition("Token", typeof(string), maxLength: 40),
                    new ColumnDefinition("RawToken", typeof(byte[]), maxLength: 16),
                    new ColumnDefinition("Big", typeof(int)),
                ],
                Ct);
            await writer.CreateTableAsync(
                ItemTable,
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("CoerceId", typeof(int)),
                    new ColumnDefinition("Shade", typeof(int)),
                    new ColumnDefinition("Token", typeof(string), maxLength: 40),
                    new ColumnDefinition("Big", typeof(int)),
                ],
                Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_CoerceItem_Coerce", Table, "Id", ItemTable, "CoerceId"), Ct);

            await writer.InsertRowsAsync(
                Table,
                [
                    [1, 2, "blue", TokenOne.ToString("B").ToUpperInvariant(), RawOne.ToByteArray(), 300],
                    [2, 7, "Red", TokenTwo.ToString("D"), RawTwo.ToByteArray(), 5],
                ],
                Ct);
            await writer.InsertRowsAsync(
                ItemTable,
                [
                    [10, 1, 3, TokenTwo.ToString("B"), 300],
                    [11, 1, 1, TokenOne.ToString("N"), 4],
                    [12, 2, 2, TokenOne.ToString("D"), 5],
                ],
                Ct);
        }

        ms.Position = 0;
        return ms;
    }

    /// <summary>Maps a Long column and a Text column to enums, and a Text and a Binary column to <see cref="Guid"/>.</summary>
    [Table(Table)]
    internal sealed class CoercedRow
    {
        public int Id { get; set; }

        public Shade Shade { get; set; }

        public Shade? ShadeName { get; set; }

        public Guid Token { get; set; }

        public Guid RawToken { get; set; }
    }

    /// <summary>Maps the Long column Big, which holds 300 in row 1, to a <see cref="byte"/>.</summary>
    internal sealed class OverflowRow
    {
        public int Id { get; set; }

        [Column("Big")]
        public byte Small { get; set; }
    }

    /// <summary>A row of <c>Coerce</c> that maps only its key, so only the included entities convert values.</summary>
    internal sealed class ItemParent
    {
        public int Id { get; set; }

        public List<CoercedItem> Items { get; set; } = [];

        public List<OverflowItem> OverflowItems { get; set; } = [];
    }

    /// <summary>A row of <c>CoerceItem</c> with an enum and a <see cref="Guid"/> property.</summary>
#pragma warning disable CA1812 // Only the include loader instantiates it, through reflection.
    [Table(ItemTable)]
    internal sealed class CoercedItem
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public int CoerceId { get; set; }

        public Shade Shade { get; set; }

        public Guid Token { get; set; }
    }

    /// <summary>A row of <c>CoerceItem</c> whose Big column, 300 in row 10, maps to a <see cref="byte"/>.</summary>
#pragma warning disable CA1812 // Only the include loader instantiates it, through reflection.
    [Table(ItemTable)]
    internal sealed class OverflowItem
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public int CoerceId { get; set; }

        [Column("Big")]
        public byte Small { get; set; }
    }

    /// <summary>A row of <c>CoerceItem</c> that maps only its keys, so only the included owners convert values.</summary>
    internal sealed class OwnedItem
    {
        public int Id { get; set; }

        public int CoerceId { get; set; }

        public CoercedRow? Owner { get; set; }

        public OverflowOwner? OverflowOwner { get; set; }

        public Holder? Holder { get; set; }
    }

    /// <summary>A row of <c>Coerce</c> that maps only its key, with the items whose Big column maps to a <see cref="byte"/>.</summary>
#pragma warning disable CA1812 // Only the include loader instantiates it, through reflection.
    [Table(Table)]
    internal sealed class Holder
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public List<OverflowItem> OverflowItems { get; set; } = [];
    }

    /// <summary>A row of <c>Coerce</c> whose Big column, 300 in row 1, maps to a <see cref="byte"/>.</summary>
#pragma warning disable CA1812 // Only the include loader instantiates it, through reflection.
    [Table(Table)]
    internal sealed class OverflowOwner
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        [Column("Big")]
        public byte Small { get; set; }
    }
}
