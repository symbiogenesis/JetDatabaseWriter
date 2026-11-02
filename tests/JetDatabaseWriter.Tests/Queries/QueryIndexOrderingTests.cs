namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Queries;
using Xunit;

/// <summary>Index ordering preserves the mapped values and the complete LINQ key expression.</summary>
public sealed class QueryIndexOrderingTests
{
    private const string Table = "Items";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets integral native ranges whose values are exactly representable as Int64.</summary>
    public static TheoryData<DatabaseFormat, Type> IntegralRanges => new()
    {
        { DatabaseFormat.Jet4Mdb, typeof(byte) },
        { DatabaseFormat.AceAccdb, typeof(byte) },
        { DatabaseFormat.Jet4Mdb, typeof(short) },
        { DatabaseFormat.AceAccdb, typeof(short) },
        { DatabaseFormat.Jet4Mdb, typeof(int) },
        { DatabaseFormat.AceAccdb, typeof(int) },
        { DatabaseFormat.AceAccdb, typeof(long) },
    };

    /// <summary>A narrowing key conversion must sort by the converted keys before paging.</summary>
    /// <param name="format">The seek-capable database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task NarrowingCast_OrdersConvertedKeysBeforePaging(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateAsync(format, typeof(int), [1, 256]);
        await using AccessReader reader = await OpenAsync(stream);
        List<IntegerRow> rows = await reader.Rows<IntegerRow>(Table, cancellationToken: Ct).ToListAsync(Ct);

        Assert.Equal(
            rows.OrderBy(row => unchecked((byte)row.Id)).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderBy(row => unchecked((byte)row.Id)).ToListAsync(Ct)).Select(row => row.Id));
        Assert.Equal(
            rows.OrderBy(row => unchecked((byte)row.Id)).Take(1).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderBy(row => unchecked((byte)row.Id)).Take(1).ToListAsync(Ct)).Select(row => row.Id));
        Assert.Equal(
            rows.OrderByDescending(row => unchecked((byte)row.Id)).Skip(1).Take(1).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderByDescending(row => unchecked((byte)row.Id)).Skip(1).Take(1).ToListAsync(Ct)).Select(row => row.Id));
    }

    /// <summary>A checked conversion still observes overflow before a following Take returns.</summary>
    /// <param name="format">The seek-capable database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CheckedCast_PreservesOverflowBeforePaging(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateAsync(format, typeof(int), [1, 256]);
        await using AccessReader reader = await OpenAsync(stream);
        List<IntegerRow> rows = await reader.Rows<IntegerRow>(Table, cancellationToken: Ct).ToListAsync(Ct);

        Assert.Throws<OverflowException>(() => rows.OrderBy(row => checked((byte)row.Id)).Take(1).ToList());
        await Assert.ThrowsAsync<OverflowException>(async () =>
            await reader.Query<IntegerRow>(Table).OrderBy(row => checked((byte)row.Id)).Take(1).ToListAsync(Ct));
        await Assert.ThrowsAsync<OverflowException>(async () =>
            await reader.Query<IntegerRow>(Table).OrderBy(row => checked((byte)row.Id)).Skip(1).Take(1).ToListAsync(Ct));
    }

    /// <summary>A mapped integer property does not establish the native Text index's order.</summary>
    /// <param name="format">The seek-capable database format.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task TextMappedToInteger_OrdersMappedValuesBeforePaging(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateAsync(format, typeof(string), ["10", "2"]);
        await using AccessReader reader = await OpenAsync(stream);
        List<IntegerRow> rows = await reader.Rows<IntegerRow>(Table, cancellationToken: Ct).ToListAsync(Ct);

        Assert.Equal(
            rows.OrderBy(row => row.Id).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderBy(row => row.Id).ToListAsync(Ct)).Select(row => row.Id));
        Assert.Equal(
            rows.OrderBy(row => row.Id).Take(1).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct)).Select(row => row.Id));
        Assert.Equal(
            rows.OrderByDescending(row => row.Id).Skip(1).Take(1).Select(row => row.Id),
            (await reader.Query<IntegerRow>(Table).OrderByDescending(row => row.Id).Skip(1).Take(1).ToListAsync(Ct)).Select(row => row.Id));
    }

    /// <summary>Exact integral mappings retain their index fast path and agree at signed boundaries.</summary>
    /// <param name="format">The seek-capable database format.</param>
    /// <param name="columnType">The native integral column type.</param>
    [Theory]
    [MemberData(nameof(IntegralRanges))]
    public async Task ExactIntegralMappings_RetainIndexOrderAndPaging(DatabaseFormat format, Type columnType)
    {
        object[] keys = (int)Type.GetTypeCode(columnType) switch
        {
            (int)TypeCode.Byte => [byte.MaxValue, byte.MinValue],
            (int)TypeCode.Int16 => [short.MaxValue, short.MinValue, (short)0],
            (int)TypeCode.Int32 => [int.MaxValue, int.MinValue, 0],
            _ => [long.MaxValue, long.MinValue, 0L],
        };
        await using MemoryStream stream = await CreateAsync(format, columnType, keys);
        await using AccessReader reader = await OpenAsync(stream);
        List<LongRow> rows = await reader.Rows<LongRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        ResolvedTable? resolved = await reader.Services.Tables.ResolveLocalTableAsync(Table, Ct);
        Assert.NotNull(resolved);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync(Table, Ct);
        var stage = new OrderStage();
        Expression<Func<LongRow, long>> selector = row => row.Id;
        stage.Keys.Add(new OrderingKey(selector, Descending: false));
        Assert.NotNull(stage.FindCoveringIndex(indexes, resolved.Definition, typeof(LongRow)));

        Assert.Equal(
            rows.OrderBy(row => row.Id).Select(row => row.Id),
            (await reader.Query<LongRow>(Table).OrderBy(row => row.Id).ToListAsync(Ct)).Select(row => row.Id));
        Assert.Equal(
            rows.OrderBy(row => row.Id).Skip(1).Take(1).Select(row => row.Id),
            (await reader.Query<LongRow>(Table).OrderBy(row => row.Id).Skip(1).Take(1).ToListAsync(Ct)).Select(row => row.Id));
    }

    /// <summary>A property accessor can change native key values even without an expression-tree cast.</summary>
    [Fact]
    public async Task CustomAccessor_SortsItsObservedValues()
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.Jet4Mdb, typeof(int), [1, 256]);
        await using AccessReader reader = await OpenAsync(stream);
        List<TransformedRow> rows = await reader.Rows<TransformedRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        Assert.Equal(
            rows.OrderBy(row => row.Id).Take(1).Select(row => row.Id),
            (await reader.Query<TransformedRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct)).Select(row => row.Id));
    }

    /// <summary>A narrowing row projection cannot hide a later conversion error behind index paging.</summary>
    [Fact]
    public async Task NarrowingRowMapping_PreservesConversionFailureBeforePaging()
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.AceAccdb, typeof(long), [1L, long.MaxValue]);
        await using AccessReader reader = await OpenAsync(stream);
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Rows<IntegerRow>(Table, cancellationToken: Ct).ToListAsync(Ct));
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Query<IntegerRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct));
    }

    /// <summary>Another mapped setter can change an otherwise automatic ordering property.</summary>
    [Fact]
    public async Task OtherMappedSetter_SortsTheFinalMaterializedKey()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(Table, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Trigger", typeof(int))], Ct);
            await writer.InsertRowsAsync(Table, [[1, 0], [2, 0]], Ct);
        }

        await using AccessReader reader = await OpenAsync(stream);
        List<MutatingSetterRow> rows = await reader.Rows<MutatingSetterRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        Assert.Equal(
            rows.OrderBy(row => row.Id).Take(1).Select(row => row.Id),
            (await reader.Query<MutatingSetterRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct)).Select(row => row.Id));
    }

    /// <summary>Covariance does not change which concrete entity the provider materializes.</summary>
    [Fact]
    public async Task CovariantQuery_OrdersTheSelectedBaseProperty()
    {
        await using MemoryStream stream = await CreateAsync(DatabaseFormat.Jet4Mdb, typeof(int), [2, 1]);
        await using AccessReader reader = await OpenAsync(stream);
        List<DerivedRow> rows = await reader.Rows<DerivedRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        IQueryable<BaseRow> query = reader.Query<DerivedRow>(Table);
        Assert.Equal(
            rows.OrderBy(row => ((BaseRow)row).Id).Take(1).Select(row => row.Id),
            (await query.OrderBy(row => row.Id).Take(1).ToListAsync(Ct)).Cast<DerivedRow>().Select(row => row.Id));
    }

    /// <summary>Index paging must not hide an overflowing conversion on another mapped column.</summary>
    [Fact]
    public async Task OtherMappedConversion_PreservesFailureBeforePaging()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(Table, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Payload", typeof(int))], Ct);
            await writer.InsertRowsAsync(Table, [[1, 1], [2, 256]], Ct);
        }

        await using AccessReader reader = await OpenAsync(stream);
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Rows<NarrowPayloadRow>(Table, cancellationToken: Ct).ToListAsync(Ct));
        await Assert.ThrowsAsync<InvalidCastException>(async () =>
            await reader.Query<NarrowPayloadRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct));
    }

    /// <summary>Ordinary exact nonkey fields and integral widening still permit index paging.</summary>
    [Fact]
    public async Task TotalNonkeyMappings_RetainIndexPaging()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(Table, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), maxLength: 20), new("Count", typeof(short))], Ct);
            await writer.InsertRowsAsync(Table, [[2, "two", (short)-1], [1, "one", short.MaxValue]], Ct);
        }

        await using AccessReader reader = await OpenAsync(stream);
        List<CompleteRow> rows = await reader.Rows<CompleteRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        ResolvedTable? resolved = await reader.Services.Tables.ResolveLocalTableAsync(Table, Ct);
        Assert.NotNull(resolved);
        var stage = new OrderStage();
        Expression<Func<CompleteRow, int>> selector = row => row.Id;
        stage.Keys.Add(new OrderingKey(selector, Descending: false));
        Assert.NotNull(stage.FindCoveringIndex(await reader.ListIndexesAsync(Table, Ct), resolved.Definition, typeof(CompleteRow)));
        Assert.Equal(
            rows.OrderBy(row => row.Id).Take(1).Select(row => (row.Id, row.Label, row.Count)),
            (await reader.Query<CompleteRow>(Table).OrderBy(row => row.Id).Take(1).ToListAsync(Ct)).Select(row => (row.Id, row.Label, row.Count)));
    }

    /// <summary>Required unique indexes retain the fast path; optional Null ties retain scan stability.</summary>
    /// <param name="required">Whether the nonprimary unique index refuses Null keys.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UniqueIndexes_RequireNonNullKeysForIndexPaging(bool required)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync(Table, [new("Id", typeof(int)) { IsPrimaryKey = true }, new("SortKey", typeof(int))], [new IndexDefinition("Ordering", "SortKey") { IsUnique = true, IsRequired = required }], Ct);
            object?[][] input = required
                ? [[2, 20], [1, 10], [3, 30]]
                : [[2, null], [1, null], [3, 10]];
            await writer.InsertRowsAsync(Table, input, Ct);
        }

        await using AccessReader reader = await OpenAsync(stream);
        List<OptionalKeyRow> rows = await reader.Rows<OptionalKeyRow>(Table, cancellationToken: Ct).ToListAsync(Ct);
        ResolvedTable? resolved = await reader.Services.Tables.ResolveLocalTableAsync(Table, Ct);
        Assert.NotNull(resolved);
        var stage = new OrderStage();
        Expression<Func<OptionalKeyRow, int?>> selector = row => row.SortKey;
        stage.Keys.Add(new OrderingKey(selector, Descending: false));
        IndexMetadata? selected = stage.FindCoveringIndex(await reader.ListIndexesAsync(Table, Ct), resolved.Definition, typeof(OptionalKeyRow));
        if (required)
        {
            Assert.NotNull(selected);
            Assert.Equal(IndexKind.Normal, selected.Kind);
            Assert.Equal("Ordering", selected.Name);
        }
        else
        {
            Assert.Null(selected);
        }

        Assert.Equal(
            rows.OrderBy(row => row.SortKey).Take(2).Select(row => row.Id),
            (await reader.Query<OptionalKeyRow>(Table).OrderBy(row => row.SortKey).Take(2).ToListAsync(Ct)).Select(row => row.Id));
    }

    private static async ValueTask<MemoryStream> CreateAsync(DatabaseFormat format, Type columnType, object[] keys)
    {
        var stream = new MemoryStream();
        try
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
            ColumnDefinition column = columnType == typeof(string)
                ? new("Id", columnType, maxLength: 20) { IsPrimaryKey = true }
                : new("Id", columnType) { IsPrimaryKey = true };
            await writer.CreateTableAsync(Table, [column], Ct);
            await writer.InsertRowsAsync(Table, keys.Select(key => new object?[] { key }), Ct);
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private static ValueTask<AccessReader> OpenAsync(Stream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
    }

#pragma warning disable CA1812 // Typed row readers and query expression trees create these projections.
    /// <summary>The integer projection deliberately also accepts native Text values.</summary>
    private sealed class IntegerRow
    {
        /// <summary>Gets or sets the mapped key.</summary>
        public int Id { get; set; }
    }

    /// <summary>An exact widening projection across native integral types.</summary>
    private sealed class LongRow
    {
        /// <summary>Gets or sets the mapped key.</summary>
        public long Id { get; set; }
    }

    /// <summary>A custom getter transforms the stored native key.</summary>
    private sealed class TransformedRow
    {
        /// <summary>Gets or sets the mapped key through a sign-changing getter.</summary>
        public int Id { get => -field; set; }
    }

    /// <summary>An independent setter changes the mapped ordering key.</summary>
    private sealed class MutatingSetterRow
    {
        /// <summary>Gets or sets the mapped key.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the second column and changes the first column's projection.</summary>
        public int Trigger
        {
            get;
            set
            {
                field = value;
                this.Id = -this.Id;
            }
        }
    }

    /// <summary>A covariant query can refer to this independent base property.</summary>
    private class BaseRow
    {
        /// <summary>Gets or sets the unmapped base key.</summary>
        public int Id { get; set; }
    }

    /// <summary>The concrete materializer binds the derived property.</summary>
    private sealed class DerivedRow : BaseRow
    {
        /// <summary>Gets or sets the actual mapped key.</summary>
        public new int Id { get; set; }
    }

    /// <summary>An unrelated column has a fallible narrowing projection.</summary>
    private sealed class NarrowPayloadRow
    {
        /// <summary>Gets or sets the exact mapped key.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the narrowed payload.</summary>
        public byte Payload { get; set; }
    }

    /// <summary>Ordinary identity fields and a widened integral payload.</summary>
    private sealed class CompleteRow
    {
        /// <summary>Gets or sets the mapped key.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets an identity text projection.</summary>
        public string Label { get; set; } = string.Empty;

        /// <summary>Gets or sets a widened integral projection.</summary>
        public long Count { get; set; }
    }

    /// <summary>An optional unique sort key can contain stable Null ties.</summary>
    private sealed class OptionalKeyRow
    {
        /// <summary>Gets or sets the independent row identity.</summary>
        public int Id { get; set; }

        /// <summary>Gets or sets the optional ordering key.</summary>
        public int? SortKey { get; set; }
    }
#pragma warning restore CA1812
}
