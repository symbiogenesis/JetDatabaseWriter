namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Xunit;

#pragma warning disable CA1812 // POCOs are instantiated by the tests and the row mapper.

/// <summary>
/// How <c>InsertRowAsync&lt;T&gt;</c> and <c>InsertRowsAsync&lt;T&gt;</c> treat a POCO
/// property mapped to an AutoNumber column. A non-nullable value-type property left at
/// its CLR default (<c>0</c>) counts as not supplied, so the row gets the next AutoNumber,
/// as a <see langword="null"/> <c>int?</c> does; any other value is stored as given. This
/// is the convention Entity Framework uses for generated keys, and the only one under
/// which a POCO with an <c>int Id</c> can insert more than one row.
/// </summary>
public sealed class PocoAutoNumberTests
{
    private const string Table = "A";

    /// <summary>Gets every format in every write mode: <c>plain</c>, <c>transactional</c> and <c>explicit</c>.</summary>
    public static TheoryData<DatabaseFormat, string> FormatsAndModes
    {
        get
        {
            var data = new TheoryData<DatabaseFormat, string>();
            foreach (DatabaseFormat format in Formats)
            {
                foreach (string mode in new[] { "plain", "transactional", "explicit" })
                {
                    data.Add(format, mode);
                }
            }

            return data;
        }
    }

    /// <summary>Gets every format.</summary>
    public static TheoryData<DatabaseFormat> FormatCases => [.. Formats];

    private static DatabaseFormat[] Formats => [DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb, DatabaseFormat.AceAccdb];

    /// <summary>
    /// Two inserts of a POCO whose <c>int Id</c> is left at 0 get two distinct generated
    /// Ids, in the writer that created the table and in a later one, with no transaction,
    /// with <see cref="AccessWriterOptions.UseTransactionalWrites"/> and in an explicit
    /// committed transaction. Both used to store 0.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode"><c>plain</c>, <c>transactional</c> or <c>explicit</c>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRowAsync_IntIdLeftAtZero_GeneratesNextAutoNumber(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, mode == "transactional", ct))
        {
            JetTransaction? tx = mode == "explicit" ? await writer.BeginTransactionAsync(ct) : null;
            await writer.InsertRowAsync(Table, new PocoA { Name = "a" }, ct);
            await writer.InsertRowAsync(Table, new PocoA { Name = "b" }, ct);
            if (tx is not null)
            {
                await tx.CommitAsync(ct);
                await tx.DisposeAsync();
            }
        }

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new PocoA { Name = "c" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
        Assert.Equal(3, ids["c"]);
        Assert.Equal(3, ids.Count);
    }

    /// <summary>
    /// A batch of POCOs whose <c>int Id</c> is left at 0 gets one generated Id per row,
    /// with no transaction, with <see cref="AccessWriterOptions.UseTransactionalWrites"/>
    /// and in an explicit committed transaction.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <param name="mode"><c>plain</c>, <c>transactional</c> or <c>explicit</c>.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task InsertRowsAsync_IntIdsLeftAtZero_GenerateOneAutoNumberEach(DatabaseFormat format, string mode)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, mode == "transactional", ct))
        {
            JetTransaction? tx = mode == "explicit" ? await writer.BeginTransactionAsync(ct) : null;
            int inserted = await writer.InsertRowsAsync(
                Table,
                new List<PocoA> { new() { Name = "a" }, new() { Name = "b" }, new() { Name = "c" } },
                ct);
            Assert.Equal(3, inserted);
            if (tx is not null)
            {
                await tx.CommitAsync(ct);
                await tx.DisposeAsync();
            }
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
        Assert.Equal(3, ids["c"]);
    }

    /// <summary>
    /// An <c>int Id</c> set to a value other than 0 is stored as given, beside a row whose
    /// Id is left at 0 and generated.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task InsertRowAsync_IntIdSet_IsStoredAsGiven(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new PocoA { Name = "generated" }, ct);
            await writer.InsertRowAsync(Table, new PocoA { Id = 42, Name = "given" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["generated"]);
        Assert.Equal(42, ids["given"]);
    }

    /// <summary>
    /// An <c>int?</c> Id generates for <see langword="null"/> and stores a value as given,
    /// beside a POCO whose <c>int Id</c> is left at 0, which generates too.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task InsertRowAsync_NullableIntId_GeneratesForNullAndStoresValue(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new PocoA { Name = "zero" }, ct);
            await writer.InsertRowAsync(Table, new NullableIdPoco { Id = null, Name = "null" }, ct);
            await writer.InsertRowAsync(Table, new NullableIdPoco { Id = 7, Name = "seven" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["zero"]);
        Assert.Equal(2, ids["null"]);
        Assert.Equal(7, ids["seven"]);
    }

    /// <summary>
    /// A <c>long</c> property mapped to a Long Integer AutoNumber column, left at 0, also
    /// generates: the property's CLR default counts, whatever the column's type.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task InsertRowAsync_LongIdLeftAtZero_GeneratesNextAutoNumber(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new LongIdPoco { Name = "a" }, ct);
            await writer.InsertRowAsync(Table, new LongIdPoco { Name = "b" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["a"]);
        Assert.Equal(2, ids["b"]);
    }

    /// <summary>
    /// On an Integer AutoNumber column, a <c>short</c> property and an enum property left
    /// at 0 generate too, as an <c>int</c> does on a Long Integer one.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task InsertRowAsync_ShortAndEnumIdsLeftAtZero_GenerateOnIntegerAutoNumber(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(short), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new ShortIdPoco { Name = "short" }, ct);
            await writer.InsertRowAsync(Table, new EnumIdPoco { Name = "enum" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(1, ids["short"]);
        Assert.Equal(2, ids["enum"]);
    }

    /// <summary>
    /// A row inserted as <see cref="RowValues"/> or <c>object[]</c> stores an explicit 0 in
    /// the AutoNumber column, which a POCO left at 0 cannot, and a later POCO still gets
    /// the next generated value.
    /// </summary>
    /// <param name="format">The database format.</param>
    /// <returns>A <see cref="Task"/> representing the asynchronous test.</returns>
    [Theory]
    [MemberData(nameof(FormatCases))]
    public async Task InsertRowAsync_ExplicitZeroAsRowValuesOrArray_IsStored(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = await CreateTableAsync(format, typeof(int), ct);

        await using (AccessWriter writer = await OpenWriterAsync(stream, transactional: false, ct))
        {
            await writer.InsertRowAsync(Table, new RowValues { ["Id"] = 0, ["Name"] = "values" }, ct);
            await writer.InsertRowAsync(Table, [0, "array"], ct);
            await writer.InsertRowAsync(Table, new PocoA { Name = "poco" }, ct);
        }

        Dictionary<string, long> ids = await ReadIdsByNameAsync(stream, ct);
        Assert.Equal(0, ids["values"]);
        Assert.Equal(0, ids["array"]);
        Assert.Equal(1, ids["poco"]);
    }

    /// <summary>Creates a database holding table <c>A</c>: <c>Id</c> (AutoNumber) and <c>Name</c>.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="idType">The CLR type of the <c>Id</c> column: <c>int</c> (Long Integer) or <c>short</c> (Integer).</param>
    /// <param name="ct">A token used to cancel the operation.</param>
    private static async Task<MemoryStream> CreateTableAsync(DatabaseFormat format, Type idType, CancellationToken ct)
    {
        var ms = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            ms, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync(
                Table,
                [
                    new("Id", idType) { IsAutoIncrement = true, IsNullable = false },
                    new("Name", typeof(string), maxLength: 50),
                ],
                ct);
        }

        return ms;
    }

    private static async Task<Dictionary<string, long>> ReadIdsByNameAsync(MemoryStream stream, CancellationToken ct)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(
            stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, ct);
        var ids = new Dictionary<string, long>(StringComparer.Ordinal);
        await foreach (object[] row in reader.Rows(Table, cancellationToken: ct))
        {
            ids.Add((string)row[1], Convert.ToInt64(row[0], CultureInfo.InvariantCulture));
        }

        return ids;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, bool transactional, CancellationToken ct)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { UseLockFile = false, UseTransactionalWrites = transactional },
            leaveOpen: true,
            ct);
    }

    private enum Key
    {
        None = 0,
        First = 1,
    }

    private sealed class PocoA
    {
        public int Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class NullableIdPoco
    {
        public int? Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class LongIdPoco
    {
        public long Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class ShortIdPoco
    {
        public short Id { get; set; }

        public string? Name { get; set; }
    }

    private sealed class EnumIdPoco
    {
        public Key Id { get; set; }

        public string? Name { get; set; }
    }
}
