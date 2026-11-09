namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Small non-key edits retain physical identity, long values and index pages.</summary>
public sealed class InPlaceRowUpdateTests
{
    /// <summary>An indexed non-key update must refuse payload ownership shared by two live slots.</summary>
    /// <param name="format">The native file generation.</param>
    /// <param name="wider">Whether replacement requires row relocation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task IndexedUpdate_RefusesLivePayloadAliasBeforeChangingBytes(DatabaseFormat format, bool wider)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        RowLocation first;
        RowLocation second;
        JetFormat profile;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 80)], ct);
            await creator.InsertRowsAsync("Items", [[1, "first"], [2, "other"]], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow[] rows = [.. await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)];
            first = rows[0].Location;
            second = rows[1].Location;
            Assert.Equal(first.PageNumber, second.PageNumber);
            profile = creator.Database.Format;
        }

        byte[] bytes = seed.ToArray();
        int slot = checked((int)(first.PageNumber * profile.PageSize)) + profile.DataPage.RowsStart + (second.RowIndex * sizeof(ushort));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(slot, sizeof(ushort)), checked((ushort)first.RowStart));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using var trace = new CountingStream(stream);
        await using AccessWriter writer = await AccessWriter.OpenAsync(trace, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        trace.Reset();

        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(() => writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = wider ? "a much wider replacement" : "third" }, ct).AsTask());

        Assert.Equal(bytes, stream.ToArray());
        Assert.Equal(0, trace.BytesWritten);
        Assert.Equal(JetErrorCode.MalformedValue, failure.ErrorCode);
        Assert.Equal(first.PageNumber, failure.ErrorInfo.PageNumber);
        Assert.Contains("duplicates live row slot", failure.ErrorInfo.Reason, StringComparison.Ordinal);
    }

    /// <summary>A deleted alias keeps the native relocation fallback available for a uniquely owned live row.</summary>
    /// <param name="format">The native file generation.</param>
    /// <param name="wider">Whether replacement requires row relocation independently of the alias.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task IndexedUpdate_WithDeletedPayloadAlias_RelocatesAndPreservesOtherLiveRows(DatabaseFormat format, bool wider)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        RowLocation first;
        JetFormat profile;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 80)], ct);
            await creator.InsertRowsAsync("Items", [[1, "first"], [2, "other"]], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            first = (await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct))[0].Location;
            profile = creator.Database.Format;
        }

        byte[] bytes = seed.ToArray();
        int page = checked((int)(first.PageNumber * profile.PageSize));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + profile.DataPage.NumRows, sizeof(ushort)), 3);
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + profile.DataPage.RowsStart + (2 * sizeof(ushort)), sizeof(ushort)), checked((ushort)(first.RowStart | Constants.DataPage.DeletedRowFlag)));
        int free = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(page + 2, sizeof(ushort)));
        BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(page + 2, sizeof(ushort)), checked((ushort)(free - sizeof(ushort))));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);

        string replacement = wider ? "a much wider replacement" : "third";
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = replacement }, ct));

        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        List<LocatedRow> changed = await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct);
        Assert.Equal(2, changed.Count);
        Assert.Equal(replacement, Assert.Single(changed, row => Equals(row.Values[0], 1)).Values[1]);
        Assert.Equal("other", Assert.Single(changed, row => Equals(row.Values[0], 2)).Values[1]);
    }

    /// <summary>A scalar edit on a related row retains every long-value and index page.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ScalarUpdate_WithForeignKeyAndLongValue_ChangesOnlyTheExistingDataPage(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Parents", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
        await writer.CreateTableAsync("Children", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Parent", typeof(int)), new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Note", typeof(string))], ct);
        await writer.CreateRelationshipAsync(new RelationshipDefinition("FK", "Parents", "Id", "Children", "Parent"), ct);
        await writer.InsertRowAsync("Parents", [1], ct);
        string memo = new('m', 12_000);
        await writer.InsertRowAsync("Children", [1, 1, 5, memo], ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Children", ct);
        Assert.NotNull(table);
        LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        byte[] before = stream.ToArray();

        Assert.Equal(1, await writer.UpdateRowsAsync("Children", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = 9 }, ct));

        LocatedRow changed = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        Assert.Equal(original.Location, changed.Location);
        Assert.Equal(9, changed.Values[2]);
        Assert.Equal(memo, changed.Values[3]);
        Assert.NotNull(original.LongValues);
        Assert.NotNull(changed.LongValues);
        Assert.Equal(original.LongValues[3], changed.LongValues[3]);
        byte[] after = stream.ToArray();
        Assert.Equal(before.Length, after.Length);
        int size = writer.PageSize;
        for (int offset = 0; offset < before.Length; offset += size)
        {
            if (offset / size != original.Location.DataPageNumber)
            {
                Assert.True(before.AsSpan(offset, size).SequenceEqual(after.AsSpan(offset, size)), $"Unrelated page {offset / size} changed during a non-key scalar update.");
            }
        }
    }

    /// <summary>Compatible text, null and Boolean changes retain slots and exact rollback.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task SameWidthAndNullEdits_RetainPhysicalRowsAndRollbackExactly(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 40), new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Enabled", typeof(bool))], ct);
        await writer.InsertRowsAsync("Items", [[1, "first", 3, true], [2, "other", DBNull.Value, false]], ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        LocatedRow[] original = [.. await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)];
        byte[] before = stream.ToArray();
        await using (JetTransaction tx = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "third", ["Score"] = DBNull.Value, ["Enabled"] = false }, ct));
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 2), new RowValues { ["Score"] = 7, ["Enabled"] = true }, ct));
            LocatedRow[] changed = [.. await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)];
            Assert.Equal(original.Select(row => row.Location), changed.Select(row => row.Location));
            Assert.Equal("third", changed[0].Values[1]);
            Assert.Equal(DBNull.Value, changed[0].Values[2]);
            Assert.Equal(false, changed[0].Values[3]);
            Assert.Equal(7, changed[1].Values[2]);
            Assert.Equal(true, changed[1].Values[3]);
            await tx.RollbackAsync(ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    /// <summary>Unassigned null slots retain their original hidden storage bytes.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ScalarUpdate_PreservesUnassignedNullPayloadBytes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int payloadOffset;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Spare", typeof(int))], ct);
            await creator.InsertRowAsync("Items", [1, 3, DBNull.Value], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow row = Assert.Single(await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
            payloadOffset = checked((int)(row.Location.PageNumber * creator.PageSize)) + row.Location.RowStart + creator.Database.Format.RowFields.NumCols + table.Definition.Columns[2].FixedOff;
        }

        byte[] bytes = seed.ToArray();
        bytes.AsSpan(payloadOffset, sizeof(int)).Fill(0x5B);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = 4 }, ct));
        Assert.True(stream.ToArray().AsSpan(payloadOffset, sizeof(int)).SequenceEqual(bytes.AsSpan(payloadOffset, sizeof(int))));
    }

    /// <summary>A changed-width replacement reuses unchanged native long-value descriptors.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task WiderTextReplacement_RetainsUnassignedLongValueStorage(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 80), new ColumnDefinition("Note", typeof(string))], ct);
        string memo = new('v', 12_000);
        await writer.InsertRowAsync("Items", [1, "short", memo], ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        LocatedRow before = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));

        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "a much wider label" }, ct));

        LocatedRow after = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        Assert.Equal("a much wider label", after.Values[1]);
        Assert.Equal(memo, after.Values[2]);
        Assert.NotNull(before.LongValues);
        Assert.NotNull(after.LongValues);
        Assert.Equal(before.LongValues[2], after.LongValues[2]);
    }

    /// <summary>Exact indexed updates and delete-target discovery avoid whole-table snapshots.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task PrimaryKeyTargets_AvoidWholeTableSnapshots(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var source = new MemoryStream();
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(source, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Note", typeof(string))], ct);
            await creator.InsertRowsAsync("Items", Enumerable.Range(1, 64).Select(id => new object[] { id, 0, new string('m', 12_000) }), ct);
        }

        await using var trace = new CountingStream(source);
        await using AccessWriter writer = await AccessWriter.OpenAsync(trace, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, ct);
        trace.Reset();
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1).And("Score", 0), new RowValues { ["Score"] = 4 }, ct));
        Assert.True(trace.BytesRead < source.Length / 3, $"Point update read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        trace.Reset();
        Assert.Equal(0, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1).And("Score", 0), new RowValues { ["Score"] = 5 }, ct));
        Assert.Equal(0, trace.BytesWritten);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        trace.Reset();
        List<LocatedRow>? targets = await writer.Services.Snapshots.TryReadIndexedRowsAsync(table, RowCriteria.Where("Id", 1).And("Score", 4), ct);
        Assert.NotNull(targets);
        Assert.Equal(1, Assert.Single(targets).Values[0]);
        Assert.True(trace.BytesRead < source.Length / 3, $"Delete-target discovery read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        trace.Reset();
        Assert.Equal(1, await writer.DeleteRowsAsync("Items", RowCriteria.Where("Id", 1).And("Score", 4), ct));
    }

    /// <summary>Unique scalar and composite keys select only their matching row in either index direction.</summary>
    /// <param name="format">The native file generation.</param>
    /// <param name="descending">Whether the GUID component is descending.</param>
    /// <param name="composite">Whether the index includes an integer component.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, false)]
    [InlineData(DatabaseFormat.AceAccdb, false, false)]
    [InlineData(DatabaseFormat.AceAccdb, true, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet3Mdb, true, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, true, true)]
    [InlineData(DatabaseFormat.AceAccdb, false, true)]
    [InlineData(DatabaseFormat.AceAccdb, true, true)]
    public async Task UniqueGuidAndCompositeTargets_AvoidWholeTableSnapshots(DatabaseFormat format, bool descending, bool composite)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var source = new MemoryStream();
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(source, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            var index = new IndexDefinition("Target", composite ? ["Part", "Token"] : ["Token"])
            {
                IsUnique = true,
                DescendingColumns = descending ? ["Token"] : [],
            };
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Part", typeof(int)), new ColumnDefinition("Token", typeof(Guid)), new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Note", typeof(string))], [index], ct);
            await creator.InsertRowsAsync("Items", Enumerable.Range(1, 64).Select(id => new object[] { id % 3, new Guid(id, 0, 0, new byte[8]), 0, new string('m', 12_000) }), ct);
        }

        await using var trace = new CountingStream(source);
        await using AccessWriter writer = await AccessWriter.OpenAsync(trace, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        var criteria = RowCriteria.Where("Token", new Guid(1, 0, 0, new byte[8]));
        if (composite)
        {
            criteria.And("Part", 1);
            Assert.Null(await writer.Services.Snapshots.TryReadIndexedRowsAsync(table, RowCriteria.Where("Token", new Guid(1, 0, 0, new byte[8])), ct));
        }

        trace.Reset();
        List<LocatedRow>? targets = await writer.Services.Snapshots.TryReadIndexedRowsAsync(table, criteria, ct);
        Assert.NotNull(targets);
        Assert.Equal(new Guid(1, 0, 0, new byte[8]), Assert.Single(targets).Values[1]);
        Assert.True(trace.BytesRead < source.Length / 3, $"Target discovery read {trace.BytesRead} bytes from a {source.Length}-byte database.");

        trace.Reset();
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", criteria.And("Score", 0), new RowValues { ["Score"] = 4 }, ct));
        Assert.True(trace.BytesRead < source.Length / 3, $"Point update read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        Assert.Equal(0, await writer.UpdateRowsAsync("Items", criteria, new RowValues { ["Score"] = 5 }, ct));
        Assert.Equal(1, await writer.DeleteRowsAsync("Items", RowCriteria.Where("Token", new Guid(1, 0, 0, new byte[8])).And("Part", 1), ct));
    }

    /// <summary>Null keys, coercions and earlier unsupported comparisons retain complete residual evaluation.</summary>
    [Fact]
    public async Task UniqueTargets_KeepUnsafeAndIncompleteCriteriaOnTheScanPath()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Token", typeof(Guid)), new ColumnDefinition("Name", typeof(string), maxLength: 30)], [new IndexDefinition("TokenKey", "Token") { IsUnique = true }], ct);
        Guid token = new(1, 0, 0, new byte[8]);
        await writer.InsertRowsAsync("Items", [[1, token, "first"], [2, DBNull.Value, "second"], [3, DBNull.Value, "third"]], ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        foreach (RowCriteria criteria in new[]
        {
            RowCriteria.Where("Token", null),
            RowCriteria.Where("Token", token.ToString()),
            RowCriteria.Where(ColumnPredicate.GreaterThan("Id", 0)).And("Token", token),
            RowCriteria.Where("Name", "first").And("Token", token),
        })
        {
            Assert.Null(await writer.Services.Snapshots.TryReadIndexedRowsAsync(table, criteria, ct));
        }

        Assert.Equal(2, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Token", null), new RowValues { ["Name"] = "missing" }, ct));
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Token", token.ToString()), new RowValues { ["Name"] = "coerced" }, ct));
    }

    /// <summary>Scan snapshots retain full stored values and descriptors only for matching rows without reading non-matching external LVAL chains.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MatchingSnapshots_RetainOnlySelectedRowsAndPreserveTheirLongValues(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var source = new MemoryStream();
        string memo = new('m', 12_000);
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(source, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Name", typeof(string), maxLength: 30), new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Note", typeof(string))], ct);
            await creator.InsertRowsAsync("Items", Enumerable.Range(1, 32).Select(id => new object[] { $"item{id}", 0, memo }), ct);
        }

        await using var trace = new CountingStream(source);
        await using AccessWriter writer = await AccessWriter.OpenAsync(trace, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, ct);
        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        RowCriteria criteria = RowCriteria.Where("Name", "item1").And("Note", memo);
        var predicate = RowCriteriaEvaluator.Compile(criteria, table.Definition, "Items", nameof(criteria));
        trace.Reset();
        List<LocatedRow> retained = await writer.Services.Snapshots.ReadMatchingRowsAsync(table, predicate, ct);
        Assert.True(trace.BytesRead < source.Length / 4, $"Filtered scan read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        LocatedRow selected = Assert.Single(retained);
        Assert.Equal("item1", selected.Values[0]);
        Assert.Equal(memo, selected.Values[2]);
        Assert.NotNull(selected.TextBytes);
        Assert.NotNull(selected.LongValues);
        trace.Reset();
        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Name", "item1"), new RowValues { ["Score"] = 4 }, ct));
        Assert.True(trace.BytesRead < source.Length / 4, $"Scan-fallback update read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        trace.Reset();
        Assert.Equal(0, await writer.DeleteRowsAsync("Items", RowCriteria.Where("Name", "missing"), ct));
        Assert.True(trace.BytesRead < source.Length / 4, $"Scan-fallback delete target discovery read {trace.BytesRead} bytes from a {source.Length}-byte database.");
        Assert.Equal(1, await writer.DeleteRowsAsync("Items", RowCriteria.Where("Name", "item1").And("Score", 4), ct));
        Assert.Empty(await writer.Services.Snapshots.ReadMatchingRowsAsync(table, predicate, ct));
    }

    /// <summary>Contradictory unique-index bookmarks refuse before any row or index byte changes.</summary>
    /// <param name="fault">The synthetic bookmark contradiction.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task IndexedMutation_RejectsWrongOwnerAbsentMismatchedAndDuplicateBookmarks(int fault)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        long root;
        long owner;
        long otherPage;
        JetFormat profile;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Score", typeof(int))], ct);
            await creator.CreateTableAsync("Other", [new ColumnDefinition("Id", typeof(int))], ct);
            await creator.InsertRowsAsync("Items", [[1, 3], [2, 4]], ct);
            await creator.InsertRowAsync("Other", [1], ct);
            ResolvedTable? items = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            ResolvedTable? other = await creator.Services.Snapshots.ResolveTableAsync("Other", ct);
            Assert.NotNull(items);
            Assert.NotNull(other);
            owner = items.Entry.TDefPage;
            otherPage = Assert.Single(await creator.Services.Snapshots.ReadRowsAsync(other.Entry.TDefPage, ct)).Location.PageNumber;
            root = Assert.Single(await creator.Services.Snapshots.ReadIndexMetadataSnapshotAsync("Items", ct)).FirstDp;
            profile = creator.Database.Format;
        }

        byte[] bytes = seed.ToArray();
        byte[] leaf = bytes.AsSpan(checked((int)(root * profile.PageSize)), profile.PageSize).ToArray();
        Assert.True(IndexPageCodec.TryDecodeLeafEntries(profile.IndexPage, leaf, profile.PageSize, out List<IndexEntry> entries));
        IndexEntry first = entries[0];
        entries[0] = fault switch
        {
            0 => first with { DataPage = otherPage },
            1 => first with { DataRow = 250 },
            2 => first with { DataRow = entries[1].DataRow },
            _ => first,
        };
        if (fault == 3)
        {
            entries[1] = entries[1] with { Key = first.Key };
        }

        byte[] malformed = IndexPageCodec.BuildLeafPage(profile.IndexPage, profile.PageSize, owner, entries, enablePrefixCompression: false);
        malformed.CopyTo(bytes, checked((int)(root * profile.PageSize)));
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        JetCorruptDataException failure = await Assert.ThrowsAsync<JetCorruptDataException>(() => writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = 8 }, ct).AsTask());
        Assert.Equal(JetErrorCode.CorruptIndex, failure.ErrorCode);
        Assert.Equal(bytes, stream.ToArray());
    }

    /// <summary>Earlier relocation cannot make a later same-page update use stale snapshot bounds.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task MixedSamePageUpdates_ResolveCurrentSlotAndPreserveHiddenBytesAndRollback(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int hiddenOffset;
        int hiddenRelative;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), maxLength: 80), new ColumnDefinition("Score", typeof(int)), new ColumnDefinition("Spare", typeof(int)), new ColumnDefinition("Note", typeof(string))], ct);
            await creator.InsertRowsAsync("Items", [[1, "a", 0, DBNull.Value, new string('x', 8_000)], [2, "replacement", 0, DBNull.Value, new string('y', 8_000)]], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow[] rows = [.. await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)];
            Assert.Equal(rows[0].Location.PageNumber, rows[1].Location.PageNumber);
            hiddenRelative = creator.Database.Format.RowFields.NumCols + table.Definition.Columns[3].FixedOff;
            hiddenOffset = checked((int)(rows[1].Location.PageNumber * creator.PageSize)) + rows[1].Location.RowStart + hiddenRelative;
        }

        byte[] bytes = seed.ToArray();
        bytes.AsSpan(hiddenOffset, sizeof(int)).Fill(0x5B);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), row => Equals(row.Values[0], 2));
        await using (JetTransaction transaction = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(2, await writer.UpdateRowsAsync("Items", RowCriteria.All(), new RowValues { ["Label"] = "replacement", ["Score"] = 7 }, ct));
            LocatedRow changed = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), row => Equals(row.Values[0], 2));
            Assert.Equal(original.Location.PageNumber, changed.Location.PageNumber);
            Assert.Equal(original.Location.RowIndex, changed.Location.RowIndex);
            Assert.NotEqual(original.Location.RowStart, changed.Location.RowStart);
            byte[] page = await writer.Database.Pages.ReadPageAsync(changed.Location.PageNumber, ct);
            try
            {
                Assert.True(page.AsSpan(changed.Location.RowStart + hiddenRelative, sizeof(int)).SequenceEqual(bytes.AsSpan(hiddenOffset, sizeof(int))));
            }
            finally
            {
                PageBuffers.Return(page);
            }

            Assert.NotNull(original.LongValues);
            Assert.NotNull(changed.LongValues);
            Assert.Equal(original.LongValues[4], changed.LongValues[4]);
            await transaction.RollbackAsync(ct);
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    /// <summary>Zero-width variable Null and empty transitions patch the row in place and roll back cleanly.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ZeroWidthVariableNullAndEmptyTransitions_PatchRowInPlaceAndRollback(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int hiddenOffset;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Label", typeof(string), maxLength: 40) { AllowZeroLength = true },
                    new ColumnDefinition("Payload", typeof(byte[]), maxLength: 32) { ForceVariableLengthStorage = true },
                    new ColumnDefinition("Spare", typeof(int)),
                    new ColumnDefinition("Note", typeof(string)),
                ],
                ct);
            await creator.InsertRowAsync("Items", [1, DBNull.Value, Array.Empty<byte>(), DBNull.Value, new string('z', 8_000)], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow stored = Assert.Single(await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
            hiddenOffset = checked((int)(stored.Location.PageNumber * creator.PageSize)) + stored.Location.RowStart + creator.Database.Format.RowFields.NumCols + table.Definition.Columns[3].FixedOff;
        }

        byte[] bytes = seed.ToArray();
        bytes.AsSpan(hiddenOffset, sizeof(int)).Fill(0x5B);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));

        await using (JetTransaction transaction = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = string.Empty, ["Payload"] = DBNull.Value }, ct));
            byte[] afterFirst = stream.ToArray();
            for (int offset = 0; offset < bytes.Length; offset += writer.PageSize)
            {
                if (offset / writer.PageSize != original.Location.PageNumber)
                {
                    Assert.True(bytes.AsSpan(offset, writer.PageSize).SequenceEqual(afterFirst.AsSpan(offset, writer.PageSize)));
                }
            }

            Assert.True(afterFirst.AsSpan(hiddenOffset, sizeof(int)).SequenceEqual(bytes.AsSpan(hiddenOffset, sizeof(int))));
            LocatedRow first = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));
            Assert.Equal(original.Location, first.Location);
            Assert.Equal(string.Empty, first.Values[1]);
            Assert.Equal(DBNull.Value, first.Values[2]);

            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = DBNull.Value, ["Payload"] = Array.Empty<byte>() }, ct));
            LocatedRow second = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));
            Assert.Equal(original.Location, second.Location);
            Assert.Equal(DBNull.Value, second.Values[1]);
            Assert.Empty(Assert.IsType<byte[]>(second.Values[2]));

            await transaction.RollbackAsync(ct);
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    /// <summary>Relocated row updates preserve unassigned Null fixed-slot bytes and raw variable/fixed slots.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RelocatedRowUpdate_PreservesUnassignedNullFixedPayloadAndRawSlots(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int hiddenRelative;
        int hiddenOffset;
        int doubleRelative;
        int doubleOffset;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Label", typeof(string), maxLength: 80),
                    new ColumnDefinition("Spare", typeof(int)),
                    new ColumnDefinition("Measure", typeof(double)),
                    new ColumnDefinition("Raw", typeof(byte[]), maxLength: 16) { ForceVariableLengthStorage = true },
                    new ColumnDefinition("Note", typeof(string)),
                ],
                ct);
            await creator.InsertRowAsync("Items", [1, "a", DBNull.Value, 0.0d, new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, new string('m', 8_000)], ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow stored = Assert.Single(await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
            hiddenRelative = creator.Database.Format.RowFields.NumCols + table.Definition.Columns[2].FixedOff;
            hiddenOffset = checked((int)(stored.Location.PageNumber * creator.PageSize)) + stored.Location.RowStart + hiddenRelative;
            doubleRelative = creator.Database.Format.RowFields.NumCols + table.Definition.Columns[3].FixedOff;
            doubleOffset = checked((int)(stored.Location.PageNumber * creator.PageSize)) + stored.Location.RowStart + doubleRelative;
        }

        byte[] bytes = seed.ToArray();
        bytes.AsSpan(hiddenOffset, sizeof(int)).Fill(0x5B);
        byte[] negativeZeroBytes = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleLittleEndian(negativeZeroBytes, -0.0d);
        negativeZeroBytes.CopyTo(bytes.AsSpan(doubleOffset, sizeof(double)));

        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));

        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "relocated-row-label" }, ct));
        LocatedRow relocated = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct));
        Assert.NotEqual(original.Location.RowSize, relocated.Location.RowSize);
        Assert.Equal("relocated-row-label", relocated.Values[1]);
        Assert.Equal(DBNull.Value, relocated.Values[2]);
        Assert.Equal(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF }, Assert.IsType<byte[]>(relocated.Values[4]));
        Assert.NotNull(original.LongValues);
        Assert.NotNull(relocated.LongValues);
        Assert.Equal(original.LongValues[5], relocated.LongValues[5]);

        byte[] page = await writer.Database.Pages.ReadPageAsync(relocated.Location.PageNumber, ct);
        try
        {
            Assert.True(page.AsSpan(relocated.Location.RowStart + hiddenRelative, sizeof(int)).SequenceEqual(bytes.AsSpan(hiddenOffset, sizeof(int))));
            Assert.True(page.AsSpan(relocated.Location.RowStart + doubleRelative, sizeof(double)).SequenceEqual(negativeZeroBytes));
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>Point updates of non-unique indexed columns seek the target row and leave unaffected index trees and TDEF metadata untouched.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task PointUpdateOfNonUniqueIndexedColumn_SeeksTargetAndLeavesPrimaryKeyIndexUntouched(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync(
            "Items",
            [
                new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("Score", typeof(int)),
                new ColumnDefinition("OptionalCode", typeof(int)),
                new ColumnDefinition("Filler", typeof(string), maxLength: 200),
            ],
            ct);
        await writer.CreateIndexAsync("Items", new IndexDefinition("IX_Score", ["Score"]) { IsUnique = false }, ct);
        await writer.CreateIndexAsync("Items", new IndexDefinition("IX_Optional", ["OptionalCode"]) { IgnoreNulls = true }, ct);
        await writer.CreateIndexAsync("Items", new IndexDefinition("IX_Filler", ["Filler"]) { IsUnique = false }, ct);
        for (int id = 1; id <= 40; id++)
        {
            await writer.InsertRowAsync("Items", [id, id * 10, DBNull.Value, $"{id:D4}-" + new string('f', 150)], ct);
        }

        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow targetBefore = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), row => Equals(row.Values[0], 1));
        byte[] before = stream.ToArray();

        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Score"] = 999 }, ct));
        byte[] after = stream.ToArray();

        var changedPages = new List<long>();
        for (int offset = 0; offset < before.Length; offset += writer.PageSize)
        {
            if (!before.AsSpan(offset, writer.PageSize).SequenceEqual(after.AsSpan(offset, writer.PageSize)))
            {
                changedPages.Add(offset / writer.PageSize);
            }
        }

        Assert.Equal(2, changedPages.Count);
        Assert.Contains(targetBefore.Location.PageNumber, changedPages);
        Assert.DoesNotContain(resolved.Entry.TDefPage, changedPages);

        LocatedRow updated = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), row => Equals(row.Values[1], 999));
        Assert.Equal(1, updated.Values[0]);
        Assert.Equal(999, updated.Values[1]);
    }

    /// <summary>Cascading fixed-key updates patch child rows in place while preserving LVAL descriptors and hidden bytes.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CascadeUpdate_FixedKey_PatchesChildRowInPlaceAndPreservesLongValuesAndHiddenBytes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int hiddenOffset;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync("Parents", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], ct);
            await creator.CreateTableAsync(
                "Children",
                [
                    new ColumnDefinition("ChildId", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("ParentId", typeof(int)),
                    new ColumnDefinition("Spare", typeof(int)),
                    new ColumnDefinition("Note", typeof(string)),
                ],
                ct);
            await creator.CreateRelationshipAsync(
                new RelationshipDefinition("FK_Children_Parents", "Parents", ["Id"], "Children", ["ParentId"]) { EnforceReferentialIntegrity = true, CascadeUpdates = true },
                ct);
            await creator.InsertRowAsync("Parents", [10], ct);
            await creator.InsertRowAsync("Children", [1, 10, DBNull.Value, new string('c', 8_000)], ct);

            ResolvedTable? childTable = await creator.Services.Snapshots.ResolveTableAsync("Children", ct);
            Assert.NotNull(childTable);
            LocatedRow storedChild = Assert.Single(await creator.Services.Snapshots.ReadRowsAsync(childTable.Entry.TDefPage, ct));
            hiddenOffset = checked((int)(storedChild.Location.PageNumber * creator.PageSize)) + storedChild.Location.RowStart + creator.Database.Format.RowFields.NumCols + childTable.Definition.Columns[2].FixedOff;
        }

        byte[] bytes = seed.ToArray();
        bytes.AsSpan(hiddenOffset, sizeof(int)).Fill(0x5B);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        ResolvedTable? resolvedChild = await writer.Services.Snapshots.ResolveTableAsync("Children", ct);
        Assert.NotNull(resolvedChild);
        LocatedRow beforeChild = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolvedChild.Entry.TDefPage, ct));

        Assert.Equal(1, await writer.UpdateRowsAsync("Parents", RowCriteria.Where("Id", 10), new RowValues { ["Id"] = 20 }, ct));
        byte[] afterBytes = stream.ToArray();
        Assert.Equal(bytes.Length, afterBytes.Length);
        Assert.True(afterBytes.AsSpan(hiddenOffset, sizeof(int)).SequenceEqual(bytes.AsSpan(hiddenOffset, sizeof(int))));

        LocatedRow afterChild = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolvedChild.Entry.TDefPage, ct));
        Assert.Equal(beforeChild.Location, afterChild.Location);
        Assert.Equal(20, afterChild.Values[1]);
        Assert.NotNull(beforeChild.LongValues);
        Assert.NotNull(afterChild.LongValues);
        Assert.Equal(beforeChild.LongValues[3], afterChild.LongValues[3]);
    }

    /// <summary>Relocated row updates with multiple long-value columns preserve unchanged descriptors while reclaiming replaced unshared LVAL chains.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RelocatedRowUpdate_WithMixedLongValueColumns_PreservesUnchangedDescriptorAndReclaimsReplacedChain(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
        };
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct);
        await writer.CreateTableAsync(
            "Items",
            [
                new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("Label", typeof(string), maxLength: 80),
                new ColumnDefinition("MemoA", typeof(string)),
                new ColumnDefinition("MemoB", typeof(string)),
            ],
            ct);
        string memoA = new('a', 12_000);
        string memoB = new('b', 12_000);
        await writer.InsertRowAsync("Items", [1, "short", memoA, memoB], ct);

        ResolvedTable? table = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(table);
        LocatedRow before = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        Assert.NotNull(before.LongValues);
        Assert.True(LongValueDescriptor.TryRead(before.LongValues[2], out LongValueDescriptor oldMemoADescriptor));
        Assert.True(LongValueDescriptor.TryRead(before.LongValues[3], out LongValueDescriptor oldMemoBDescriptor));
        byte[] snapshotBefore = stream.ToArray();

        await using (JetTransaction tx = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "a much wider relocated label", ["MemoA"] = "inline-replacement" }, ct));
            await tx.RollbackAsync(ct);
        }

        Assert.Equal(snapshotBefore, stream.ToArray());

        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "a much wider relocated label", ["MemoA"] = "inline-replacement" }, ct));
        LocatedRow after = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct));
        Assert.Equal("a much wider relocated label", after.Values[1]);
        Assert.Equal("inline-replacement", after.Values[2]);
        Assert.Equal(memoB, after.Values[3]);
        Assert.NotNull(after.LongValues);
        Assert.True(LongValueDescriptor.TryRead(after.LongValues[2], out LongValueDescriptor newMemoADescriptor));
        Assert.True(newMemoADescriptor.IsInline);
        Assert.Equal(before.LongValues[3], after.LongValues[3]);
        Assert.True(LongValueDescriptor.TryRead(after.LongValues[3], out LongValueDescriptor preservedMemoBDescriptor));
        Assert.Equal(oldMemoBDescriptor.FirstDp, preservedMemoBDescriptor.FirstDp);

        byte[] oldMemoAPage = await writer.Database.Pages.ReadPageCopyAsync(oldMemoADescriptor.FirstDp >> 8, ct);
        Assert.Equal(Constants.PageTypes.Freed, oldMemoAPage[0]);
    }

    /// <summary>Changed-width variable-length updates compact the DATA page in place, preserve slot indexes and shifted rows' hidden bytes, scrub vacated space under secure erase, and roll back byte-for-byte.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task ChangedWidthInPlaceUpdate_CompactsDataPagePreservesSlotIndexAndScrubsVacatedBytes(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var seed = new MemoryStream();
        int hiddenRelative;
        int row1HiddenOffset;
        await using (AccessWriter creator = await AccessWriter.CreateDatabaseAsync(seed, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct))
        {
            await creator.CreateTableAsync(
                "Items",
                [
                    new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                    new ColumnDefinition("Label", typeof(string), maxLength: 120),
                    new ColumnDefinition("Spare", typeof(int)),
                    new ColumnDefinition("Note", typeof(string)),
                ],
                ct);
            await creator.InsertRowsAsync(
                "Items",
                [
                    [1, "medium-initial-text", DBNull.Value, new string('m', 8_000)],
                    [2, "second-row-text", DBNull.Value, new string('n', 8_000)],
                ],
                ct);
            ResolvedTable? table = await creator.Services.Snapshots.ResolveTableAsync("Items", ct);
            Assert.NotNull(table);
            LocatedRow[] initialRows = [.. await creator.Services.Snapshots.ReadRowsAsync(table.Entry.TDefPage, ct)];
            Assert.Equal(initialRows[0].Location.PageNumber, initialRows[1].Location.PageNumber);
            hiddenRelative = creator.Database.Format.RowFields.NumCols + table.Definition.Columns[2].FixedOff;
            row1HiddenOffset = checked((int)(initialRows[1].Location.PageNumber * creator.PageSize)) + initialRows[1].Location.RowStart + hiddenRelative;
        }

        byte[] seededBytes = seed.ToArray();
        seededBytes.AsSpan(row1HiddenOffset, sizeof(int)).Fill(0x5B);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(seededBytes, ct);
        stream.Position = 0;

        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
        };
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, options, leaveOpen: true, ct);
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow[] before = [.. await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct)];

        await using (JetTransaction tx = await writer.BeginTransactionAsync(ct))
        {
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = new string('w', 90) }, ct));
            LocatedRow[] grown = [.. await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct)];
            Assert.Equal(before[0].Location.PageNumber, grown[0].Location.PageNumber);
            Assert.Equal(before[0].Location.RowIndex, grown[0].Location.RowIndex);
            Assert.True(grown[0].Location.RowSize > before[0].Location.RowSize);
            Assert.Equal(before[1].Location.PageNumber, grown[1].Location.PageNumber);
            Assert.Equal(before[1].Location.RowIndex, grown[1].Location.RowIndex);
            Assert.True(grown[1].Location.RowStart < before[1].Location.RowStart);
            await tx.RollbackAsync(ct);
        }

        Assert.Equal(seededBytes, stream.ToArray());

        Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 1), new RowValues { ["Label"] = "z" }, ct));
        LocatedRow[] shrunk = [.. await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct)];
        Assert.Equal(before[0].Location.PageNumber, shrunk[0].Location.PageNumber);
        Assert.Equal(before[0].Location.RowIndex, shrunk[0].Location.RowIndex);
        Assert.True(shrunk[0].Location.RowSize < before[0].Location.RowSize);
        Assert.Equal(before[1].Location.PageNumber, shrunk[1].Location.PageNumber);
        Assert.Equal(before[1].Location.RowIndex, shrunk[1].Location.RowIndex);
        Assert.True(shrunk[1].Location.RowStart > before[1].Location.RowStart);

        byte[] page = await writer.Database.Pages.ReadPageAsync(shrunk[1].Location.PageNumber, ct);
        try
        {
            Assert.Equal(2, BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(writer.Database.Format.DataPage.NumRows, sizeof(ushort))));
            Assert.True(page.AsSpan(shrunk[1].Location.RowStart + hiddenRelative, sizeof(int)).SequenceEqual(seededBytes.AsSpan(row1HiddenOffset, sizeof(int))));
            int vacatedLength = shrunk[1].Location.RowStart - before[1].Location.RowStart;
            Assert.True(vacatedLength > 0);
            Assert.False(page.AsSpan(before[1].Location.RowStart, vacatedLength).ContainsAnyExcept((byte)0));
        }
        finally
        {
            PageBuffers.Return(page);
        }
    }

    /// <summary>Overflow rows support both same-width and changed-width in-place updates on their target payload page while preserving their 0x4000 header.</summary>
    /// <param name="format">The native file generation.</param>
    /// <param name="layout">The overflow pointer layout.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.Jet3Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.Jet4Mdb, OverflowRowLayout.TwoHop)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.CrossPage)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.SamePage)]
    [InlineData(DatabaseFormat.AceAccdb, OverflowRowLayout.TwoHop)]
    public async Task OverflowRow_SameWidthAndChangedWidthUpdates_PatchTargetSlotInPlaceAndPreserveHeader(DatabaseFormat format, OverflowRowLayout layout)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(format, "Items", 300, primaryKey: true, ct);
        SyntheticOverflowRow moved = await SyntheticOverflowRows.MoveRowAsync(bytes, "Items", layout, ct);

        await using var stream = new MemoryStream();
        await stream.WriteAsync(bytes, ct);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        ResolvedTable? resolved = await writer.Services.Snapshots.ResolveTableAsync("Items", ct);
        Assert.NotNull(resolved);
        LocatedRow overflowBefore = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), r => r.Location.IsOverflow);
        int movedId = Assert.IsType<int>(overflowBefore.Values[0]);
        string originalName = Assert.IsType<string>(overflowBefore.Values[1]);

        await using (JetTransaction tx = await writer.BeginTransactionAsync(ct))
        {
            string sameLengthName = new('s', originalName.Length);
            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", movedId), new RowValues { ["Name"] = sameLengthName }, ct));
            LocatedRow afterSameWidth = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), r => Equals(r.Values[0], movedId));
            Assert.True(afterSameWidth.Location.IsOverflow);
            Assert.Equal(moved.HeaderPage, afterSameWidth.Location.PageNumber);
            Assert.Equal(moved.HeaderRow, afterSameWidth.Location.RowIndex);
            Assert.Equal(moved.DataPage, afterSameWidth.Location.DataPageNumber);
            Assert.Equal(moved.DataRow, afterSameWidth.Location.DataRowIndex);
            Assert.Equal(sameLengthName, afterSameWidth.Values[1]);

            Assert.Equal(1, await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", movedId), new RowValues { ["Name"] = "compact-overflow-name" }, ct));
            LocatedRow afterChangedWidth = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(resolved.Entry.TDefPage, ct), r => Equals(r.Values[0], movedId));
            Assert.True(afterChangedWidth.Location.IsOverflow);
            Assert.Equal(moved.HeaderPage, afterChangedWidth.Location.PageNumber);
            Assert.Equal(moved.HeaderRow, afterChangedWidth.Location.RowIndex);
            Assert.Equal(moved.DataPage, afterChangedWidth.Location.DataPageNumber);
            Assert.Equal(moved.DataRow, afterChangedWidth.Location.DataRowIndex);
            Assert.Equal("compact-overflow-name", afterChangedWidth.Values[1]);

            await tx.RollbackAsync(ct);
        }

        Assert.Equal(bytes, stream.ToArray());
    }

    /// <summary>Multi-row batch UpdateRowsAsync atomically swaps unique keys, primary keys with child cascades, and self-referencing keys, and refuses post-update duplicates cleanly.</summary>
    /// <param name="format">The native file generation.</param>
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task BatchUpdateRows_SwapsUniqueAndCascadedPrimaryKeysAtomicallyAndRefusesDuplicates(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, ct);
        await writer.CreateTableAsync(
            "Nodes",
            [
                new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("Code", typeof(string), maxLength: 20),
                new ColumnDefinition("ParentId", typeof(int)),
            ],
            [new IndexDefinition("UX_Nodes_Code", ["Code"]) { IsUnique = true }],
            ct);
        await writer.CreateTableAsync(
            "Leaves",
            [
                new ColumnDefinition("LeafId", typeof(int)) { IsPrimaryKey = true },
                new ColumnDefinition("NodeId", typeof(int)),
            ],
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Nodes_Self", "Nodes", ["Id"], "Nodes", ["ParentId"]) { EnforceReferentialIntegrity = true, CascadeUpdates = true },
            ct);
        await writer.CreateRelationshipAsync(
            new RelationshipDefinition("FK_Leaves_Nodes", "Nodes", ["Id"], "Leaves", ["NodeId"]) { EnforceReferentialIntegrity = true, CascadeUpdates = true },
            ct);

        await writer.InsertRowsAsync("Nodes", [[1, "ALPHA", DBNull.Value], [2, "BETA", 1], [3, "GAMMA", 1]], ct);
        Assert.Equal(1, await writer.UpdateRowsAsync("Nodes", RowCriteria.Where("Id", 1), new RowValues { ["ParentId"] = 2 }, ct));
        await writer.InsertRowsAsync("Leaves", [[100, 1], [200, 2]], ct);

        ResolvedTable? nodesTable = await writer.Services.Snapshots.ResolveTableAsync("Nodes", ct);
        Assert.NotNull(nodesTable);
        LocatedRow[] beforeLocations = [.. await writer.Services.Snapshots.ReadRowsAsync(nodesTable.Entry.TDefPage, ct)];

        int updated = await writer.UpdateRowsAsync(
            "Nodes",
            [
                new RowUpdate(RowCriteria.Where("Id", 1), new RowValues { ["Code"] = "BETA" }),
                new RowUpdate(RowCriteria.Where("Id", 2), new RowValues { ["Code"] = "GAMMA" }),
                new RowUpdate(RowCriteria.Where("Id", 3), new RowValues { ["Code"] = "ALPHA" }),
            ],
            ct);
        Assert.Equal(3, updated);

        LocatedRow[] afterCodeSwap = [.. await writer.Services.Snapshots.ReadRowsAsync(nodesTable.Entry.TDefPage, ct)];
        Assert.Equal(beforeLocations.Select(r => r.Location.PageNumber), afterCodeSwap.Select(r => r.Location.PageNumber));
        Assert.Equal(beforeLocations.Select(r => r.Location.RowIndex), afterCodeSwap.Select(r => r.Location.RowIndex));
        Assert.Equal("BETA", afterCodeSwap.Single(r => Equals(r.Values[0], 1)).Values[1]);
        Assert.Equal("GAMMA", afterCodeSwap.Single(r => Equals(r.Values[0], 2)).Values[1]);
        Assert.Equal("ALPHA", afterCodeSwap.Single(r => Equals(r.Values[0], 3)).Values[1]);

        byte[] snapshotBeforeDuplicate = stream.ToArray();
        _ = await Assert.ThrowsAsync<JetConstraintException>(() => writer.UpdateRowsAsync(
            "Nodes",
            [
                new RowUpdate(RowCriteria.Where("Id", 1), new RowValues { ["Code"] = "DUPLICATE" }),
                new RowUpdate(RowCriteria.Where("Id", 2), new RowValues { ["Code"] = "DUPLICATE" }),
            ],
            ct).AsTask());
        Assert.Equal(snapshotBeforeDuplicate, stream.ToArray());

        int pkSwapped = await writer.UpdateRowsAsync(
            "Nodes",
            [
                new RowUpdate(RowCriteria.Where("Id", 1), new RowValues { ["Id"] = 2 }),
                new RowUpdate(RowCriteria.Where("Id", 2), new RowValues { ["Id"] = 1 }),
            ],
            ct);
        Assert.Equal(2, pkSwapped);

        LocatedRow[] afterPkSwap = [.. await writer.Services.Snapshots.ReadRowsAsync(nodesTable.Entry.TDefPage, ct)];
        LocatedRow node1 = afterPkSwap.Single(r => Equals(r.Values[0], 1));
        LocatedRow node2 = afterPkSwap.Single(r => Equals(r.Values[0], 2));
        LocatedRow node3 = afterPkSwap.Single(r => Equals(r.Values[0], 3));
        Assert.Equal("GAMMA", node1.Values[1]);
        Assert.Equal(2, node1.Values[2]);
        Assert.Equal("BETA", node2.Values[1]);
        Assert.Equal(1, node2.Values[2]);
        Assert.Equal(2, node3.Values[2]);

        ResolvedTable? leavesTable = await writer.Services.Snapshots.ResolveTableAsync("Leaves", ct);
        Assert.NotNull(leavesTable);
        LocatedRow[] leaves = [.. await writer.Services.Snapshots.ReadRowsAsync(leavesTable.Entry.TDefPage, ct)];
        Assert.Equal(2, leaves.Single(r => Equals(r.Values[0], 100)).Values[1]);
        Assert.Equal(1, leaves.Single(r => Equals(r.Values[0], 200)).Values[1]);
    }
}
