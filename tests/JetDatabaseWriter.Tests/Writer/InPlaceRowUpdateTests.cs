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
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
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
}
