namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues.Models;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tables;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using JetDatabaseWriter.ValueDecoding.Models;
using Xunit;

/// <summary>Dropping a link removes only its local catalog identity and permissions.</summary>
public sealed class DropLinkedTableTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every linked-table kind, database format and write mode.</summary>
    public static TheoryData<DatabaseFormat, WriteMode, LinkedTableKind> Cases()
    {
        var cases = new TheoryData<DatabaseFormat, WriteMode, LinkedTableKind>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                foreach (LinkedTableKind kind in Enum.GetValues<LinkedTableKind>())
                {
                    cases.Add(format, mode, kind);
                }
            }
        }

        return cases;
    }

    /// <summary>Gets each database format and linked-table kind for recovery checks.</summary>
    public static TheoryData<DatabaseFormat, LinkedTableKind> RecoveryCases()
    {
        var cases = new TheoryData<DatabaseFormat, LinkedTableKind>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (LinkedTableKind kind in Enum.GetValues<LinkedTableKind>())
            {
                cases.Add(format, kind);
            }
        }

        return cases;
    }

    /// <summary>Gets native Access and cached ODBC links in every write mode.</summary>
    public static TheoryData<string, WriteMode> NativeCases()
    {
        var cases = new TheoryData<string, WriteMode>();
        foreach (string fixture in new[] { TestDatabases.LinkerTestV2007, TestDatabases.OdbcLinkerTestV2007 })
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                cases.Add(fixture, mode);
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task DropLink_PreservesOtherObjectsAndNeverAcquiresSource(DatabaseFormat format, WriteMode mode, LinkedTableKind kind)
    {
        await using MemoryStream stream = await SeedAsync(format);
        Snapshot expected = await ReadAsync(stream, [kind.ToString()]);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.DropTableAsync(kind.ToString().ToLowerInvariant(), Ct), Ct);
            await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropTableAsync(kind.ToString(), Ct));
        }

        Snapshot actual = await ReadAsync(stream, []);
        Assert.Equal(expected.Catalog, actual.Catalog);
        Assert.Equal(expected.Permissions, actual.Permissions);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IReadOnlyList<LinkedTableInfo> links = await reader.ListLinkedTablesAsync(Ct);
        Assert.Equal(2, links.Count);
        Assert.DoesNotContain(links, link => link.Kind == kind);
        using DataTable kept = await reader.ReadTableAsync("Keep", cancellationToken: Ct);
        Assert.Equal([7, "unchanged"], Assert.Single(kept.AsEnumerable()).ItemArray);
    }

    [Theory]
    [MemberData(nameof(RecoveryCases))]
    public async Task DropLink_RollbackRestoresEveryByteAndSameWriterCanRetry(DatabaseFormat format, LinkedTableKind kind)
    {
        await using MemoryStream stream = await SeedAsync(format);
        byte[] original = stream.ToArray();
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.DropTableAsync(kind.ToString(), Ct);
            await transaction.RollbackAsync(Ct);
            Assert.Equal(original, stream.ToArray());
            await writer.DropTableAsync(kind.ToString(), Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.DoesNotContain(await reader.ListLinkedTablesAsync(Ct), link => link.Kind == kind);
    }

    [Theory]
    [MemberData(nameof(RecoveryCases))]
    public async Task DropLink_PartialWriteFailuresRestoreCatalogPermissionsAndAllowRetry(DatabaseFormat format, LinkedTableKind kind)
    {
        await using MemoryStream seed = await SeedAsync(format);
        byte[] original = seed.ToArray();
        int writes;
        await using (WriteFaultStream baseline = Copy(original))
        {
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(baseline, new() { UseLockFile = false, PageCacheSize = 0 }, cancellationToken: Ct);
            int start = baseline.WriteCount;
            await writer.DropTableAsync(kind.ToString(), Ct);
            writes = baseline.WriteCount - start;
        }

        Assert.True(writes > 0);
        for (int point = 1; point <= writes; point++)
        {
            await using WriteFaultStream stream = Copy(original);
            await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, new() { UseLockFile = false, PageCacheSize = 0 }, cancellationToken: Ct);
            stream.FailDuringWrite(point);
            await Assert.ThrowsAnyAsync<IOException>(async () => await writer.DropTableAsync(kind.ToString(), Ct));
            Assert.True(stream.Faulted);
            Assert.Equal(original, stream.ToArray());
            await writer.DropTableAsync(kind.ToString(), Ct);
        }
    }

    [Theory]
    [MemberData(nameof(NativeCases))]
    public async Task NativeLinks_PreserveUnrelatedCatalogAndPermissions(string fixture, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await stream.WriteAsync(await File.ReadAllBytesAsync(fixture, Ct), Ct);
        stream.Position = 0;
        string[] names;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            names = (await reader.ListLinkedTablesAsync(Ct)).Select(link => link.Name).ToArray();
            Assert.NotEmpty(names);
        }

        string[] removedObjects;
        stream.Position = 0;
        await using (AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct))
        {
            string[] relationships = (await reader.ListRelationshipsAsync(Ct))
                .Where(relationship => names.Contains(relationship.PrimaryTable, StringComparer.OrdinalIgnoreCase)
                    || names.Contains(relationship.ForeignTable, StringComparer.OrdinalIgnoreCase))
                .Select(relationship => relationship.Name).ToArray();
            removedObjects = [.. names, .. relationships];
        }

        Snapshot expected = await ReadAsync(stream, removedObjects);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(
                writer,
                mode,
                async () =>
                {
                    foreach (string name in names)
                    {
                        await writer.DropTableAsync(name, Ct);
                    }
                },
                Ct);
        }

        Snapshot actual = await ReadAsync(stream, []);
        Assert.Equal(expected.Catalog, actual.Catalog);
        Assert.Equal(expected.Permissions, actual.Permissions);
        stream.Position = 0;
        await using AccessReader actualReader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Empty(await actualReader.ListLinkedTablesAsync(Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropLink_RetainsOtherObjectsSharedLongValueTailUnderSecureErase(DatabaseFormat format)
    {
        await using MemoryStream stream = await SeedAsync(format);
        byte[] payload = Enumerable.Range(0, 5000).Select(index => unchecked((byte)index)).ToArray();
        byte[] expectedPayload;
        LongValueSnapshot expected;
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            TableDef objects = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
            var plan = new MutationPlan();
            foreach (string name in new[] { "Odbc", "Keep" })
            {
                LocatedRow original = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(2, Ct), row => Equals(row.Values[objects.FindColumnIndex("Name")], name));
                object[] replacement = (object[])original.Values.Clone();
                replacement[objects.FindColumnIndex("RmtInfoLong")] = name == "Odbc" ? payload : new byte[128];
                plan.ForTable(2, objects, "MSysObjects", MutationPlan.IndexPolicy.CatalogRelocation).Update(original.Location, original.Values, replacement);
            }

            await writer.Services.Mutations.ApplyAsync(plan, Ct);
            LongValueSnapshot source = await ReadLongValueAsync(writer, "Odbc");
            Assert.True(LongValueDescriptor.TryRead(source.Field, out LongValueDescriptor descriptor));
            Assert.True(descriptor.UsesChainedPages);
            uint tail = BinaryPrimitives.ReadUInt32LittleEndian(source.Rows[0]);
            Assert.NotEqual(0u, tail);
            int firstLength = source.Rows[0].Length - sizeof(uint);
            expectedPayload = payload[firstLength..];
            (RowLocation location, ColumnSlice slice) = await FindLongValueAsync(writer, "Keep");
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(location.PageNumber, Ct);
            LongValueDescriptor.Chained(expectedPayload.Length, tail, 0x98765432).WriteTo(page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen));
            await writer.Pager.WritePageAsync(location.PageNumber, page, Ct);
            expected = await ReadLongValueAsync(writer, "Keep");
            Assert.NotEmpty(expected.Rows);
        }

        byte[] originalBytes = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, new() { UseLockFile = false, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages }, cancellationToken: Ct))
        {
            await using (JetTransaction transaction = await writer.BeginTransactionAsync(Ct))
            {
                await writer.DropTableAsync("Odbc", Ct);
                await transaction.RollbackAsync(Ct);
            }

            Assert.Equal(originalBytes, stream.ToArray());
            await writer.DropTableAsync("Odbc", Ct);
            LongValueSnapshot actual = await ReadLongValueAsync(writer, "Keep");
            Assert.Equal(expected.Field, actual.Field);
            Assert.Equal(expected.Rows, actual.Rows);
        }

        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        Assert.Equal(expectedPayload, Assert.IsType<byte[]>(Assert.Single(catalog.AsEnumerable(), row => Equals(row["Name"], "Keep"))["RmtInfoLong"]));
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task DropLink_RemovesUnenforcedRelationshipMetadataAndItsPermissions(DatabaseFormat format, WriteMode mode, LinkedTableKind kind)
    {
        await using MemoryStream stream = await SeedAsync(format);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await SeedRelationshipAsync(writer, kind.ToString(), enforced: false);
        }

        Snapshot expected = await ReadAsync(stream, [kind.ToString(), "LinkedRelationship"]);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, WriteModes.WriterOptions(mode), cancellationToken: Ct))
        {
            await WriteModes.RunAsync(writer, mode, async () => await writer.DropTableAsync(kind.ToString(), Ct), Ct);
        }

        Snapshot actual = await ReadAsync(stream, []);
        Assert.Equal(expected.Catalog, actual.Catalog);
        Assert.Equal(expected.Permissions, actual.Permissions);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Empty(await reader.ListRelationshipsAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(RecoveryCases))]
    public async Task DropLink_WithEnforcedExternalRelationshipRefusesWithoutChangingBytes(DatabaseFormat format, LinkedTableKind kind)
    {
        await using MemoryStream stream = await SeedAsync(format);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await SeedRelationshipAsync(writer, kind.ToString(), enforced: true);
        }

        byte[] original = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetOperationException error = await Assert.ThrowsAsync<JetOperationException>(async () => await writer.DropTableAsync(kind.ToString(), Ct));
            Assert.Equal(JetErrorCode.TableInRelationship, error.ErrorCode);
        }

        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropLink_SharedIdentityWithOpaqueObjectRefusesWithoutChangingBytes(DatabaseFormat format)
    {
        await using MemoryStream stream = await SeedAsync(format);
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            _ = await writer.Services.CatalogWriter.InsertCatalogObjectAsync(new(123456, Constants.SystemObjects.TablesParentId, "OpaqueAlias", 127, 0), Ct);
            TableDef objects = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
            List<CatalogRow> entries = await writer.Services.CatalogRows.GetCatalogRowsAsync(objects, Ct);
            CatalogRow link = Assert.Single(entries, entry => entry.Name == "Odbc");
            LocatedRow opaque = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(2, Ct), row => Equals(row.Values[objects.FindColumnIndex("Name")], "OpaqueAlias"));
            byte[] page = await writer.Database.Pages.ReadPageCopyAsync(opaque.Location.PageNumber, Ct);
            Assert.True(RowDecodePlan.TryParseRowLayout(writer.Database.Format.RowFields, page, opaque.Location.RowStart, opaque.Location.RowSize, objects.HasVarColumns, out RowLayout layout));
            ColumnSlice id = RowDecodePlan.ResolveColumnSlice(writer.Database.Format.RowFields, page, opaque.Location.RowStart, opaque.Location.RowSize, layout, Assert.IsType<ColumnInfo>(objects.FindColumn("Id")));
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(opaque.Location.RowStart + id.DataStart, id.DataLen), checked((int)link.Id));
            await writer.Pager.WritePageAsync(opaque.Location.PageNumber, page, Ct);
        }

        byte[] original = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.DropTableAsync("Odbc", Ct));
            Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        }

        Assert.Equal(original, stream.ToArray());
    }

    private static async Task SeedRelationshipAsync(AccessWriter writer, string link, bool enforced)
    {
        long page = await writer.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysRelationships", Ct);
        TableDef relationships = await writer.Database.TableDefs.ReadRequiredTableDefAsync(page, "MSysRelationships", Ct);
        object[] row = relationships.CreateNullValueRow();
        relationships.SetValueByName(row, "ccolumn", 1);
        relationships.SetValueByName(row, "grbit", enforced ? 0 : 2);
        relationships.SetValueByName(row, "icolumn", 0);
        relationships.SetValueByName(row, "szColumn", "Id");
        relationships.SetValueByName(row, "szObject", link);
        relationships.SetValueByName(row, "szReferencedColumn", "Id");
        relationships.SetValueByName(row, "szReferencedObject", "Keep");
        relationships.SetValueByName(row, "szRelationship", "LinkedRelationship");
        await writer.Services.Mutations.InsertSystemRowAndMaintainAsync(page, relationships, "MSysRelationships", row, cancellationToken: Ct);
        _ = await writer.Services.CatalogWriter.InsertCatalogObjectAsync(CatalogObjectArtifact.Relationship("LinkedRelationship"), Ct);
    }

    private static async Task<LongValueSnapshot> ReadLongValueAsync(AccessWriter writer, string objectName)
    {
        (RowLocation location, ColumnSlice slice) = await FindLongValueAsync(writer, objectName);
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(location.PageNumber, Ct);
        byte[] field = page.AsSpan(location.RowStart + slice.DataStart, slice.DataLen).ToArray();
        Assert.True(LongValueDescriptor.TryRead(field, out LongValueDescriptor descriptor));
        var rows = new List<byte[]>();
        uint pointer = descriptor.IsExternal ? descriptor.FirstDp : 0;
        while (pointer != 0)
        {
            Assert.InRange(rows.Count, 0, 3);
            byte[] data = await writer.Database.Pages.ReadPageCopyAsync(pointer >> 8, Ct);
            Assert.True(DataPageRows.TryGetSlotBound(writer.Database.Format, data, checked((int)(pointer & 0xFF)), out RowBound bound));
            byte[] row = data.AsSpan(bound.RowStart, bound.RowSize).ToArray();
            rows.Add(row);
            pointer = descriptor.IsSinglePage ? 0 : BinaryPrimitives.ReadUInt32LittleEndian(row);
        }

        return new(field, rows);
    }

    private static async Task<(RowLocation Location, ColumnSlice Slice)> FindLongValueAsync(AccessWriter writer, string objectName)
    {
        TableDef objects = await writer.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
        LocatedRow row = Assert.Single(await writer.Services.Snapshots.ReadRowsAsync(2, Ct), value => Equals(value.Values[objects.FindColumnIndex("Name")], objectName));
        byte[] page = await writer.Database.Pages.ReadPageCopyAsync(row.Location.PageNumber, Ct);
        Assert.True(RowDecodePlan.TryParseRowLayout(writer.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, objects.HasVarColumns, out RowLayout layout));
        ColumnSlice slice = RowDecodePlan.ResolveColumnSlice(writer.Database.Format.RowFields, page, row.Location.RowStart, row.Location.RowSize, layout, Assert.IsType<ColumnInfo>(objects.FindColumn("RmtInfoLong")));
        return (row.Location, slice);
    }

    private static async Task<MemoryStream> SeedAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new() { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Keep", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Label", typeof(string), 40)], Ct);
            await writer.InsertRowAsync("Keep", [7, "unchanged"], Ct);
            string missing = Path.Combine(Path.GetTempPath(), "JetDatabaseWriter-unavailable-linked-source", "absent.mdb");
            await writer.CreateLinkedTableAsync("Access", missing, "Remote", Ct);
            await writer.CreateLinkedTextTableAsync("Text", Path.GetDirectoryName(missing)!, "absent.csv", "Text;HDR=YES;FMT=Delimited", Ct);
            await writer.CreateLinkedOdbcTableAsync("Odbc", "ODBC;DSN=UnavailableDropControl", "dbo.Remote", Ct);
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task<Snapshot> ReadAsync(MemoryStream stream, string[] removedNames)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        using DataTable catalog = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        HashSet<int> ids = catalog.AsEnumerable().Where(row => removedNames.Contains((string)row["Name"], StringComparer.Ordinal))
            .Select(row => (int)row["Id"]).ToHashSet();
        string[] retained = catalog.AsEnumerable().Where(row => !ids.Contains((int)row["Id"]))
            .Select(Encode).Order(StringComparer.Ordinal).ToArray();
        using DataTable permissions = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string[] grants = permissions.AsEnumerable().Where(row => !ids.Contains((int)row["ObjectId"]))
            .Select(Encode).Order(StringComparer.Ordinal).ToArray();
        return new(retained, grants);
    }

    private static string Encode(DataRow row) => string.Join("|", row.ItemArray.Select(value => value switch
    {
        byte[] bytes => "bytes:" + Convert.ToHexString(bytes),
        DateTime date => "date:" + date.ToBinary().ToString(CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    }));

    private static WriteFaultStream Copy(byte[] original)
    {
        var stream = new WriteFaultStream();
        stream.Write(original, 0, original.Length);
        stream.Position = 0;
        return stream;
    }

    private sealed record Snapshot(string[] Catalog, string[] Permissions);

    private sealed record LongValueSnapshot(byte[] Field, List<byte[]> Rows);
}
