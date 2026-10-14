namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Buffers.Binary;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Relationship drops release only their native catalog and security identities.</summary>
/// <param name="db">Caches native-format seed files.</param>
public sealed class RelationshipCatalogIdentityTests(DatabaseCache db) : IClassFixture<DatabaseCache>
{
    private const string Relationship = "FK_C_P";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static TheoryData<DatabaseFormat, WriteMode> FormatsAndModes() => ForeignKeyTestDatabase.FormatsAndModes();

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task Drop_ReleasesIdentityAndPermissions_PreservesUnrelatedRowsAndAllowsNameReuse(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await this.CreateAsync(format);
        DataTable beforeObjects = await ReadAsync(stream, "MSysObjects");
        int id = RelationshipId(beforeObjects);
        DataTable beforeAces = await ReadAsync(stream, "MSysACEs");
        Assert.NotEmpty(beforeAces.AsEnumerable().Where(row => (int)row["ObjectId"] == id));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () => await writer.DropRelationshipAsync("fk_c_p", Ct));
        }

        DataTable afterObjects = await ReadAsync(stream, "MSysObjects");
        Assert.DoesNotContain(afterObjects.AsEnumerable(), row => (int)row["Id"] == id);
        Assert.Equal(Snapshot(beforeObjects, row => (int)row["Id"] != id), Snapshot(afterObjects, _ => true));
        DataTable afterAces = await ReadAsync(stream, "MSysACEs");
        Assert.Equal(Snapshot(beforeAces, row => (int)row["ObjectId"] != id), Snapshot(afterAces, _ => true));
        await ForeignKeyTestDatabase.AssertIndexesCoverRowsAsync(stream, "P", "C", Relationship);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.CreateRelationshipAsync(Definition(), Ct);
                await writer.DropRelationshipAsync(Relationship, Ct);
                await writer.CreateRelationshipAsync(Definition(), Ct);
            });
        }

        Assert.Single((await ReadAsync(stream, "MSysRelationships")).AsEnumerable(), row => (string)row["szRelationship"] == Relationship);
        _ = RelationshipId(await ReadAsync(stream, "MSysObjects"));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DropAndRecreate_CallerRollback_RestoresEveryByte(DatabaseFormat format)
    {
        await using MemoryStream stream = await this.CreateAsync(format);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.ExplicitCommit))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await writer.RenameRelationshipAsync(Relationship, "FK_Renamed", Ct);
            await writer.DropRelationshipAsync("FK_Renamed", Ct);
            await writer.CreateRelationshipAsync(Definition(), Ct);
            await transaction.RollbackAsync(Ct);
            await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.CreateRelationshipAsync(Definition(), Ct));
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData("type", WriteMode.Direct)]
    [InlineData("type", WriteMode.AutoCommit)]
    [InlineData("type", WriteMode.ExplicitCommit)]
    [InlineData("shared-id", WriteMode.Direct)]
    [InlineData("shared-id", WriteMode.AutoCommit)]
    [InlineData("shared-id", WriteMode.ExplicitCommit)]
    [InlineData("zero-id", WriteMode.Direct)]
    [InlineData("zero-id", WriteMode.AutoCommit)]
    [InlineData("zero-id", WriteMode.ExplicitCommit)]
    public async Task AmbiguousCatalogIdentity_RefusesBeforeMutationAndLeavesCallerTransactionUsable(string corruption, WriteMode mode)
    {
        await using MemoryStream stream = await this.CreateAsync(DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            TableDef objects = await harness.Database.TableDefs.ReadRequiredTableDefAsync(2, "MSysObjects", Ct);
            ColumnInfo name = Assert.IsType<ColumnInfo>(objects.FindColumn("Name"));
            ColumnInfo type = Assert.IsType<ColumnInfo>(objects.FindColumn("Type"));
            ColumnInfo target = Assert.IsType<ColumnInfo>(objects.FindColumn(corruption == "type" ? "Type" : "Id"));
            int sharedId = checked((int)(await harness.Services.CatalogRows.GetCatalogRowsAsync(objects, Ct)).Single(row => row.Name == "FK_Unrelated").Id);
            bool changed = false;
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(2, Ct))
            {
                byte[] page = await harness.Database.Pages.ReadPageAsync(location.PageNumber, Ct);
                if (ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, page, location.RowStart, location.RowSize, name) != Relationship
                    || ScalarColumnReader.DecodeSimpleColumnValue(harness.Database.Format, page, location.RowStart, location.RowSize, type) != "8")
                {
                    continue;
                }

                int offset = location.RowStart + harness.Database.Format.RowFields.NumCols + target.FixedOff;
                if (corruption == "type")
                {
                    BinaryPrimitives.WriteInt16LittleEndian(page.AsSpan(offset, 2), 5);
                }
                else
                {
                    BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(offset, 4), corruption == "zero-id" ? 0 : sharedId);
                }

                await harness.Pager.WritePageAsync(location.PageNumber, page, Ct);
                changed = true;
                break;
            }

            Assert.True(changed);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.DropRelationshipAsync(Relationship, Ct));
                Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
                error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await writer.RenameRelationshipAsync(Relationship, "FK_Renamed", Ct));
                Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
                Assert.Equal(before, stream.ToArray());
                await writer.InsertRowAsync("P", [2, "two"], Ct);
            });
        }

        Assert.Equal(2, (await ReadAsync(stream, "P")).Rows.Count);
    }

    [Theory]
    [MemberData(nameof(FormatsAndModes))]
    public async Task RenameThenDrop_ReleasesBothNamesAndPreservesSecurityIdentity(DatabaseFormat format, WriteMode mode)
    {
        await using MemoryStream stream = await this.CreateAsync(format);
        int id = RelationshipId(await ReadAsync(stream, "MSysObjects"));
        string[] aces = Snapshot(await ReadAsync(stream, "MSysACEs"), _ => true);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () => await writer.RenameRelationshipAsync(Relationship, "FK_Renamed", Ct));
        }

        DataTable renamed = await ReadAsync(stream, "MSysObjects");
        DataRow identity = Assert.Single(renamed.AsEnumerable(), row => (int)row["Id"] == id);
        Assert.Equal("FK_Renamed", identity["Name"]);
        Assert.Equal(aces, Snapshot(await ReadAsync(stream, "MSysACEs"), _ => true));
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await writer.DropRelationshipAsync("FK_Renamed", Ct);
                await writer.CreateRelationshipAsync(Definition(), Ct);
                await writer.RenameRelationshipAsync(Relationship, "FK_Renamed", Ct);
                await writer.DropRelationshipAsync("FK_Renamed", Ct);
                await writer.CreateRelationshipAsync(Definition(), Ct);
            });
        }

        _ = RelationshipId(await ReadAsync(stream, "MSysObjects"));
    }

    [Theory]
    [InlineData(WriteMode.Direct)]
    [InlineData(WriteMode.AutoCommit)]
    [InlineData(WriteMode.ExplicitCommit)]
    public async Task Rename_CatalogOnlyNameCollision_RefusesBeforeMutation(WriteMode mode)
    {
        await using MemoryStream stream = await this.CreateAsync(DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            _ = await harness.Services.CatalogArtifacts.ExecutePlanAsync(new([], [CatalogObjectArtifact.Relationship("FK_Reserved")]), Ct);
        }

        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                await Assert.ThrowsAsync<JetObjectExistsException>(async () => await writer.RenameRelationshipAsync(Relationship, "FK_Reserved", Ct));
                Assert.Equal(before, stream.ToArray());
                await writer.InsertRowAsync("P", [2, "two"], Ct);
            });
        }

        Assert.Equal(2, (await ReadAsync(stream, "P")).Rows.Count);
    }

    [Fact]
    public async Task Drop_AdditionalCatalogAndPermissionIndexKeys_RemovesEveryIndexEntry()
    {
        await using MemoryStream stream = await this.CreateAsync(DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct))
        {
            foreach (string name in new[] { "MSysObjects", "MSysACEs" })
            {
                long pageNumber = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(name, Ct);
                TableDef definition = await harness.Database.TableDefs.ReadRequiredTableDefAsync(pageNumber, name, Ct);
                byte[] bytes = Assert.IsType<byte[]>(await harness.Database.TableDefs.ReadTDefBytesAsync(pageNumber, Ct));
                Assert.Equal(harness.Database.Format.PageSize, bytes.Length);
                TDefCounts counts = TDefCodec.ReadCounts(harness.Database.Format, bytes);
                int descriptorStart = IndexCatalogReader.LocateRealIdxDescStart(harness.Database.Format, bytes, counts.ColumnCount, counts.RealIndexCount);
                int slot = name == "MSysObjects" ? 1 : 0;
                int descriptor = harness.Database.Format.Index.RealIdxPhysOffset(descriptorStart, slot);
                int map = harness.Database.Format.Index.ColMapSlotOffset(descriptor, 0);
                ColumnInfo replacement = Assert.IsType<ColumnInfo>(definition.FindColumn(name == "MSysObjects" ? "Type" : "ACM"));
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(map, 2), checked((ushort)replacement.ColNum));
                int nextMap = harness.Database.Format.Index.ColMapSlotOffset(descriptor, 1);
                BinaryPrimitives.WriteUInt16LittleEndian(bytes.AsSpan(nextMap, 2), ushort.MaxValue);
                int flags = descriptor + harness.Database.Format.Index.FlagsOffsetWithinPhys;
                bytes[flags] = (byte)((bytes[flags] & ~1) | Constants.TableDefinition.IgnoreNullsIndexFlag);
                await harness.Pager.WritePageAsync(pageNumber, bytes, Ct);
                await harness.Services.Indexes.MaintainIndexesAsync(pageNumber, definition, name, Ct);
            }
        }

        await AssertSystemIndexesAsync(stream);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.DropRelationshipAsync(Relationship, Ct);
        }

        await AssertSystemIndexesAsync(stream);
    }

    [Theory]
    [InlineData(WriteMode.Direct, "create")]
    [InlineData(WriteMode.Direct, "rename")]
    [InlineData(WriteMode.Direct, "rewrite")]
    [InlineData(WriteMode.AutoCommit, "create")]
    [InlineData(WriteMode.AutoCommit, "rename")]
    [InlineData(WriteMode.AutoCommit, "rewrite")]
    [InlineData(WriteMode.ExplicitCommit, "create")]
    [InlineData(WriteMode.ExplicitCommit, "rename")]
    [InlineData(WriteMode.ExplicitCommit, "rewrite")]
    public async Task UnsupportedDatabaseNameSortOrder_RefusesBeforeMutation(WriteMode mode, string operation)
    {
        await using MemoryStream stream = await this.CreateAsync(DatabaseFormat.AceAccdb);
        byte[] image = stream.ToArray();
        EncryptionManager.TransformHeaderMask(image, DatabaseFormat.AceAccdb);
        BinaryPrimitives.WriteUInt16LittleEndian(image.AsSpan(0x6E, 2), 0x0411);
        EncryptionManager.TransformHeaderMask(image, DatabaseFormat.AceAccdb);
        stream.Position = 0;
        await stream.WriteAsync(image, Ct);
        byte[] before = stream.ToArray();
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, mode))
        {
            await ForeignKeyTestDatabase.RunAsync(writer, mode, async () =>
            {
                Func<Task> mutation = operation switch
                {
                    "create" => async () => await writer.CreateRelationshipAsync(new("FK_New", "P", "Id", "C", "ParentId"), Ct),
                    "rename" => async () => await writer.RenameRelationshipAsync(Relationship, "FK_Renamed", Ct),
                    _ => async () => await writer.AddColumnAsync("P", new("Extra", typeof(int)), Ct),
                };
                JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(mutation);
                Assert.Equal(JetErrorCode.UnsupportedTextCollation, error.ErrorCode);
                Assert.Equal(before, stream.ToArray());
            });
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task RenameAcrossNativeNameSortBoundaries_KeepsNativeOrder(DatabaseFormat format)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.CreateTableAsync("NameSortParent", [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("NameSortChild", [new("Id", typeof(int)), new("ParentId", typeof(int)), new("Note", typeof(int))], [new("A_C_Note", "Note"), new("PK_C", "Id") { IsPrimaryKey = true }, new("Z_C_Note", ["Note", "Id"])], Ct);
            await writer.CreateRelationshipAsync(new("Rel", "NameSortParent", "Id", "NameSortChild", "ParentId"), Ct);
        }

        string previous = "Rel";
        foreach (string name in new[] { "ZZ_Rel", "AA_Rel", "A-Note", "A Note", "Aé" })
        {
            await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
            {
                await writer.RenameRelationshipAsync(previous, name, Ct);
            }

            string[] expected = name switch
            {
                "ZZ_Rel" => ["A_C_Note", "PK_C", "Z_C_Note", name],
                "A Note" => [name, "A_C_Note", "PK_C", "Z_C_Note"],
                "A-Note" when format == DatabaseFormat.Jet3Mdb => [name, "A_C_Note", "PK_C", "Z_C_Note"],
                _ => ["A_C_Note", name, "PK_C", "Z_C_Note"],
            };
            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
            Assert.Equal(expected, (await reader.ListIndexesAsync("NameSortChild", Ct)).Select(index => index.Name));
            previous = name;
        }

        await using (AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct))
        {
            await writer.DropRelationshipAsync(previous, Ct);
            await writer.CreateRelationshipAsync(new("AA_Rel", "NameSortParent", "Id", "NameSortChild", "ParentId"), Ct);
        }

        stream.Position = 0;
        await using AccessReader finalReader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        Assert.Equal(["A_C_Note", "AA_Rel", "PK_C", "Z_C_Note"], (await finalReader.ListIndexesAsync("NameSortChild", Ct)).Select(index => index.Name));
    }

    private static async Task AssertSystemIndexesAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: Ct);
        foreach (string name in new[] { "MSysObjects", "MSysACEs" })
        {
            long page = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync(name, Ct);
            foreach (long root in await IndexLeafChain.ReadRealIndexRootsAsync(harness.Database, page, Ct))
            {
                await IndexLeafChain.AssertCoversLiveRowsAsync(harness.Database, page, root, Ct);
            }
        }
    }

    private static RelationshipDefinition Definition() => new(Relationship, "P", "Id", "C", "ParentId");

    private static int RelationshipId(DataTable objects) =>
        (int)Assert.Single(objects.AsEnumerable(), row => (string)row["Name"] == Relationship && Convert.ToInt32(row["Type"], CultureInfo.InvariantCulture) == 8)["Id"];

    private static string[] Snapshot(DataTable table, Func<DataRow, bool> include) =>
        table.AsEnumerable().Where(include).Select(row => string.Join("|", row.ItemArray.Select(value => value is byte[] bytes ? Convert.ToHexString(bytes) : Convert.ToString(value, CultureInfo.InvariantCulture)))).Order(StringComparer.Ordinal).ToArray();

    private static async Task<DataTable> ReadAsync(MemoryStream stream, string table)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, Ct);
        return await reader.ReadTableAsync(table, cancellationToken: Ct);
    }

    private async Task<MemoryStream> CreateAsync(DatabaseFormat format)
    {
        MemoryStream stream = await ForeignKeyTestDatabase.CreateAsync(db, format, [[1, "one"]], [[1, 1, "a"]]);
        await using AccessWriter writer = await ForeignKeyTestDatabase.OpenWriterAsync(stream, WriteMode.Direct);
        await writer.CreateTableAsync(Relationship, [new("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
        await writer.CreateRelationshipAsync(Definition(), Ct);
        await writer.CreateRelationshipAsync(new("FK_Unrelated", "P", "Id", "C", "Id"), Ct);
        return stream;
    }
}
