namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Schema rewrites preserve opaque native index metadata while relocating rebuilt storage.</summary>
public sealed class IndexDescriptorPreservationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every schema operation, format and write mode.</summary>
    public static TheoryData<DatabaseFormat, string, WriteMode> Changes()
    {
        var cases = new TheoryData<DatabaseFormat, string, WriteMode>();
        foreach (DatabaseFormat format in Enum.GetValues<DatabaseFormat>())
        {
            foreach (string operation in new[] { "add", "drop", "rename" })
            {
                foreach (WriteMode mode in Enum.GetValues<WriteMode>())
                {
                    cases.Add(format, operation, mode);
                }
            }
        }

        return cases;
    }

    /// <summary>Gets every format and writer mode for a newly synthesized primary key.</summary>
    public static TheoryData<DatabaseFormat, WriteMode> NewPrimaryKeyCases() => WriteModes.Combine(Enum.GetValues<DatabaseFormat>());

    /// <summary>Gets native-accepted physical topology variants on the DAO3.6 formats.</summary>
    public static TheoryData<DatabaseFormat, string, string> Dao36TopologyCases()
    {
        var cases = new TheoryData<DatabaseFormat, string, string>();
        foreach (DatabaseFormat format in new[] { DatabaseFormat.Jet3Mdb, DatabaseFormat.Jet4Mdb })
        {
            foreach (string operation in new[] { "add", "drop", "rename" })
            {
                foreach (string layout in new[] { "descending", "distinct", "shared" })
                {
                    cases.Add(format, operation, layout);
                }
            }
        }

        return cases;
    }

    /// <summary>Gets native-accepted physical topology variants on ACE.</summary>
    public static TheoryData<string, string> Dao120TopologyCases()
    {
        var cases = new TheoryData<string, string>();
        foreach (string operation in new[] { "add", "drop", "rename" })
        {
            foreach (string layout in new[] { "descending", "distinct", "shared" })
            {
                cases.Add(operation, layout);
            }
        }

        return cases;
    }

    [Theory]
    [MemberData(nameof(NewPrimaryKeyCases))]
    public async Task AddPrimaryKey_PreservesExistingForeignKeyWithDefaultPrimaryKeyName(DatabaseFormat format, WriteMode mode)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Parent", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], cancellationToken: Ct);
            await writer.CreateTableAsync("Child", [new ColumnDefinition("ParentId", typeof(int))], cancellationToken: Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("PrimaryKey", "Parent", "Id", "Child", "ParentId"), Ct);
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Child", Ct);
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct);
            TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
            TDefLogicalIndexDescriptor foreign = Assert.Single(image.LogicalIndexes);
            chain.Bytes[image.RealIndexes[foreign.Entry.IndexNum2].Slot.PhysStart + writer.Database.Format.Index.ColMapStartWithinPhys + 2] |= 0x40;
            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
            var properties = ColumnPropertyBlockBuilder.FromBlock(ColumnPropertyBlock.Empty(writer.Database.Format));
            var target = new ColumnPropertyTargetBuilder { Name = "PrimaryKey", ChunkType = ColumnPropertyChunkType.PropertyBlockAlt2 };
            target.AddText("Description", "relationship index", writer.Database.Format);
            properties.Targets.Add(target);
            await writer.Services.CatalogWriter.UpdateTablePropertiesAsync("Child", table.Entry.TDefPage, properties.ToBytes(writer.Database.Format), cancellationToken: Ct);
        }

        DefinitionSnapshot before = await ReadDefinitionAsync(stream, "Child");
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => writer.AddColumnAsync("Child", new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, Ct).AsTask(), Ct);
        }

        DefinitionSnapshot after = await ReadDefinitionAsync(stream, "Child");
        TDefLogicalIndexDescriptor oldForeign = Assert.Single(before.Image.LogicalIndexes);
        TDefLogicalIndexDescriptor newForeign = Assert.Single(after.Image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey);
        Assert.Equal(oldForeign.Name, newForeign.Name);
        byte[] expected = oldForeign.RawDescriptor.ToArray();
        byte[] actual = newForeign.RawDescriptor.ToArray();
        int fields = before.Format.Index.LogicalEntryFieldsOffset;
        actual.AsSpan(fields, 17).CopyTo(expected.AsSpan(fields));
        Assert.Equal(expected, actual);
        Assert.Equal(before.Image.RealIndexes[oldForeign.Entry.IndexNum2].RawDescriptor[before.Format.Index.ColMapStartWithinPhys + 2], after.Image.RealIndexes[newForeign.Entry.IndexNum2].RawDescriptor[after.Format.Index.ColMapStartWithinPhys + 2]);
        stream.Position = 0;
        await using AccessWriter reopened = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("Child", Ct);
        IndexMetadata primary = Assert.Single(indexes, index => index.Kind == IndexKind.PrimaryKey);
        Assert.NotEqual("PrimaryKey", primary.Name);
        Assert.Equal("Id", Assert.Single(primary.Columns).Name);
        Assert.True(primary.EnforcesUniqueness);
        Assert.True(primary.IsRequired);
        ResolvedTable child = await reopened.Services.Catalog.ResolveRequiredTableAsync("Child", Ct);
        ColumnPropertyBlock block = Assert.IsType<ColumnPropertyBlock>(await reopened.Services.Snapshots.ReadLvPropBlockAsync(child.Entry.TDefPage, Ct));
        ColumnPropertyTarget property = Assert.Single(block.Targets, target => target.ChunkType == ColumnPropertyChunkType.PropertyBlockAlt2);
        Assert.Equal("PrimaryKey", property.Name);
        Assert.Equal("relationship index", block.TextEncoding!.GetString(Assert.IsType<ColumnPropertyEntry>(property.Find("Description")).Value));
        Assert.Equal(["Child", "Parent"], (await reader.ListTablesAsync(Ct)).Order(StringComparer.OrdinalIgnoreCase));
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public async Task Rewrite_PreservesOpaqueDescriptorsAndSharedForeignKeys(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format);
        DefinitionSnapshot before = await ReadDefinitionAsync(stream);
        Assert.Contains(before.Image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey);
        Assert.True(before.Image.LogicalIndexes.Count > before.Image.RealIndexes.Count);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation), Ct);
            await Assert.ThrowsAsync<JetConstraintException>(async () => await writer.InsertRowAsync("T", new RowValues { ["Id"] = 1 }, Ct));
        }

        DefinitionSnapshot after = await ReadDefinitionAsync(stream);
        AssertPreserved(before, after);
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IReadOnlyList<IndexMetadata> indexes = await reader.ListIndexesAsync("T", Ct);
        Assert.Equal(before.Image.LogicalIndexes.Count, indexes.Count);
        foreach (IndexMetadata index in indexes)
        {
            object key = index.Columns[0].Name switch { "ParentId" => 1, "Id" => 2, _ => 20 };
            Assert.Single(await reader.SeekRowsAsync("T", index.Name, [key], Ct).ToListAsync(Ct));
            if (index.IsForeignKey)
            {
                IndexMetadata partner = Assert.Single(indexes, candidate => candidate.IndexNumber == index.RelatedIndexNumber);
                Assert.Equal(index.IndexNumber, partner.RelatedIndexNumber);
                Assert.Equal(before.Root, index.RelatedTablePage);
            }
        }

        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: Ct);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Equal(["T"], await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rewrite_RollbackRestoresCompleteMarkedImage(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format);
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.ExplicitCommit), leaveOpen: true, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await RewriteAsync(writer, "rename");
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public async Task Rewrite_DistinctPhysicalMetadataRetainsItsIdentity(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, conflictingMetadata: true);
        DefinitionSnapshot before = await ReadDefinitionAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation), Ct);
        }

        AssertPreserved(before, await ReadDefinitionAsync(stream));
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public async Task Rewrite_OrdinaryIndexesRetainSharedPhysicalIdentity(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, conflictingMetadata: true, sharedOrdinary: true);
        DefinitionSnapshot before = await ReadDefinitionAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation), Ct);
        }

        AssertPreserved(before, await ReadDefinitionAsync(stream));
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Single(await reader.SeekRowsAsync("T", "PK", [2], Ct).ToListAsync(Ct));
        Assert.Single(await reader.SeekRowsAsync("T", "ZZDuplicate", [2], Ct).ToListAsync(Ct));
        Assert.Equal(["T"], await reader.ListTablesAsync(Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, "shared")]
    [InlineData(DatabaseFormat.Jet4Mdb, "shared")]
    [InlineData(DatabaseFormat.AceAccdb, "shared")]
    [InlineData(DatabaseFormat.Jet3Mdb, "descending")]
    [InlineData(DatabaseFormat.Jet4Mdb, "descending")]
    [InlineData(DatabaseFormat.AceAccdb, "descending")]
    public async Task Rewrite_ChangedTopologyRollbackRestoresCompleteImage(DatabaseFormat format, string layout)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, conflictingMetadata: layout == "shared", descendingForeignKey: layout == "descending", sharedOrdinary: layout == "shared");
        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.ExplicitCommit), leaveOpen: true, Ct))
        {
            await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
            await RewriteAsync(writer, "drop");
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public async Task Rewrite_ForeignKeyIgnoreNullsKeepsItsTreeMembership(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, ignoreNullForeignKey: true);
        await AssertSingleForeignKeyEntryAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation), Ct);
        }

        await AssertSingleForeignKeyEntryAsync(stream);
    }

    private static async Task AssertSingleForeignKeyEntryAsync(MemoryStream stream)
    {
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IndexMetadata foreign = Assert.Single(await reader.ListIndexesAsync("T", Ct), index => index.IsForeignKey && index.Columns[0].Name == "ParentId");
        Assert.True(foreign.IgnoreNulls);
        byte[] page = await reader.ReadPageCopyAsync(foreign.FirstDp, Ct);
        Assert.Single(IndexPageCodec.DecodeLeafEntries(reader.Database.Format.IndexPage, page, reader.Database.Format.PageSize));
        Assert.Single(await reader.SeekRowsAsync("T", foreign.Name, [1], Ct).ToListAsync(Ct));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, -1)]
    [InlineData(DatabaseFormat.Jet3Mdb, int.MaxValue)]
    [InlineData(DatabaseFormat.Jet4Mdb, -1)]
    [InlineData(DatabaseFormat.Jet4Mdb, int.MaxValue)]
    [InlineData(DatabaseFormat.AceAccdb, -1)]
    [InlineData(DatabaseFormat.AceAccdb, int.MaxValue)]
    public async Task Rewrite_InvalidLogicalRealIndexReferenceRefusesWithoutChangingBytes(DatabaseFormat format, int reference)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format);
        long root;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            root = table.Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
            TDefLogicalIndexDescriptor index = Assert.Single(image.LogicalIndexes, candidate => candidate.Name == "IX");
            Wi32(chain.Bytes, index.Entry.FieldsOffset + 4, reference);
            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(() => RewriteAsync(writer, "rename"));
            Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
            Assert.Equal("T", error.ErrorInfo.TableName);
            Assert.Equal("IX", error.ErrorInfo.IndexName);
            Assert.Equal(root, error.ErrorInfo.PageNumber);
            Assert.NotNull(error.ErrorInfo.Reason);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rewrite_LogicalIndexesWithoutPhysicalDescriptorsRefusesWithoutChangingBytes(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format);
        long root;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            root = table.Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
            JetDatabaseWriter.Indexes.Models.IndexSectionAnchors section = Assert.IsType<JetDatabaseWriter.Indexes.Models.IndexSectionAnchors>(image.IndexSection);
            int columnStart = writer.Database.Format.TDef.BlockEnd + (image.RealIndexes.Count * writer.Database.Format.TDef.RealIdxEntrySz);
            int columnLength = section.RealIdxDescStart - columnStart;
            int oldEnd = checked(Ri32(chain.Bytes, 8) + 8);
            int logicalLength = oldEnd - section.LogIdxStart;
            byte[] bytes = chain.Bytes.ToArray();
            Buffer.BlockCopy(chain.Bytes, columnStart, bytes, writer.Database.Format.TDef.BlockEnd, columnLength);
            Buffer.BlockCopy(chain.Bytes, section.LogIdxStart, bytes, writer.Database.Format.TDef.BlockEnd + columnLength, logicalLength);
            Wi32(bytes, writer.Database.Format.TDef.NumRealIdx, 0);
            int end = writer.Database.Format.TDef.BlockEnd + columnLength + logicalLength;
            await chain.WriteAsync(bytes, end, writer.Services.PageAllocator.AllocatePageAsync, writer.Pager.WritePageAsync, writer.Services.PageAllocator.DeallocatePageAsync, writer.Database.Format.WritesTDefFreeSpace, Ct);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(() => RewriteAsync(writer, "rename"));
            Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
            Assert.Equal("T", error.ErrorInfo.TableName);
            Assert.Equal(root, error.ErrorInfo.PageNumber);
            Assert.NotNull(error.ErrorInfo.Reason);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rewrite_DuplicateLogicalIndexNamesRefuseWithoutChangingBytes(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format);
        long root;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            root = table.Entry.TDefPage;
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(root, Ct);
            TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
            int position = image.IndexSection!.Value.LogIdxNamesStart;
            foreach (TDefLogicalIndexDescriptor index in image.LogicalIndexes)
            {
                int start = position;
                Assert.True(writer.Database.Format.ReadColumnName(chain.Bytes, ref position, out _) >= 0);
                if (index.Name == "IX")
                {
                    byte[] name = writer.Database.Format.EncodeTDefNameRecord("PK");
                    Assert.Equal(position - start, name.Length);
                    name.CopyTo(chain.Bytes.AsSpan(start));
                }
            }

            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        }

        byte[] before = stream.ToArray();
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(() => RewriteAsync(writer, "rename"));
            Assert.Equal(JetErrorCode.CorruptIndex, error.ErrorCode);
            Assert.Equal("T", error.ErrorInfo.TableName);
            Assert.Equal("PK", error.ErrorInfo.IndexName);
            Assert.Equal(root, error.ErrorInfo.PageNumber);
            Assert.NotNull(error.ErrorInfo.Reason);
        }

        Assert.Equal(before, stream.ToArray());
    }

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb, "add")]
    [InlineData(DatabaseFormat.Jet3Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet3Mdb, "rename")]
    [InlineData(DatabaseFormat.Jet4Mdb, "add")]
    [InlineData(DatabaseFormat.Jet4Mdb, "drop")]
    [InlineData(DatabaseFormat.Jet4Mdb, "rename")]
    public async Task Dao36_NativeDescriptorsSurviveRewriteMutationAndCompact(DatabaseFormat format, string operation)
        => await AssertNativeRewriteAsync(format, operation);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData("add")]
    [InlineData("drop")]
    [InlineData("rename")]
    public async Task Dao120_NativeDescriptorsSurviveRewriteMutationAndCompact(string operation)
        => await AssertNativeRewriteAsync(DatabaseFormat.AceAccdb, operation);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Dao36TopologyCases))]
    public async Task Dao36_AcceptedPhysicalTopologySurvivesRewriteAndCompact(DatabaseFormat format, string operation, string layout)
        => await AssertNativeRewriteAsync(format, operation, layout);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [MemberData(nameof(Dao120TopologyCases))]
    public async Task Dao120_AcceptedPhysicalTopologySurvivesRewriteAndCompact(string operation, string layout)
        => await AssertNativeRewriteAsync(DatabaseFormat.AceAccdb, operation, layout);

    [Theory(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(Jet3RoundTripEnvironment.IsAvailable),
        SkipType = typeof(Jet3RoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    public async Task Dao36_NewPrimaryKeyPreservesForeignKeyNameAndProperties(DatabaseFormat format)
        => await AssertNativePrimaryKeyNameAsync(format);

    [Fact(
        Skip = AccessRoundTripEnvironment.RequiresMicrosoftAccessSkipReason,
        SkipUnless = nameof(AccessRoundTripEnvironment.IsAvailable),
        SkipType = typeof(AccessRoundTripEnvironment))]
    [Trait("Category", "RequiresMicrosoftAccess")]
    public async Task Dao120_NewPrimaryKeyPreservesForeignKeyNameAndProperties()
        => await AssertNativePrimaryKeyNameAsync(DatabaseFormat.AceAccdb);

    private static async Task AssertNativePrimaryKeyNameAsync(DatabaseFormat format)
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.PrimaryKeyIndexName", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE Parent (Id LONG CONSTRAINT ParentPK PRIMARY KEY)',128)
                $db.Execute('CREATE TABLE Child (ParentId LONG)',128)
                $db.Execute('ALTER TABLE Child ADD CONSTRAINT PrimaryKey FOREIGN KEY (ParentId) REFERENCES Parent (Id)',128)
                $index=$db.TableDefs('Child').Indexes('PrimaryKey')
                $index.Properties.Append($index.CreateProperty('Description',10,'native foreign index'))
                if(!$index.Foreign -or $index.Primary){throw 'Wrong native source index kind.'}
                $index=$null
                $db.Execute('INSERT INTO Parent VALUES (1)',128)
                $db.Execute('INSERT INTO Child VALUES (1)',128)
                $db.Execute('DELETE FROM Child',128)
            } finally {$db.Close()}
            Write-Output 'NATIVE_PRIMARYKEY_NAME_SOURCE_ACCEPTED'
            """,
            format,
            session);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await writer.AddColumnAsync("Child", new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, Ct);
        }

        const string verify = """
            $tdf=$db.TableDefs('Child')
            $foreign=$tdf.Indexes('PrimaryKey')
            if(!$foreign.Foreign -or $foreign.Primary -or $foreign.Properties('Description').Value -ne 'native foreign index'){throw 'Lost native foreign-index identity or properties.'}
            $primary=@($tdf.Indexes | Where-Object {$_.Primary})
            if($primary.Count -ne 1 -or $primary[0].Name -eq 'PrimaryKey' -or $primary[0].Fields(0).Name -ne 'Id' -or !$primary[0].Unique -or !$primary[0].Required){throw 'New primary key did not retain a distinct required unique identity.'}
            $primary=$null;$foreign=$null;$tdf=$null
            $db.Execute('INSERT INTO Child (Id,ParentId) VALUES (1,1)',128)
            $refused=$false
            try{$db.Execute('INSERT INTO Child (Id,ParentId) VALUES (1,1)',128)}catch{if(($_.Exception.HResult -band 65535) -ne 3022){throw};$refused=$true}
            if(!$refused){throw 'New primary key accepted a duplicate.'}
            $refused=$false
            try{$db.Execute('INSERT INTO Child (Id,ParentId) VALUES (2,999)',128)}catch{if(($_.Exception.HResult -band 65535) -ne 3201){throw};$refused=$true}
            if(!$refused){throw 'Old foreign key accepted a missing parent.'}
            $db.Execute('DELETE FROM Child',128)
            """;
        RunNative(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                {{verify}}
            } finally {$db.Close()}
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                {{verify}}
            } finally {$db.Close()}
            Write-Output 'NATIVE_DISTINCT_PRIMARYKEY_NAME_VERIFIED'
            """,
            format,
            session);
    }

    private static async Task AssertNativeRewriteAsync(DatabaseFormat format, string operation, string layout = "native")
    {
        await using var session = AccessRoundTripSession.CreateEmpty("JetDatabaseWriter.Tests.IndexDescriptorPreservation", databaseExtension: format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb");
        string source = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.SourcePath);
        int version = format switch { DatabaseFormat.Jet3Mdb => 32, DatabaseFormat.Jet4Mdb => 64, DatabaseFormat.AceAccdb => 128, _ => throw new ArgumentOutOfRangeException(nameof(format)) };
        RunNative(
            $$"""
            $db=$engine.CreateDatabase({{source}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            try {
                $db.Execute('CREATE TABLE T (Spare LONG,Id LONG,ParentId LONG,K LONG)',128)
                $db.Execute('INSERT INTO T VALUES (7,1,NULL,10)',128)
                $db.Execute('INSERT INTO T VALUES (8,2,1,20)',128)
                $db.Execute('CREATE UNIQUE INDEX PK ON T (Id) WITH PRIMARY',128)
                $db.Execute('CREATE INDEX IX ON T (K DESC)',128)
                {{(layout is "distinct" or "shared" ? "$db.Execute('CREATE UNIQUE INDEX ZZDuplicate ON T (Id)',128)" : string.Empty)}}
                $db.Execute('ALTER TABLE T ADD CONSTRAINT Self FOREIGN KEY (ParentId) REFERENCES T (Id)',128)
            } finally {$db.Close()}
            Write-Output 'NATIVE_DESCRIPTORS_CREATED'
            """,
            format,
            session);
        if (layout != "native")
        {
            await PlantNativeTopologyAsync(session.SourcePath, layout);
            RunNative(
                $$"""
                $db=$engine.OpenDatabase({{source}})
                try {
                    {{NativeVerification("K", layout)}}
                    $db.Execute('INSERT INTO T (Id,ParentId,K) VALUES (3,1,30)',128)
                    $db.Execute('UPDATE T SET K=31 WHERE Id=3',128)
                    $db.Execute('DELETE FROM T WHERE Id=3',128)
                } finally {$db.Close()}
                Write-Output 'NATIVE_TOPOLOGY_ACCEPTED_BEFORE_SCHEMA_EDIT'
                """,
                format,
                session);
            if (layout == "descending")
            {
                string controlPath = Path.Combine(session.WorkDir, "native-control" + Path.GetExtension(session.SourcePath));
                RunNative(
                    $$"""
                    $engine.CompactDatabase({{source}},{{AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(controlPath)}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
                    Write-Output 'ORIGINAL_NATIVE_TOPOLOGY_COMPACTED'
                    """,
                    format,
                    session);
                await using AccessReader control = await TestDatabase.OpenReaderAsync(controlPath, cancellationToken: Ct);
                IndexMetadata child = Assert.Single(await control.ListIndexesAsync("T", Ct), index => index.IsForeignKey && index.Columns[0].Name == "ParentId");
                Assert.True(child.Columns[0].IsAscending);
            }
        }

        DefinitionSnapshot before = await ReadFileDefinitionAsync(session.SourcePath);
        Assert.True(before.Image.LogicalIndexes.Count > before.Image.RealIndexes.Count);
        await using (AccessWriter writer = await session.OpenWriterAsync(Ct))
        {
            await RewriteAsync(writer, operation);
        }

        AssertPreserved(before, await ReadFileDefinitionAsync(session.SourcePath));
        string key = operation == "rename" ? "Renamed" : "K";
        string compacted = AccessRoundTripEnvironment.ToPowerShellSingleQuotedLiteral(session.CompactedPath);
        string verify = NativeVerification(key, layout);
        RunNative(
            $$"""
            $db=$engine.OpenDatabase({{source}})
            try {
                {{verify}}
                $db.Execute('INSERT INTO T (Id,ParentId,[{{key}}]) VALUES (3,1,30)',128)
                $db.Execute('UPDATE T SET [{{key}}]=31 WHERE Id=3',128)
                $db.Execute('DELETE FROM T WHERE Id=3',128)
            } finally {$db.Close()}
            $engine.CompactDatabase({{source}},{{compacted}},';LANGID=0x0409;CP=1252;COUNTRY=0',{{version}})
            $db=$engine.OpenDatabase({{compacted}})
            try {
                {{NativeVerification(key, "compacted")}}
            } finally {$db.Close()}
            Write-Output 'NATIVE_DESCRIPTORS_VERIFIED'
            """,
            format,
            session);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(session.CompactedPath, cancellationToken: Ct);
        Assert.Equal(before.Image.LogicalIndexes.Count, (await reader.ListIndexesAsync("T", Ct)).Count);
        Assert.Single(await reader.SeekRowsAsync("T", "IX", [20], Ct).ToListAsync(Ct));
    }

    private static string NativeVerification(string key, string layout)
        => $$"""
            $rs=$db.OpenRecordset('T',1)
            try {
                $rs.Index='PK';$rs.Seek('=',2)
                if($rs.NoMatch -or $rs.Fields('{{key}}').Value -ne 20){throw 'Native primary-key read lost the row.'}
                $rs.Index='IX';$rs.Seek('=',10)
                if($rs.NoMatch -or $rs.Fields('Id').Value -ne 1){throw 'Native secondary-index read lost the row.'}
                $rs.MoveFirst();$count=0
                while(!$rs.EOF){$count++;$rs.MoveNext()}
                if($count -ne 2){throw 'Native index lost rows.'}
                $rs.Index='Self';$rs.Seek('=',1)
                if($rs.NoMatch -or $rs.Fields('Id').Value -ne 2){throw 'Native foreign-key seek lost the row.'}
                {{(layout == "descending" ? "$rs.MoveFirst();if($rs.Fields('Id').Value -ne 2){throw 'Native foreign-key tree lost its descending order.'}" : string.Empty)}}
            } finally {$rs.Close()}
            $refused=$false
            try{$db.Execute('INSERT INTO T (Id) VALUES (1)',128)}catch{if(($_.Exception.HResult -band 65535) -ne 3022){throw};$refused=$true}
            if(!$refused){throw 'Native uniqueness accepted a duplicate.'}
            $refused=$false
            try{$db.Execute('INSERT INTO T (Id,ParentId) VALUES (3,999)',128)}catch{if(($_.Exception.HResult -band 65535) -ne 3201){throw};$refused=$true}
            if(!$refused){throw 'Native foreign-key enforcement accepted a missing parent.'}
            """;

    private static async Task PlantNativeTopologyAsync(string path, string layout)
    {
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(path, cancellationToken: Ct);
        ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
        LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct);
        TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
        if (layout == "descending")
        {
            foreach (TDefLogicalIndexDescriptor foreign in image.LogicalIndexes.Where(index => index.Entry.IndexType == IndexKind.ForeignKey))
            {
                int order = image.RealIndexes[foreign.Entry.IndexNum2].Slot.PhysStart + writer.Database.Format.Index.ColMapStartWithinPhys + 2;
                chain.Bytes[order] &= 0xFE;
            }
        }
        else
        {
            TDefLogicalIndexDescriptor duplicate = Assert.Single(image.LogicalIndexes, index => index.Name == "ZZDuplicate");
            TDefLogicalIndexDescriptor parent = Assert.Single(image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey && index.Name != "Self");
            Wi32(chain.Bytes, parent.Entry.FieldsOffset + 4, duplicate.Entry.IndexNum2);
            if (layout == "shared")
            {
                TDefLogicalIndexDescriptor primary = Assert.Single(image.LogicalIndexes, index => index.Name == "PK");
                Wi32(chain.Bytes, duplicate.Entry.FieldsOffset + 4, primary.Entry.IndexNum2);
            }
        }

        await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
        writer.Database.TableDefs.OnInvalidateAll();
        await writer.Services.Indexes.MaintainIndexesAsync(table.Entry.TDefPage, table.Definition, "T", Ct);
    }

    private static void RunNative(string script, DatabaseFormat format, AccessRoundTripSession session)
    {
        if (format == DatabaseFormat.AceAccdb)
        {
            AccessRoundTripEnvironment.CompactResult result = AccessRoundTripEnvironment.RunDaoEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0, $"DAO120 failed: {result.StdOut}{Environment.NewLine}{result.StdErr}");
        }
        else
        {
            JetDatabaseWriter.TestSupport.PowerShellRunResult result = Jet3RoundTripEnvironment.RunEngineScript(script, session.WorkDir, TimeSpan.FromMinutes(2));
            Assert.True(result.ExitCode == 0 && !result.TimedOut && result.OutputComplete, $"DAO36 failed: {result.StandardOutput}{Environment.NewLine}{result.StandardError}");
        }
    }

    private static async Task<DefinitionSnapshot> ReadFileDefinitionAsync(string path)
    {
        await using var stream = new MemoryStream(await File.ReadAllBytesAsync(path, Ct));
        return await ReadDefinitionAsync(stream);
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task Rewrite_SparseColumnMapKeepsNativeSlotShapeAndOpaqueOrderBytes(DatabaseFormat format)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, sparseMetadata: true);
        DefinitionSnapshot before = await ReadDefinitionAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct))
        {
            await RewriteAsync(writer, "drop");
        }

        AssertPreserved(before, await ReadDefinitionAsync(stream));
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        Assert.Single(await reader.SeekRowsAsync("T", "IX", [20], Ct).ToListAsync(Ct));
        Assert.Single(await reader.SeekRowsAsync("T", "PK", [2], Ct).ToListAsync(Ct));
    }

    [Theory]
    [MemberData(nameof(Changes))]
    public async Task Rewrite_StandaloneForeignKeyKeepsDescendingDirection(DatabaseFormat format, string operation, WriteMode mode)
    {
        await using MemoryStream stream = await CreateMarkedDatabaseAsync(format, descendingForeignKey: true);
        DefinitionSnapshot before = await ReadDefinitionAsync(stream);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, WriteModes.WriterOptions(mode), leaveOpen: true, Ct))
        {
            await WriteModes.RunAsync(writer, mode, () => RewriteAsync(writer, operation), Ct);
        }

        AssertPreserved(before, await ReadDefinitionAsync(stream));
        stream.Position = 0;
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        IndexMetadata foreign = Assert.Single(await reader.ListIndexesAsync("T", Ct), index => index.IsForeignKey && index.Columns[0].Name == "ParentId");
        Assert.False(foreign.Columns[0].IsAscending);
        Assert.Single(await reader.SeekRowsAsync("T", foreign.Name, [1], Ct).ToListAsync(Ct));
    }

    private static async Task<MemoryStream> CreateMarkedDatabaseAsync(DatabaseFormat format, bool conflictingMetadata = false, bool sparseMetadata = false, bool descendingForeignKey = false, bool sharedOrdinary = false, bool ignoreNullForeignKey = false)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, WriteModes.WriterOptions(WriteMode.Direct), leaveOpen: true, Ct))
        {
            var indexes = new List<IndexDefinition> { new("PK", "Id") { IsPrimaryKey = true }, new("IX", "K") };
            if (conflictingMetadata)
            {
                indexes.Add(new IndexDefinition("ZZDuplicate", "Id") { IsUnique = true });
            }

            await writer.CreateTableAsync("T", [new ColumnDefinition("Spare", typeof(int)), new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("K", typeof(int))], indexes, Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("Self", "T", "Id", "T", "ParentId"), Ct);
            await writer.InsertRowAsync("T", [7, 1, DBNull.Value, 10], Ct);
            await writer.InsertRowAsync("T", [8, 2, 1, 20], Ct);
            ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("T", Ct);
            LogicalTDefChain chain = await writer.Database.TableDefs.ReadTDefChainAsync(table.Entry.TDefPage, Ct);
            TDefImage image = Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, chain.Bytes));
            foreach ((TDefIndexDescriptor index, int number) in image.RealIndexes.Select((index, number) => (index, number)))
            {
                int columnMap = index.Slot.PhysStart + writer.Database.Format.Index.ColMapStartWithinPhys;
                for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
                {
                    chain.Bytes[columnMap + (slot * Constants.TableDefinition.ColMapSlotSize) + 2] |= 0x40;
                }

                if (!writer.Database.Format.IsJet3)
                {
                    Array.Fill(chain.Bytes, (byte)0xD3, index.Slot.PhysStart + 42, 4);
                    Array.Fill(chain.Bytes, (byte)0xD5, index.Slot.PhysStart + 47, 5);
                    int bookkeeping = writer.Database.Format.TDef.BlockEnd + (number * writer.Database.Format.TDef.RealIdxEntrySz);
                    Array.Fill(chain.Bytes, (byte)0xD7, bookkeeping, 4);
                    Wi32(chain.Bytes, bookkeeping + 4, 0x0BADCAFE);
                    Array.Fill(chain.Bytes, (byte)0xD9, bookkeeping + 8, 4);
                }
            }

            if (!writer.Database.Format.IsJet3)
            {
                foreach (TDefLogicalIndexDescriptor index in image.LogicalIndexes)
                {
                    Array.Fill(chain.Bytes, (byte)0xDB, index.Entry.FieldsOffset + 20, 4);
                }
            }

            if (sparseMetadata)
            {
                foreach (TDefIndexDescriptor index in image.RealIndexes)
                {
                    int first = index.Slot.PhysStart + writer.Database.Format.Index.ColMapStartWithinPhys;
                    int last = first + ((Constants.TableDefinition.ColMapSlotCount - 1) * Constants.TableDefinition.ColMapSlotSize);
                    byte[] entry = chain.Bytes.AsSpan(first, Constants.TableDefinition.ColMapSlotSize).ToArray();
                    chain.Bytes.AsSpan(last, Constants.TableDefinition.ColMapSlotSize).CopyTo(chain.Bytes.AsSpan(first));
                    entry.CopyTo(chain.Bytes.AsSpan(last));
                }
            }

            if (descendingForeignKey || ignoreNullForeignKey)
            {
                int parentColumn = table.Definition.FindColumn("ParentId")!.ColNum;
                TDefLogicalIndexDescriptor index = Assert.Single(image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey && image.RealIndexes[index.Entry.IndexNum2].Columns[0].ColNum == parentColumn);
                int order = image.RealIndexes[index.Entry.IndexNum2].Slot.PhysStart + writer.Database.Format.Index.ColMapStartWithinPhys + 2;
                if (descendingForeignKey)
                {
                    chain.Bytes[order] &= 0xFE;
                }

                if (ignoreNullForeignKey)
                {
                    chain.Bytes[writer.Database.Format.Index.FlagsAbsoluteOffset(image.RealIndexes[index.Entry.IndexNum2].Slot.PhysStart)] |= Constants.TableDefinition.IgnoreNullsIndexFlag;
                }
            }

            if (conflictingMetadata)
            {
                TDefLogicalIndexDescriptor duplicate = Assert.Single(image.LogicalIndexes, index => index.Name == "ZZDuplicate");
                TDefLogicalIndexDescriptor parent = Assert.Single(image.LogicalIndexes, index => index.Entry.IndexType == IndexKind.ForeignKey && index.Entry.RelTblPage != 0 && image.RealIndexes[index.Entry.IndexNum2].Columns[0].ColNum == table.Definition.FindColumn("Id")!.ColNum);
                Assert.NotEqual(duplicate.Entry.IndexNum2, parent.Entry.IndexNum2);
                Wi32(chain.Bytes, parent.Entry.FieldsOffset + 4, duplicate.Entry.IndexNum2);
                if (sharedOrdinary)
                {
                    TDefLogicalIndexDescriptor primary = Assert.Single(image.LogicalIndexes, index => index.Name == "PK");
                    Wi32(chain.Bytes, duplicate.Entry.FieldsOffset + 4, primary.Entry.IndexNum2);
                }
            }

            await chain.WriteInPlaceAsync(writer.Pager.ReadPageAsync, PageBuffers.Return, writer.Pager.WritePageAsync, Ct);
            if (descendingForeignKey || ignoreNullForeignKey)
            {
                writer.Database.TableDefs.OnInvalidateAll();
                await writer.Services.Indexes.MaintainIndexesAsync(table.Entry.TDefPage, table.Definition, "T", Ct);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static Task RewriteAsync(AccessWriter writer, string operation)
        => operation switch
        {
            "add" => writer.AddColumnAsync("T", new ColumnDefinition("Added", typeof(int)), Ct).AsTask(),
            "drop" => writer.DropColumnAsync("T", "Spare", Ct).AsTask(),
            _ => writer.RenameColumnAsync("T", "K", "Renamed", Ct).AsTask(),
        };

    private static async Task<DefinitionSnapshot> ReadDefinitionAsync(MemoryStream stream, string tableName = "T")
    {
        stream.Position = 0;
        await using AccessWriter writer = await TestDatabase.OpenWriterAsync(stream, cancellationToken: Ct);
        ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync(tableName, Ct);
        byte[] bytes = Assert.IsType<byte[]>(await writer.Database.TableDefs.ReadTDefBytesAsync(table.Entry.TDefPage, Ct));
        return new DefinitionSnapshot(writer.Database.Format, table.Entry.TDefPage, Assert.IsType<TDefImage>(TDefCodec.Parse(writer.Database.Format, bytes)), bytes);
    }

    private static void AssertPreserved(DefinitionSnapshot before, DefinitionSnapshot after)
    {
        Assert.Equal(before.Root, after.Root);
        Assert.Equal(before.Image.RealIndexes.Count, after.Image.RealIndexes.Count);
        foreach (TDefLogicalIndexDescriptor left in before.Image.LogicalIndexes)
        {
            foreach (TDefLogicalIndexDescriptor right in before.Image.LogicalIndexes)
            {
                TDefLogicalIndexDescriptor afterLeft = Assert.Single(after.Image.LogicalIndexes, index => index.Name == left.Name);
                TDefLogicalIndexDescriptor afterRight = Assert.Single(after.Image.LogicalIndexes, index => index.Name == right.Name);
                Assert.Equal(left.Entry.IndexNum2 == right.Entry.IndexNum2, afterLeft.Entry.IndexNum2 == afterRight.Entry.IndexNum2);
            }
        }

        foreach (TDefLogicalIndexDescriptor source in before.Image.LogicalIndexes)
        {
            TDefLogicalIndexDescriptor target = Assert.Single(after.Image.LogicalIndexes, index => index.Name == source.Name);
            byte[] expectedLogical = source.RawDescriptor.ToArray();
            byte[] actualLogical = target.RawDescriptor.ToArray();
            int fields = before.Format.Index.LogicalEntryFieldsOffset;
            actualLogical.AsSpan(fields, 17).CopyTo(expectedLogical.AsSpan(fields));
            Assert.Equal(expectedLogical, actualLogical);
            TDefIndexDescriptor sourcePhysical = before.Image.RealIndexes[source.Entry.IndexNum2];
            TDefIndexDescriptor targetPhysical = after.Image.RealIndexes[target.Entry.IndexNum2];
            byte[] expectedPhysical = sourcePhysical.RawDescriptor.ToArray();
            byte[] actualPhysical = targetPhysical.RawDescriptor.ToArray();
            int map = before.Format.Index.ColMapStartWithinPhys;
            for (int slot = 0; slot < Constants.TableDefinition.ColMapSlotCount; slot++)
            {
                Assert.Equal(Ru16(expectedPhysical, map + (slot * Constants.TableDefinition.ColMapSlotSize)) == ushort.MaxValue, Ru16(actualPhysical, map + (slot * Constants.TableDefinition.ColMapSlotSize)) == ushort.MaxValue);
                actualPhysical.AsSpan(map + (slot * Constants.TableDefinition.ColMapSlotSize), 2).CopyTo(expectedPhysical.AsSpan(map + (slot * Constants.TableDefinition.ColMapSlotSize)));
            }

            int references = before.Format.Index.FirstDpAbsoluteOffset(0) - 4;
            actualPhysical.AsSpan(references, 8).CopyTo(expectedPhysical.AsSpan(references));
            Assert.Equal(expectedPhysical, actualPhysical);
            int width = before.Format.TDef.RealIdxEntrySz;
            int sourceBookkeeping = before.Format.TDef.BlockEnd + (source.Entry.IndexNum2 * width);
            int targetBookkeeping = after.Format.TDef.BlockEnd + (target.Entry.IndexNum2 * width);
            byte[] expectedBookkeeping = before.Bytes.AsSpan(sourceBookkeeping, width).ToArray();
            byte[] actualBookkeeping = after.Bytes.AsSpan(targetBookkeeping, width).ToArray();
            if (!before.Format.IsJet3)
            {
                Assert.Equal(2, Ri32(after.Bytes, targetBookkeeping + 4));
                actualBookkeeping.AsSpan(4, 4).CopyTo(expectedBookkeeping.AsSpan(4));
            }

            Assert.Equal(expectedBookkeeping, actualBookkeeping);
        }
    }

    private sealed record DefinitionSnapshot(JetFormat Format, long Root, TDefImage Image, byte[] Bytes);
}
