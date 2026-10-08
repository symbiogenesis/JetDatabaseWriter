namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Buffers.Binary;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

public sealed class ComplexColumnCatalogValidationTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnsupportedScalarComplexCodec_PreservesOpaqueMetadata(bool strict)
    {
        await using AccessReader reader = await AccessReader.OpenAsync(TestDatabases.UnsupportedFieldsTestV2007, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, ComplexColumnTestSupport.Ct);
        System.Collections.Generic.IReadOnlyList<ColumnMetadata> metadata = await reader.GetColumnMetadataAsync("Test", ComplexColumnTestSupport.Ct);
        ColumnMetadata unsupported = Assert.Single(metadata, column => column.Name == "UnknownComplex");
        Assert.Equal("Multi-value 0x48", unsupported.TypeName);
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await reader.GetMultiValueItemsAsync("Test", "UnknownComplex", ComplexColumnTestSupport.Ct));
        Assert.Contains("0x48", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(false, false)]
    public async Task ComplexColumns_MissingOrDuplicateParentDescriptorIdentity_IsCorruption(bool strict, bool duplicate)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("First", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Second", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            ResolvedTable parent = await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", ComplexColumnTestSupport.Ct);
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(parent.Entry.TDefPage, ComplexColumnTestSupport.Ct);
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.TDef.NumRealIdx, 4));
            int descriptors = harness.Database.Format.TDef.BlockEnd + (realIndexes * harness.Database.Format.TDef.RealIdxEntrySz);
            int descriptorIndex = duplicate ? 1 : 0;
            int offset = descriptors + (descriptorIndex * harness.Database.Format.ColumnDescriptor.Size) + harness.Database.Format.ColumnDescriptor.MiscOff;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(offset, 4), duplicate ? parent.Definition.Columns[0].Misc : 0);
            await harness.Pager.WritePageAsync(parent.Entry.TDefPage, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            using DataTable rejected = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        });
    }

    [Theory]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    public async Task ComplexColumns_AttachmentPayloadNamesAndTypesMustAgree(bool strict, bool swapTypes, bool zeroTemplate)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value], ComplexColumnTestSupport.Ct);
            await writer.AddAttachmentAsync("Docs", "Files", new System.Collections.Generic.Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("Seed.txt", [1, 2, 3]), ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        long flatPage;
        await using (AccessReader inspection = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            flatPage = Assert.Single(await inspection.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct)).FlatTableId;
        }

        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            if (zeroTemplate)
            {
                long metadataPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
                TableDef metadata = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(metadataPage, ComplexColumnTestSupport.Ct));
                RowLocation location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(metadataPage, ComplexColumnTestSupport.Ct));
                ColumnInfo templateId = Assert.IsType<ColumnInfo>(metadata.FindColumn("ComplexTypeObjectID"));
                byte[] metadataBytes = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
                BinaryPrimitives.WriteInt32LittleEndian(metadataBytes.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + templateId.FixedOff, 4), 0);
                await harness.Pager.WritePageAsync(location.PageNumber, metadataBytes, ComplexColumnTestSupport.Ct);
            }

            TableDef flat = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(flatPage, ComplexColumnTestSupport.Ct));
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(flatPage, ComplexColumnTestSupport.Ct);
            if (swapTypes)
            {
                int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.TDef.NumRealIdx, 4));
                int descriptors = harness.Database.Format.TDef.BlockEnd + (realIndexes * harness.Database.Format.TDef.RealIdxEntrySz);
                int dataType = descriptors + (flat.FindColumnIndex("FileData") * harness.Database.Format.ColumnDescriptor.Size) + harness.Database.Format.ColumnDescriptor.TypeOff;
                int urlType = descriptors + (flat.FindColumnIndex("FileURL") * harness.Database.Format.ColumnDescriptor.Size) + harness.Database.Format.ColumnDescriptor.TypeOff;
                (page[dataType], page[urlType]) = (page[urlType], page[dataType]);
            }
            else
            {
                int offset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes("FileData"));
                Assert.True(offset >= 0);
                page[offset] = (byte)'X';
            }

            await harness.Pager.WritePageAsync(flatPage, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        if (strict)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        }
        else
        {
            Assert.Empty(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
            using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
            Assert.IsType<DBNull>(Assert.Single(table.Rows.Cast<DataRow>())["Files"]);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexColumns_MissingRequiredMapping_IsCorruption(bool strict)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            long metadataPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            RowLocation location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(metadataPage, ComplexColumnTestSupport.Ct));
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
            int offset = harness.Database.Format.DataPage.RowsStart + (location.RowIndex * sizeof(ushort));
            ushort slot = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(offset, sizeof(ushort)));
            BinaryPrimitives.WriteUInt16LittleEndian(page.AsSpan(offset, sizeof(ushort)), (ushort)(slot | Constants.DataPage.DeletedRowFlag));
            await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexColumns_TemplateNameOnNonTableObject_IsRefused(bool strict)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            TableDef objects = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, ComplexColumnTestSupport.Ct));
            CatalogRow template = Assert.Single(await harness.Services.CatalogRows.GetCatalogRowsAsync(objects, ComplexColumnTestSupport.Ct), entry => entry.Name == "MSysComplexType_Attachment");
            RowLocation location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(2, ComplexColumnTestSupport.Ct), row => row.PageNumber == template.PageNumber && row.RowIndex == template.RowIndex);
            ColumnInfo type = Assert.IsType<ColumnInfo>(objects.FindColumn("Type"));
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
            BinaryPrimitives.WriteInt16LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + type.FixedOff, 2), 5);
            await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        if (strict)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        }
        else
        {
            Assert.Empty(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexDiscovery_FallbackBudgetIsSharedAcrossColumns(bool strict)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("First", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Second", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("Docs", [DBNull.Value, DBNull.Value], ComplexColumnTestSupport.Ct);
        }

        int budget;
        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            TableDef objects = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, ComplexColumnTestSupport.Ct));
            budget = (await harness.Services.CatalogRows.GetCatalogRowsAsync(objects, ComplexColumnTestSupport.Ct)).Count + 2;
            long metadataPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            TableDef metadata = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(metadataPage, ComplexColumnTestSupport.Ct));
            ColumnInfo identity = Assert.IsType<ColumnInfo>(metadata.FindColumn("ComplexID"));
            foreach (RowLocation location in await harness.Database.GetLiveRowLocationsAsync(metadataPage, ComplexColumnTestSupport.Ct))
            {
                byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
                BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + identity.FixedOff, 4), 0);
                await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
            }
        }

        stream.Position = 0;
        await using (AccessReader limited = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxComplexDiscoveryEntries = budget }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            {
                using DataTable rejected = await limited.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
            });
            Assert.Equal(JetErrorCode.ComplexDiscoveryBudgetExceeded, error.ErrorCode);
        }

        stream.Position = 0;
        await using AccessReader permitted = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxComplexDiscoveryEntries = 1000 }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        using DataTable table = await permitted.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        Assert.Single(table.Rows.Cast<DataRow>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexColumns_MissingRequiredCatalog_IsCorruption(bool strict)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            TableDef catalog = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(2, ComplexColumnTestSupport.Ct));
            CatalogRow entry = Assert.Single(await harness.Services.CatalogRows.GetCatalogRowsAsync(catalog, ComplexColumnTestSupport.Ct), row => row.Name == "MSysComplexColumns");
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(entry.PageNumber, ComplexColumnTestSupport.Ct);
            int offset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes("MSysComplexColumns"));
            if (offset < 0)
            {
                offset = page.AsSpan().IndexOf(Encoding.UTF8.GetBytes("MSysComplexColumns"));
            }

            Assert.True(offset >= 0);
            page[offset] = (byte)'X';
            await harness.Pager.WritePageAsync(entry.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ComplexDiscovery_NonpositiveBudget_IsRejected(int budget)
    {
        await using var stream = new MemoryStream();
        await using (await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
        }

        stream.Position = 0;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessReader rejected = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, MaxComplexDiscoveryEntries = budget }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexDiscovery_BudgetRefusalPropagates_AndOverrideSucceeds(bool strict)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("First", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Second", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using (AccessReader limited = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxComplexDiscoveryEntries = 1 }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            JetLimitationException error = await Assert.ThrowsAsync<JetLimitationException>(async () => await limited.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
            Assert.Equal(JetErrorCode.ComplexDiscoveryBudgetExceeded, error.ErrorCode);
        }

        stream.Position = 0;
        await using AccessReader permitted = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict, MaxComplexDiscoveryEntries = 1000 }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        Assert.Equal(2, (await permitted.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct)).Count);
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    public async Task ComplexColumns_TemplateAndFlatSchemaDisagree_RefusesDescriptor(bool strict, bool damageTemplate, bool unknownCodec)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Values", typeof(int)) { IsMultiValue = true, MultiValueElementType = typeof(int) }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader inspection = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            ComplexColumnInfo info = Assert.Single(await inspection.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
            long targetPage = damageTemplate ? info.ComplexTypeObjectId : info.FlatTableId;
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(targetPage, ComplexColumnTestSupport.Ct);
            int realIndexes = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(harness.Database.Format.TDef.NumRealIdx, 4));
            int descriptor = harness.Database.Format.TDef.BlockEnd + (realIndexes * harness.Database.Format.TDef.RealIdxEntrySz);
            TableDef flat = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(targetPage, ComplexColumnTestSupport.Ct));
            int valueIndex = flat.FindColumnIndex("Value");
            page[descriptor + (valueIndex * harness.Database.Format.ColumnDescriptor.Size) + harness.Database.Format.ColumnDescriptor.TypeOff] = unknownCodec ? (byte)0x48 : (byte)JetDatabaseWriter.Enums.ColumnType.FloatType;
            await harness.Pager.WritePageAsync(targetPage, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        if (strict)
        {
            await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        }
        else
        {
            Assert.Empty(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ComplexColumns_ReadFailure_PropagatesAndDoesNotPoisonRetry(bool strict)
    {
        await using var stream = new WriteFaultStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        stream.FailOnRead(1);
        await Assert.ThrowsAsync<IOException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        Assert.True(stream.Faulted);
        Assert.Equal("Files", Assert.Single(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct)).ColumnName);
    }

    [Theory]
    [InlineData(true, "ColumnName")]
    [InlineData(false, "ColumnName")]
    [InlineData(true, "ComplexID")]
    [InlineData(false, "ComplexID")]
    [InlineData(true, "FlatTableID")]
    [InlineData(false, "FlatTableID")]
    [InlineData(true, "ConceptualTableID")]
    [InlineData(false, "ConceptualTableID")]
    [InlineData(true, "ComplexTypeObjectID")]
    [InlineData(false, "ComplexTypeObjectID")]
    public async Task ComplexColumns_MissingRequiredColumn_IsCorruptionInEveryMode(bool strict, string missing)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader inspection = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            long pageNumber = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(pageNumber, ComplexColumnTestSupport.Ct);
            int nameOffset = page.AsSpan().IndexOf(Encoding.Unicode.GetBytes(missing));
            Assert.True(nameOffset >= 0);
            page[nameOffset] = (byte)'X';
            await harness.Pager.WritePageAsync(pageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
        Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
        Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
        JetCorruptDataException scanError = await Assert.ThrowsAsync<JetCorruptDataException>(async () =>
        {
            using DataTable rejected = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
        });
        Assert.Equal(JetErrorCode.CorruptCatalog, scanError.ErrorCode);
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(false, false, true)]
    public async Task ComplexColumns_InvalidTemplateIdentity_DoesNotLoseValidSibling(bool strict, bool wrongKind, bool duplicate)
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await ComplexColumnTestSupport.CreateWriterAsync(stream))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Broken", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Valid", typeof(byte[])) { IsAttachment = true }], ComplexColumnTestSupport.Ct);
            await writer.InsertRowAsync("Docs", [1, DBNull.Value, DBNull.Value], ComplexColumnTestSupport.Ct);
            await writer.AddAttachmentAsync("Docs", "Broken", new System.Collections.Generic.Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("Broken.txt", [1]), ComplexColumnTestSupport.Ct);
            await writer.AddAttachmentAsync("Docs", "Valid", new System.Collections.Generic.Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("Valid.txt", [2]), ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader inspection = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        await using (WriterHarness harness = await WriterHarness.OpenAsync(stream, cancellationToken: ComplexColumnTestSupport.Ct))
        {
            long pageNumber = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", ComplexColumnTestSupport.Ct);
            TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(pageNumber, ComplexColumnTestSupport.Ct));
            ColumnInfo column = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexTypeObjectID"));
            RowLocation location = (await harness.Database.GetLiveRowLocationsAsync(pageNumber, ComplexColumnTestSupport.Ct))[0];
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, ComplexColumnTestSupport.Ct);
            int invalidIdentity = wrongKind ? checked((int)(await harness.Services.Catalog.ResolveRequiredTableAsync("Docs", ComplexColumnTestSupport.Ct)).Entry.TDefPage) : int.MaxValue;
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + column.FixedOff, 4), invalidIdentity);
            if (duplicate)
            {
                ColumnInfo complexId = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexID"));
                BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + complexId.FixedOff, 4), 2);
            }

            await harness.Pager.WritePageAsync(location.PageNumber, page, ComplexColumnTestSupport.Ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = strict }, leaveOpen: true, cancellationToken: ComplexColumnTestSupport.Ct);
        for (int attempt = 0; attempt < 2; attempt++)
        {
            if (strict)
            {
                JetCorruptDataException error = await Assert.ThrowsAsync<JetCorruptDataException>(async () => await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
                Assert.Equal(JetErrorCode.CorruptCatalog, error.ErrorCode);
                Assert.Equal("MSysComplexColumns", error.ErrorInfo.TableName);
                Assert.Equal("ComplexTypeObjectID", error.ErrorInfo.ColumnName);
            }
            else
            {
                ComplexColumnInfo valid = Assert.Single(await reader.GetComplexColumnsAsync("Docs", ComplexColumnTestSupport.Ct));
                Assert.Equal("Valid", valid.ColumnName);
                using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: ComplexColumnTestSupport.Ct);
                DataRow row = Assert.Single(table.Rows.Cast<DataRow>());
                Assert.IsType<DBNull>(row["Broken"]);
                Assert.Equal("Valid.txt", Assert.Single(ComplexCellValue.ReadAttachments(Assert.IsType<byte[]>(row["Valid"]))).FileName);
            }
        }
    }
}
