namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using Xunit;

public sealed class DataRemanenceTests
{
    private const int MarkerLength = 16;

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeleteRows_MarksSlotDeletedWithoutScrubbingRowPayload(DatabaseFormat format)
    {
        byte[] deletedPayload = BuildPayload(64, 0x31);
        byte[] deletedMarker = MarkerOf(deletedPayload);
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await CreateInlinePayloadTableAsync(writer, "DeleteRows", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "DeleteRows",
                [1, deletedPayload],
                TestContext.Current.CancellationToken);
        }

        byte[] beforeDelete = stream.ToArray();
        var originalSlot = AssertSingleLiveRowContaining(beforeDelete, format, deletedMarker);

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync(
                "DeleteRows",
                "Id",
                1,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, deleted);
        }

        byte[] afterDelete = stream.ToArray();
        var deletedSlot = ReadRowSnapshot(afterDelete, format, originalSlot.PageNumber, originalSlot.RowIndex);

        Assert.True(deletedSlot.IsDeleted);
        Assert.Equal(originalSlot.Start, deletedSlot.Start);
        Assert.Equal(originalSlot.Bytes, deletedSlot.Bytes);
        Assert.True(ContainsSequence(afterDelete, deletedMarker));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UpdateRows_PreservesOldRowPayloadInDeletedSlot(DatabaseFormat format)
    {
        byte[] originalPayload = BuildPayload(64, 0x41);
        byte[] replacementPayload = BuildPayload(64, 0x52);
        byte[] originalMarker = MarkerOf(originalPayload);
        byte[] replacementMarker = MarkerOf(replacementPayload);
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await CreateInlinePayloadTableAsync(writer, "UpdateRows", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "UpdateRows",
                [1, originalPayload],
                TestContext.Current.CancellationToken);
        }

        byte[] beforeUpdate = stream.ToArray();
        var originalSlot = AssertSingleLiveRowContaining(beforeUpdate, format, originalMarker);

        await using (var writer = await OpenWriterAsync(stream))
        {
            int updated = await writer.UpdateRowsAsync(
                "UpdateRows",
                "Id",
                1,
                new Dictionary<string, object?> { ["Payload"] = replacementPayload },
                TestContext.Current.CancellationToken);

            Assert.Equal(1, updated);
        }

        byte[] afterUpdate = stream.ToArray();
        var oldSlot = ReadRowSnapshot(afterUpdate, format, originalSlot.PageNumber, originalSlot.RowIndex);
        var replacementSlot = AssertSingleLiveRowContaining(afterUpdate, format, replacementMarker);

        Assert.True(oldSlot.IsDeleted);
        Assert.Equal(originalSlot.Start, oldSlot.Start);
        Assert.Equal(originalSlot.Bytes, oldSlot.Bytes);
        Assert.NotEqual((originalSlot.PageNumber, originalSlot.RowIndex), (replacementSlot.PageNumber, replacementSlot.RowIndex));
        Assert.True(ContainsSequence(afterUpdate, originalMarker));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task UpdateAndDelete_LongOleValues_RetainOldLvalPages(DatabaseFormat format)
    {
        byte[] originalPayload = BuildPayload(9000, 0x61);
        byte[] replacementPayload = BuildPayload(9000, 0x72);
        byte[] originalMarker = MarkerOf(originalPayload);
        byte[] replacementMarker = MarkerOf(replacementPayload);
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "LongValues",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "LongValues",
                [1, originalPayload],
                TestContext.Current.CancellationToken);
        }

        byte[] afterInsert = stream.ToArray();
        int lvalPagesAfterInsert = CountLvalPages(afterInsert, format);
        Assert.True(lvalPagesAfterInsert >= 2);
        Assert.True(ContainsSequence(afterInsert, originalMarker));

        await using (var writer = await OpenWriterAsync(stream))
        {
            int updated = await writer.UpdateRowsAsync(
                "LongValues",
                "Id",
                1,
                new Dictionary<string, object?> { ["Blob"] = replacementPayload },
                TestContext.Current.CancellationToken);

            Assert.Equal(1, updated);
        }

        byte[] afterUpdate = stream.ToArray();
        int lvalPagesAfterUpdate = CountLvalPages(afterUpdate, format);
        Assert.Equal(lvalPagesAfterInsert * 2, lvalPagesAfterUpdate);
        Assert.True(ContainsSequence(afterUpdate, originalMarker));
        Assert.True(ContainsSequence(afterUpdate, replacementMarker));

        await using (var reader = await OpenReaderAsync(stream))
        {
            var table = await reader.ReadDataTableAsync(
                "LongValues",
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(1, table.Rows.Count);
            var row = table.Rows[0];
            byte[] actual = Assert.IsType<byte[]>(row["Blob"]);
            Assert.Equal(replacementPayload, actual);
        }

        await using (var writer = await OpenWriterAsync(stream))
        {
            int deleted = await writer.DeleteRowsAsync(
                "LongValues",
                "Id",
                1,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, deleted);
        }

        byte[] afterDelete = stream.ToArray();
        Assert.Equal(lvalPagesAfterUpdate, CountLvalPages(afterDelete, format));
        Assert.True(ContainsSequence(afterDelete, originalMarker));
        Assert.True(ContainsSequence(afterDelete, replacementMarker));

        await using (var reader = await OpenReaderAsync(stream))
        {
            var table = await reader.ReadDataTableAsync(
                "LongValues",
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(0, table.Rows.Count);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeleteRows_SecureEraseMode_ScrubsDeletedRowPayload(DatabaseFormat format)
    {
        byte[] deletedPayload = BuildPayload(64, 0x51);
        byte[] deletedMarker = MarkerOf(deletedPayload);
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await CreateInlinePayloadTableAsync(writer, "SecureDeleteRows", TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(
                "SecureDeleteRows",
                [1, deletedPayload],
                TestContext.Current.CancellationToken);
        }

        byte[] beforeDelete = stream.ToArray();
        var originalSlot = AssertSingleLiveRowContaining(beforeDelete, format, deletedMarker);

        await using (var writer = await OpenWriterAsync(
            stream,
            new AccessWriterOptions
            {
                UseLockFile = false,
                SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
            }))
        {
            int deleted = await writer.DeleteRowsAsync(
                "SecureDeleteRows",
                "Id",
                1,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, deleted);
        }

        byte[] afterDelete = stream.ToArray();
        var deletedSlot = ReadRowSnapshot(afterDelete, format, originalSlot.PageNumber, originalSlot.RowIndex);

        Assert.True(deletedSlot.IsDeleted);
        Assert.False(ContainsSequence(deletedSlot.Bytes, deletedMarker));
        Assert.False(ContainsSequence(afterDelete, deletedMarker));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task DeleteRows_SecureEraseMode_ScrubsAndFreesLongOlePages(DatabaseFormat format)
    {
        byte[] payload = BuildPayload(9000, 0x33);
        byte[] marker = MarkerOf(payload);
        await using var stream = await CreateFreshStreamAsync(format);

        await using (var writer = await OpenWriterAsync(stream))
        {
            await writer.CreateTableAsync(
                "SecureLongValues",
                [
                    new ColumnDefinition("Id", typeof(int)),
                    new ColumnDefinition("Blob", typeof(byte[])),
                ],
                TestContext.Current.CancellationToken);

            await writer.InsertRowAsync(
                "SecureLongValues",
                [1, payload],
                TestContext.Current.CancellationToken);
        }

        byte[] afterInsert = stream.ToArray();
        Assert.True(CountLvalPages(afterInsert, format) >= 2);
        Assert.True(ContainsSequence(afterInsert, marker));

        await using (var writer = await OpenWriterAsync(
            stream,
            new AccessWriterOptions
            {
                UseLockFile = false,
                SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
            }))
        {
            int deleted = await writer.DeleteRowsAsync(
                "SecureLongValues",
                "Id",
                1,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, deleted);
        }

        byte[] afterDelete = stream.ToArray();
        Assert.Equal(0, CountLvalPages(afterDelete, format));
        Assert.False(ContainsSequence(afterDelete, marker));

        await using (var reader = await OpenReaderAsync(stream))
        {
            var table = await reader.ReadDataTableAsync(
                "SecureLongValues",
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(0, table.Rows.Count);
        }
    }

    private static async ValueTask CreateInlinePayloadTableAsync(AccessWriter writer, string tableName, System.Threading.CancellationToken cancellationToken)
    {
        await writer.CreateTableAsync(
            tableName,
            [
                new ColumnDefinition("Id", typeof(int)),
                new ColumnDefinition("Payload", typeof(byte[]), maxLength: 128),
            ],
            cancellationToken);
    }

    private static async ValueTask<MemoryStream> CreateFreshStreamAsync(DatabaseFormat format)
    {
        var stream = new MemoryStream();
        await using (await AccessWriter.CreateDatabaseAsync(
            stream,
            format,
            new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken))
        {
        }

        stream.Position = 0;
        return stream;
    }

    private static ValueTask<AccessWriter> OpenWriterAsync(MemoryStream stream, AccessWriterOptions? options = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            options ?? new AccessWriterOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(
            stream,
            new AccessReaderOptions { UseLockFile = false },
            leaveOpen: true,
            TestContext.Current.CancellationToken);
    }

    private static RowSnapshot AssertSingleLiveRowContaining(byte[] fileBytes, DatabaseFormat format, byte[] marker)
    {
        var matches = FindRowsContaining(fileBytes, format, marker, liveOnly: true);
        return Assert.Single(matches);
    }

    private static List<RowSnapshot> FindRowsContaining(byte[] fileBytes, DatabaseFormat format, byte[] marker, bool liveOnly)
    {
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        int pageCount = fileBytes.Length / pageSize;
        var matches = new List<RowSnapshot>();

        for (int pageNumber = 1; pageNumber < pageCount; pageNumber++)
        {
            ReadOnlySpan<byte> page = fileBytes.AsSpan(pageNumber * pageSize, pageSize);
            if (page[0] != 0x01 || IsLvalPage(page))
            {
                continue;
            }

            int parentTdefPage = BinaryPrimitives.ReadInt32LittleEndian(page.Slice(layout.TDefOff, 4));
            if (parentTdefPage <= 0)
            {
                continue;
            }

            int rowCount = ReadUInt16(page, layout.NumRows);
            int maxRowCount = Math.Min(rowCount, (pageSize - layout.RowsStart) / 2);
            for (int rowIndex = 0; rowIndex < maxRowCount; rowIndex++)
            {
                var snapshot = ReadRowSnapshot(fileBytes, format, pageNumber, rowIndex);
                if (liveOnly && !snapshot.IsLive)
                {
                    continue;
                }

                if (ContainsSequence(snapshot.Bytes, marker))
                {
                    matches.Add(snapshot);
                }
            }
        }

        return matches;
    }

    private static RowSnapshot ReadRowSnapshot(byte[] fileBytes, DatabaseFormat format, int pageNumber, int rowIndex)
    {
        int pageSize = PageSizeOf(format);
        DataPageLayout layout = DataPageLayout.For(format);
        ReadOnlySpan<byte> page = fileBytes.AsSpan(pageNumber * pageSize, pageSize);
        int rowCount = ReadUInt16(page, layout.NumRows);

        Assert.True(rowIndex >= 0 && rowIndex < rowCount);

        var starts = new List<int>(rowCount);
        for (int slotIndex = 0; slotIndex < rowCount; slotIndex++)
        {
            int slotOffset = layout.RowsStart + (slotIndex * 2);
            int rawOffset = ReadUInt16(page, slotOffset);
            int rowStart = rawOffset & 0x1FFF;
            if (rowStart > 0)
            {
                starts.Add(rowStart);
            }
        }

        starts.Sort();

        int targetRawOffset = ReadUInt16(page, layout.RowsStart + (rowIndex * 2));
        int targetStart = targetRawOffset & 0x1FFF;
        int sortedIndex = starts.BinarySearch(targetStart);
        Assert.True(sortedIndex >= 0);

        int targetEnd = sortedIndex + 1 < starts.Count ? starts[sortedIndex + 1] : pageSize;
        byte[] rowBytes = page.Slice(targetStart, targetEnd - targetStart).ToArray();

        return new RowSnapshot(
            pageNumber,
            rowIndex,
            targetRawOffset,
            targetStart,
            rowBytes);
    }

    private static int CountLvalPages(byte[] fileBytes, DatabaseFormat format)
    {
        int pageSize = PageSizeOf(format);
        int pageCount = fileBytes.Length / pageSize;
        int count = 0;

        for (int pageNumber = 1; pageNumber < pageCount; pageNumber++)
        {
            ReadOnlySpan<byte> page = fileBytes.AsSpan(pageNumber * pageSize, pageSize);
            if (page[0] == 0x01 && IsLvalPage(page))
            {
                count++;
            }
        }

        return count;
    }

    private static bool IsLvalPage(ReadOnlySpan<byte> page)
        => page.Length >= 8 && page[4] == 'L' && page[5] == 'V' && page[6] == 'A' && page[7] == 'L';

    private static bool ContainsSequence(byte[] bytes, byte[] marker)
        => bytes.AsSpan().IndexOf(marker) >= 0;

    private static int ReadUInt16(ReadOnlySpan<byte> bytes, int offset)
        => BinaryPrimitives.ReadUInt16LittleEndian(bytes.Slice(offset, 2));

    private static int PageSizeOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static byte[] MarkerOf(byte[] payload)
        => payload.AsSpan(0, MarkerLength).ToArray();

    private static byte[] BuildPayload(int length, byte markerByte)
    {
        var payload = new byte[length];
        for (int byteIndex = 0; byteIndex < payload.Length; byteIndex++)
        {
            unchecked
            {
                payload[byteIndex] = (byte)(markerByte + (byteIndex * 37) + (byteIndex >> 2));
            }
        }

        byte[] marker = [0xCA, 0xFE, markerByte, 0xD0, 0x0D, 0xC0, 0xDE, 0x71, 0x5A, 0xB1, 0x6C, 0x3E, 0x99, 0x24, 0x42, 0x18];
        Buffer.BlockCopy(marker, 0, payload, 0, marker.Length);
        return payload;
    }

    private readonly record struct RowSnapshot(
        int PageNumber,
        int RowIndex,
        int RawOffset,
        int Start,
        byte[] Bytes)
    {
        public bool IsLive => (RawOffset & 0xC000) == 0;

        public bool IsDeleted => (RawOffset & 0x8000) != 0;
    }
}
