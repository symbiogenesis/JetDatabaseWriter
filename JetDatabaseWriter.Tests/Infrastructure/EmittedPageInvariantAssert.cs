namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages;
using Xunit;

internal static class EmittedPageInvariantAssert
{
    public static void AllPagesAreWellFormed(byte[] fileBytes, DatabaseFormat format)
    {
        Assert.NotNull(fileBytes);

        int pageSize = PageSizeOf(format);
        Assert.True(fileBytes.Length >= pageSize, "Database image must contain at least one page.");
        Assert.Equal(0, fileBytes.Length % pageSize);

        int pageCount = fileBytes.Length / pageSize;
        var liveRowsByTdefPage = new Dictionary<int, int>();
        var rowCountsByTdefPage = new Dictionary<int, uint>();

        int tdefHeadsChecked = 0;
        int dataPagesChecked = 0;
        int indexPagesChecked = 0;

        for (int pageNumber = 1; pageNumber < pageCount; pageNumber++)
        {
            int pageOffset = pageNumber * pageSize;
            ReadOnlySpan<byte> page = fileBytes.AsSpan(pageOffset, pageSize);
            if (IsAllZero(page))
            {
                continue;
            }

            switch (page[0])
            {
                case 0x01:
                    DataPageSummary dataSummary = AssertDataPage(fileBytes, pageNumber, pageSize, format);
                    dataPagesChecked++;
                    if (dataSummary.ParentTdefPage > 0)
                    {
                        liveRowsByTdefPage.TryGetValue(dataSummary.ParentTdefPage, out int previousLiveRows);
                        liveRowsByTdefPage[dataSummary.ParentTdefPage] = previousLiveRows + dataSummary.LiveRowCount;
                    }

                    break;

                case 0x02:
                    if (AssertTdefPage(fileBytes, pageNumber, pageSize, format, rowCountsByTdefPage))
                    {
                        tdefHeadsChecked++;
                    }

                    break;

                case Constants.IndexLeafPage.PageTypeIntermediate:
                case Constants.IndexLeafPage.PageTypeLeaf:
                    AssertIndexPage(fileBytes, pageNumber, pageSize, format);
                    indexPagesChecked++;
                    break;

                case 0x05:
                    break;

                case 0x09:
                    break;

                default:
                    Assert.Fail(Message(pageNumber, $"Unexpected non-empty page type 0x{page[0]:X2}."));
                    break;
            }
        }

        Assert.True(tdefHeadsChecked > 0, "Expected at least one TDEF head page.");
        Assert.True(dataPagesChecked + indexPagesChecked > 0, "Expected at least one data or index page beyond TDEF metadata.");

        foreach (KeyValuePair<int, uint> pair in rowCountsByTdefPage)
        {
            Assert.True(
                pair.Value <= int.MaxValue,
                Message(pair.Key, $"TDEF row_count {pair.Value} exceeds the test helper's comparison range."));

            liveRowsByTdefPage.TryGetValue(pair.Key, out int liveRows);
            Assert.True(
                liveRows == pair.Value,
                Message(pair.Key, $"TDEF row_count={pair.Value} but decoded live data rows={liveRows}."));
        }
    }

    private static DataPageSummary AssertDataPage(byte[] fileBytes, int pageNumber, int pageSize, DatabaseFormat format)
    {
        ReadOnlySpan<byte> page = PageSpan(fileBytes, pageNumber, pageSize);
        var layout = DataPageLayout.For(format);

        Assert.Equal(0x01, page[1]);
        List<RowSlotInfo> rowSlots = AssertRowSlotDirectory(page, pageNumber, pageSize, layout);

        bool isLval = IsLvalPage(page);
        if (isLval)
        {
            AssertLvalPage(page, pageNumber, rowSlots);
            return new DataPageSummary(ParentTdefPage: 0, LiveRowCount: 0);
        }

        int parentTdefPage = ReadInt32(page, layout.TDefOff);
        Assert.True(parentTdefPage >= 0, Message(pageNumber, $"Data-page parent TDEF is negative ({parentTdefPage})."));
        if (parentTdefPage > 0)
        {
            AssertPageType(fileBytes, pageSize, pageNumber, parentTdefPage, [0x02], "data-page parent TDEF");
        }

        bool isUsageMap = parentTdefPage == 0 && rowSlots.Count > 0;
        if (isUsageMap)
        {
            AssertUsageMapPage(page, pageNumber, rowSlots);
        }

        int liveRows = 0;
        foreach (RowSlotInfo rowSlot in rowSlots)
        {
            if (rowSlot.IsLive)
            {
                liveRows++;
            }
        }

        return new DataPageSummary(parentTdefPage, liveRows);
    }

    private static List<RowSlotInfo> AssertRowSlotDirectory(
        ReadOnlySpan<byte> page,
        int pageNumber,
        int pageSize,
        DataPageLayout layout)
    {
        int freeSpace = ReadUInt16(page, 2);
        int numRows = ReadUInt16(page, layout.NumRows);
        Assert.InRange(numRows, 0, Constants.DataPage.MaxRowsPerPage);

        int slotTableEnd = layout.RowsStart + (numRows * 2);
        Assert.True(slotTableEnd <= pageSize, Message(pageNumber, $"Row-slot table ends at {slotTableEnd}, beyond page size {pageSize}."));

        if (numRows == 0)
        {
            Assert.Equal(pageSize - layout.RowsStart, freeSpace);
            return [];
        }

        var rawOffsets = new int[numRows];
        var starts = new int[numRows];
        for (int rowIndex = 0; rowIndex < numRows; rowIndex++)
        {
            int raw = ReadUInt16(page, layout.RowsStart + (rowIndex * 2));
            int rowStart = raw & 0x1FFF;
            rawOffsets[rowIndex] = raw;
            starts[rowIndex] = rowStart;

            Assert.True(raw != 0, Message(pageNumber, $"Row slot {rowIndex} is zero within numRows={numRows}."));
            Assert.True((raw & 0x2000) == 0, Message(pageNumber, $"Row slot {rowIndex} uses reserved marker bit 0x2000 (raw=0x{raw:X4})."));
            Assert.True(
                rowStart >= slotTableEnd && rowStart < pageSize,
                Message(pageNumber, $"Row slot {rowIndex} starts at {rowStart}, outside [{slotTableEnd}, {pageSize})."));
        }

        Array.Sort(starts);
        for (int sortedIndex = 1; sortedIndex < starts.Length; sortedIndex++)
        {
            Assert.True(
                starts[sortedIndex] > starts[sortedIndex - 1],
                Message(pageNumber, $"Duplicate or unsorted row start {starts[sortedIndex]} in row-slot directory."));
        }

        int expectedFreeSpace = starts[0] - slotTableEnd;
        Assert.Equal(expectedFreeSpace, freeSpace);

        var rowSlots = new List<RowSlotInfo>(numRows);
        for (int rowIndex = 0; rowIndex < numRows; rowIndex++)
        {
            int rowStart = rawOffsets[rowIndex] & 0x1FFF;
            int sortedIndex = Array.BinarySearch(starts, rowStart);
            Assert.True(sortedIndex >= 0, Message(pageNumber, $"Row slot {rowIndex} start {rowStart} was not found after sorting."));
            int rowEnd = sortedIndex + 1 < starts.Length ? starts[sortedIndex + 1] - 1 : pageSize - 1;
            rowSlots.Add(new RowSlotInfo(rowIndex, rawOffsets[rowIndex], rowStart, rowEnd));
        }

        return rowSlots;
    }

    private static void AssertLvalPage(ReadOnlySpan<byte> page, int pageNumber, List<RowSlotInfo> rowSlots)
    {
        Assert.Single(rowSlots);
        RowSlotInfo rowSlot = rowSlots[0];
        Assert.True(rowSlot.IsLive, Message(pageNumber, "LVAL row slot is marked deleted or overflow."));
        Assert.Equal(Constants.LongValue.LvalRowStart, rowSlot.Start);
        Assert.Equal(4, ReadUInt16(page, 2));

        uint token = ReadUInt32(page, 8);
        Assert.True(token != 0, Message(pageNumber, "LVAL token must be non-zero."));
    }

    private static void AssertUsageMapPage(ReadOnlySpan<byte> page, int pageNumber, List<RowSlotInfo> rowSlots)
    {
        const int UsageMapRowSize = 69;
        foreach (RowSlotInfo rowSlot in rowSlots)
        {
            Assert.True(rowSlot.IsLive, Message(pageNumber, $"Usage-map row {rowSlot.RowIndex} is marked deleted or overflow."));
            Assert.Equal(UsageMapRowSize, rowSlot.Size);

            byte mapType = page[rowSlot.Start];
            Assert.True(mapType is 0x00 or 0x01, Message(pageNumber, $"Usage-map row {rowSlot.RowIndex} has unknown type 0x{mapType:X2}."));
            if (mapType == 0x00)
            {
                int basePage = ReadInt32(page, rowSlot.Start + 1);
                Assert.True(basePage >= 0, Message(pageNumber, $"Usage-map row {rowSlot.RowIndex} has negative base page {basePage}."));
                Assert.Equal(0, basePage % 8);
            }
        }
    }

    private static bool AssertTdefPage(
        byte[] fileBytes,
        int pageNumber,
        int pageSize,
        DatabaseFormat format,
        Dictionary<int, uint> rowCountsByTdefPage)
    {
        ReadOnlySpan<byte> page = PageSpan(fileBytes, pageNumber, pageSize);
        var layout = TDefHeaderLayout.For(format);

        Assert.Equal(0x01, page[1]);

        int nextPage = ReadInt32(page, 4);
        if (nextPage != 0)
        {
            AssertPageType(fileBytes, pageSize, pageNumber, nextPage, [0x02], "TDEF continuation");
        }

        if (!IsTdefHead(page, layout))
        {
            return false;
        }

        int numCols = ReadUInt16(page, layout.NumCols);
        int numVarCols = ReadUInt16(page, layout.NumCols - 2);
        int numIdx = ReadInt32(page, layout.NumCols + 2);
        int numRealIdx = ReadInt32(page, layout.NumRealIdx);
        uint rowCount = ReadUInt32(page, Constants.TableDefinition.RowCountOffset);

        Assert.InRange(numCols, 0, 2048);
        Assert.InRange(numVarCols, 0, numCols);
        Assert.InRange(numIdx, 0, 1000);
        Assert.InRange(numRealIdx, 0, 1000);

        int columnStart = layout.BlockEnd + (numRealIdx * layout.RealIdxEntrySz);
        Assert.True(columnStart <= pageSize, Message(pageNumber, $"TDEF column descriptors start at {columnStart}, beyond page size {pageSize}."));

        if (format != DatabaseFormat.Jet3Mdb)
        {
            Assert.Equal(Constants.TableDefinition.Jet4.FormatMagic, ReadInt32(page, 0x0C));
            int tdefLen = ReadInt32(page, 8);
            Assert.True(tdefLen >= 0, Message(pageNumber, $"TDEF length is negative ({tdefLen})."));
            Assert.Equal(Math.Max(0, pageSize - tdefLen - 8), ReadUInt16(page, 2));
        }

        for (int realIndex = 0; realIndex < numRealIdx; realIndex++)
        {
            int countOffset = layout.BlockEnd + (realIndex * layout.RealIdxEntrySz) + 4;
            if (countOffset + 4 > pageSize)
            {
                break;
            }

            uint perIndexCount = ReadUInt32(page, countOffset);
            Assert.True(
                perIndexCount == rowCount,
                Message(pageNumber, $"real-idx {realIndex} num_idx_rows={perIndexCount} but row_count={rowCount}."));
        }

        rowCountsByTdefPage[pageNumber] = rowCount;
        return true;
    }

    private static bool IsTdefHead(ReadOnlySpan<byte> page, TDefHeaderLayout layout)
    {
        if (layout.NumCols - 5 < 0 || layout.NumCols + 2 >= page.Length)
        {
            return false;
        }

        if (page[layout.NumCols - 5] != 0x4E)
        {
            return false;
        }

        int declaredCols = ReadUInt16(page, layout.NumCols - 4);
        int repeatedCols = ReadUInt16(page, layout.NumCols);
        return declaredCols == repeatedCols;
    }

    private static void AssertIndexPage(byte[] fileBytes, int pageNumber, int pageSize, DatabaseFormat format)
    {
        byte[] page = PageCopy(fileBytes, pageNumber, pageSize);
        var layout = IndexLeafPageBuilder.GetLayout(format);
        byte pageType = page[0];

        Assert.Equal(0x01, page[1]);
        int parentTdefPage = ReadInt32(page, 4);
        AssertPageType(fileBytes, pageSize, pageNumber, parentTdefPage, [0x02], "index parent TDEF");

        int freeSpace = ReadUInt16(page, 2);
        Assert.InRange(freeSpace, 0, pageSize - layout.FirstEntryOffset);
        int payloadEnd = pageSize - freeSpace;
        Assert.InRange(payloadEnd, layout.FirstEntryOffset, pageSize);

        (long prevPage, long nextPage, long tailPage) = IndexLeafIncremental.ReadSiblingPointers(layout, page);
        byte expectedSiblingType = pageType;
        AssertOptionalPageType(fileBytes, pageSize, pageNumber, prevPage, [expectedSiblingType], "previous index sibling");
        AssertOptionalPageType(fileBytes, pageSize, pageNumber, nextPage, [expectedSiblingType], "next index sibling");
        AssertOptionalPageType(fileBytes, pageSize, pageNumber, tailPage, [Constants.IndexLeafPage.PageTypeLeaf], "index tail leaf");

        List<int> entryStarts = AssertIndexEntryDirectory(page, pageNumber, pageSize, layout, pageType);
        if (pageType == Constants.IndexLeafPage.PageTypeLeaf)
        {
            List<IndexEntry> entries = IndexLeafIncremental.DecodeEntries(layout, page, pageSize);
            Assert.Equal(entryStarts.Count, entries.Count);
            foreach (IndexEntry entry in entries)
            {
                AssertIndexDataRowPointer(fileBytes, pageSize, format, pageNumber, entry.DataPage, entry.DataRow);
            }
        }
        else
        {
            List<DecodedIntermediateEntry> entries = IndexLeafIncremental.DecodeIntermediateEntries(layout, page, pageSize);
            Assert.Equal(entryStarts.Count, entries.Count);
            foreach (DecodedIntermediateEntry entry in entries)
            {
                AssertIndexDataRowPointer(fileBytes, pageSize, format, pageNumber, entry.Entry.DataPage, entry.Entry.DataRow);
                AssertPageType(
                    fileBytes,
                    pageSize,
                    pageNumber,
                    entry.ChildPage,
                    [Constants.IndexLeafPage.PageTypeIntermediate, Constants.IndexLeafPage.PageTypeLeaf],
                    "intermediate child page");
            }
        }
    }

    private static List<int> AssertIndexEntryDirectory(
        byte[] page,
        int pageNumber,
        int pageSize,
        IndexLeafPageBuilder.LeafPageLayout layout,
        byte pageType)
    {
        int freeSpace = ReadUInt16(page, 2);
        int payloadEnd = pageSize - freeSpace;
        int bitCount = (layout.FirstEntryOffset - layout.BitmaskOffset) * 8;

        if (payloadEnd == layout.FirstEntryOffset)
        {
            AssertIndexBitRangeClear(page, pageNumber, layout, 0, bitCount);
            return [];
        }

        int sentinelBitIndex = payloadEnd - layout.FirstEntryOffset;
        Assert.InRange(sentinelBitIndex, 1, bitCount - 1);
        Assert.False(IsIndexBitSet(page, layout, 0), Message(pageNumber, "Index bitmask must not mark the implicit first entry."));
        Assert.True(IsIndexBitSet(page, layout, sentinelBitIndex), Message(pageNumber, "Index bitmask is missing its one-past-end sentinel."));
        AssertIndexBitRangeClear(page, pageNumber, layout, sentinelBitIndex + 1, bitCount);

        var entryStarts = new List<int> { layout.FirstEntryOffset };
        for (int bitIndex = 1; bitIndex < sentinelBitIndex; bitIndex++)
        {
            if (IsIndexBitSet(page, layout, bitIndex))
            {
                entryStarts.Add(layout.FirstEntryOffset + bitIndex);
            }
        }

        int minimumEntryLength = pageType == Constants.IndexLeafPage.PageTypeLeaf ? 4 : 8;
        for (int entryIndex = 0; entryIndex < entryStarts.Count; entryIndex++)
        {
            int entryStart = entryStarts[entryIndex];
            int entryEnd = entryIndex + 1 < entryStarts.Count ? entryStarts[entryIndex + 1] : payloadEnd;
            Assert.True(
                entryEnd - entryStart >= minimumEntryLength,
                Message(pageNumber, $"Index entry {entryIndex} has length {entryEnd - entryStart}, below minimum {minimumEntryLength}."));
        }

        return entryStarts;
    }

    private static void AssertIndexBitRangeClear(
        byte[] page,
        int pageNumber,
        IndexLeafPageBuilder.LeafPageLayout layout,
        int startBitInclusive,
        int endBitExclusive)
    {
        for (int bitIndex = startBitInclusive; bitIndex < endBitExclusive; bitIndex++)
        {
            Assert.False(
                IsIndexBitSet(page, layout, bitIndex),
                Message(pageNumber, $"Index bitmask has an unexpected set bit at payload offset {bitIndex}."));
        }
    }

    private static bool IsIndexBitSet(byte[] page, IndexLeafPageBuilder.LeafPageLayout layout, int bitIndex)
    {
        int byteOffset = layout.BitmaskOffset + (bitIndex / 8);
        int bitInByte = bitIndex % 8;
        return (page[byteOffset] & (1 << bitInByte)) != 0;
    }

    private static void AssertIndexDataRowPointer(
        byte[] fileBytes,
        int pageSize,
        DatabaseFormat format,
        int indexPageNumber,
        long dataPageNumber,
        byte rowIndex)
    {
        AssertPageType(fileBytes, pageSize, indexPageNumber, dataPageNumber, [0x01], "index data-row page");

        var layout = DataPageLayout.For(format);
        ReadOnlySpan<byte> dataPage = PageSpan(fileBytes, checked((int)dataPageNumber), pageSize);
        int numRows = ReadUInt16(dataPage, layout.NumRows);
        Assert.True(
            rowIndex < numRows,
            Message(indexPageNumber, $"Index row pointer targets row {rowIndex} on data page {dataPageNumber}, but numRows={numRows}."));
    }

    private static void AssertOptionalPageType(
        byte[] fileBytes,
        int pageSize,
        int sourcePageNumber,
        long targetPageNumber,
        byte[] expectedTypes,
        string label)
    {
        if (targetPageNumber == 0)
        {
            return;
        }

        AssertPageType(fileBytes, pageSize, sourcePageNumber, targetPageNumber, expectedTypes, label);
    }

    private static void AssertPageType(
        byte[] fileBytes,
        int pageSize,
        int sourcePageNumber,
        long targetPageNumber,
        byte[] expectedTypes,
        string label)
    {
        int pageCount = fileBytes.Length / pageSize;
        Assert.True(
            targetPageNumber > 0 && targetPageNumber < pageCount,
            Message(sourcePageNumber, $"{label} pointer {targetPageNumber} is outside valid page range 1..{pageCount - 1}."));

        byte actualType = fileBytes[checked((int)targetPageNumber) * pageSize];
        foreach (byte expectedType in expectedTypes)
        {
            if (actualType == expectedType)
            {
                return;
            }
        }

        Assert.Fail(Message(sourcePageNumber, $"{label} pointer {targetPageNumber} has page type 0x{actualType:X2}."));
    }

    private static bool IsLvalPage(ReadOnlySpan<byte> page)
        => page.Length >= 8 && page[4] == (byte)'L' && page[5] == (byte)'V' && page[6] == (byte)'A' && page[7] == (byte)'L';

    private static bool IsAllZero(ReadOnlySpan<byte> page)
    {
        foreach (byte value in page)
        {
            if (value != 0)
            {
                return false;
            }
        }

        return true;
    }

    private static byte[] PageCopy(byte[] fileBytes, int pageNumber, int pageSize)
    {
        var page = new byte[pageSize];
        Buffer.BlockCopy(fileBytes, pageNumber * pageSize, page, 0, pageSize);
        return page;
    }

    private static ReadOnlySpan<byte> PageSpan(byte[] fileBytes, int pageNumber, int pageSize)
        => fileBytes.AsSpan(pageNumber * pageSize, pageSize);

    private static int PageSizeOf(DatabaseFormat format)
        => format == DatabaseFormat.Jet3Mdb ? Constants.PageSizes.Jet3 : Constants.PageSizes.Jet4;

    private static int ReadUInt16(ReadOnlySpan<byte> page, int offset)
        => BinaryPrimitives.ReadUInt16LittleEndian(page.Slice(offset, 2));

    private static int ReadInt32(ReadOnlySpan<byte> page, int offset)
        => BinaryPrimitives.ReadInt32LittleEndian(page.Slice(offset, 4));

    private static uint ReadUInt32(ReadOnlySpan<byte> page, int offset)
        => BinaryPrimitives.ReadUInt32LittleEndian(page.Slice(offset, 4));

    private static string Message(int pageNumber, string detail)
        => FormattableString.Invariant($"Page {pageNumber}: {detail}");

    private readonly record struct DataPageSummary(int ParentTdefPage, int LiveRowCount);

    private readonly record struct RowSlotInfo(int RowIndex, int RawOffset, int Start, int End)
    {
        public bool IsLive => (RawOffset & 0xC000) == 0;

        public int Size => End - Start + 1;
    }
}
