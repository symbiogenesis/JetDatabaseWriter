namespace JetDatabaseWriter.Maintenance;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;

/// <content>Independently reads strict native table-owned map rows and every mapped page header.</content>
internal sealed partial class CompactActiveTableRows
{
    private async ValueTask ReadOwnedMapAsync(CancellationToken token)
    {
        uint packed = this.definition.Header.OwnedPagesReference;
        var pointer = new UsageMapPointer((int)(packed & 0xFF), checked((int)(packed >> 8)));
        if (pointer.PageNumber <= 2 || pointer.PageNumber >= this.sourcePageCount)
        {
            throw Corrupt(pointer.PageNumber, "The native table-owned map has a reserved or out-of-source row pointer.");
        }

        var bitmaps = new HashSet<long>();
        byte[] page = await this.pages.ReadPageAsync(pointer.PageNumber, token).ConfigureAwait(false);
        try
        {
            RowBound row = this.MapRow(page, pointer, token);
            if (page[row.RowStart] == Constants.UsageMap.InlineMapType)
            {
                long first = BinaryPrimitives.ReadInt32LittleEndian(page.AsSpan(row.RowStart + 1));
                if (first < 0 || row.RowSize <= Constants.UsageMap.InlineMapHeaderSize)
                {
                    throw Corrupt(pointer.PageNumber, "The native table-owned inline map has an invalid base or empty bitmap.");
                }

                this.ReadBits(page, row.RowStart + Constants.UsageMap.InlineBitmapOffset, row.RowStart + row.RowSize, first, pointer.PageNumber, token);
            }
            else if (page[row.RowStart] == Constants.UsageMap.ReferenceMapType)
            {
                int count = UsageMap.ReferencePointerCount(row.RowSize);
                for (int index = 0; index < count; index++)
                {
                    this.MapStep(pointer.PageNumber, token);
                    long bitmap = BinaryPrimitives.ReadUInt32LittleEndian(page.AsSpan(row.RowStart + 1 + (index * 4)));
                    if (bitmap == 0)
                    {
                        continue;
                    }

                    this.accounting.Charge(CompactDiscoveryCharge.References, 1, bitmap);
                    if (bitmap <= 2 || bitmap >= this.sourcePageCount || bitmap == pointer.PageNumber || bitmaps.Contains(bitmap))
                    {
                        throw Corrupt(bitmap, "The native table-owned map has a reserved, repeated, aliased or out-of-source bitmap pointer.");
                    }

                    this.Retain(128, bitmap);
                    bitmaps.Add(bitmap);
                    byte[] bits = await this.pages.ReadPageAsync(bitmap, token).ConfigureAwait(false);
                    try
                    {
                        if (bits[0] != Constants.PageTypes.UsageMap || bits[1] != 1)
                        {
                            throw Corrupt(bitmap, "The native table-owned bitmap has an invalid page type.");
                        }

                        this.ReadBits(bits, Constants.UsageMap.ReferenceMapBitmapOffset, this.format.PageSize, (long)index * UsageMap.PagesPerReferenceMapPage(this.format.PageSize), bitmap, token);
                    }
                    finally
                    {
                        PageBuffers.Return(bits);
                    }
                }
            }
            else
            {
                throw Corrupt(pointer.PageNumber, "The native table-owned row has an unknown map type.");
            }
        }
        finally
        {
            PageBuffers.Return(page);
        }

        foreach (long member in this.mappedPages)
        {
            this.accounting.Charge(CompactDiscoveryCharge.GraphSteps, 1, member);
            if (member == pointer.PageNumber || bitmaps.Contains(member))
            {
                throw Corrupt(member, "A native table-owned map member aliases map storage.");
            }

            byte[] data = await this.pages.ReadPageAsync(member, token).ConfigureAwait(false);
            try
            {
                if (data[0] != Constants.PageTypes.Data || BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(this.format.DataPage.TDefOff)) != this.OwnerTDefPage)
                {
                    throw Corrupt(member, "A native table-owned map member is not an ordinary DATA page with the exact expected owner.");
                }
            }
            finally
            {
                PageBuffers.Return(data);
            }
        }
    }

    private RowBound MapRow(byte[] page, UsageMapPointer pointer, CancellationToken token)
    {
        if (page[0] != Constants.PageTypes.Data)
        {
            throw Corrupt(pointer.PageNumber, "The table-owned native map row is not on a DATA page.");
        }

        int count = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(this.format.DataPage.NumRows));
        if (count > (this.format.PageSize - this.format.DataPage.RowsStart) / 2 || pointer.RowIndex >= count)
        {
            throw Corrupt(pointer.PageNumber, "The table-owned map directory is oversized or omits its declared row.");
        }

        this.accounting.Charge(CompactDiscoveryCharge.DirectorySlots, count, pointer.PageNumber);
        int directoryEnd = this.format.DataPage.RowsStart + (count * 2);
        int target = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(this.format.DataPage.RowsStart + (pointer.RowIndex * 2)));
        int start = target & Constants.DataPage.RowOffsetMask;
        if ((target & ~Constants.DataPage.RowOffsetMask) != 0 || start < directoryEnd || start >= this.format.PageSize)
        {
            throw Corrupt(pointer.PageNumber, "The native table-owned map row is absent, deleted, moved or outside its directory.");
        }

        int end = this.format.PageSize;
        for (int index = 0; index < count; index++)
        {
            this.MapStep(pointer.PageNumber, token);
            int raw = BinaryPrimitives.ReadUInt16LittleEndian(page.AsSpan(this.format.DataPage.RowsStart + (index * 2)));
            int other = raw & Constants.DataPage.RowOffsetMask;
            bool deleted = (raw & Constants.DataPage.DeletedRowFlag) != 0;
            if ((raw & ~(Constants.DataPage.RowOffsetMask | Constants.DataPage.NonLiveRowFlags)) != 0
                || (!deleted && (other < directoryEnd || other >= this.format.PageSize))
                || (deleted && other != 0 && (other < directoryEnd || other > this.format.PageSize))
                || (index != pointer.RowIndex && other == start))
            {
                throw Corrupt(pointer.PageNumber, "The native table-owned map directory has malformed flags, bounds or an aliased row.");
            }

            if (other > start && other < end)
            {
                end = other;
            }
        }

        if (end - start < Constants.UsageMap.InlineMapHeaderSize)
        {
            throw Corrupt(pointer.PageNumber, "The native table-owned map row is truncated.");
        }

        return new(pointer.RowIndex, start, end - start);
    }

    private void ReadBits(byte[] page, int start, int end, long first, long storagePage, CancellationToken token)
    {
        for (int offset = start; offset < end; offset++)
        {
            this.MapStep(storagePage, token);
            byte bits = page[offset];
            for (int bit = 0; bit < 8; bit++)
            {
                if ((bits & (1 << bit)) == 0)
                {
                    continue;
                }

                long member = first + ((offset - start) * 8L) + bit;
                this.accounting.Charge(CompactDiscoveryCharge.References, 1, storagePage);
                if (member <= 2 || member >= this.sourcePageCount || this.mappedPages.Contains(member))
                {
                    throw Corrupt(member, "The native table-owned map contains a reserved, repeated or out-of-source member.");
                }

                this.Retain(128, member);
                this.mappedPages.Add(member);
            }
        }
    }

    private void MapStep(long page, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        this.accounting.Charge(CompactDiscoveryCharge.MapSteps, 1, page);
    }
}
