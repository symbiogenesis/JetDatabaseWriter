namespace JetDatabaseWriter.Tests.Pages;

using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>Repeated bitmap edits retain physical validation reads without writing identical pages.</summary>
public sealed class UsageMapEditorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets each format, bitmap representation and desired state.</summary>
    public static TheoryData<DatabaseFormat, bool, bool> States => new()
    {
        { DatabaseFormat.Jet3Mdb, false, false },
        { DatabaseFormat.Jet3Mdb, false, true },
        { DatabaseFormat.Jet3Mdb, true, false },
        { DatabaseFormat.Jet3Mdb, true, true },
        { DatabaseFormat.Jet4Mdb, false, false },
        { DatabaseFormat.Jet4Mdb, false, true },
        { DatabaseFormat.Jet4Mdb, true, false },
        { DatabaseFormat.Jet4Mdb, true, true },
        { DatabaseFormat.AceAccdb, false, false },
        { DatabaseFormat.AceAccdb, false, true },
        { DatabaseFormat.AceAccdb, true, false },
        { DatabaseFormat.AceAccdb, true, true },
    };

    [Theory]
    [MemberData(nameof(States))]
    public async Task AlreadyDesiredBit_ReadsStorageWithoutWritingAndStillAllowsChanges(DatabaseFormat kind, bool reference, bool marked)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] original = CreateImage(format, reference, marked);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, Ct);
        await using var counting = new CountingStream(stream);
#pragma warning disable CA2000 // The awaited pager owns and disposes its codec.
        await using var pager = new Pager(counting, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), cacheSize: 0);
#pragma warning restore CA2000
        var editor = new UsageMapEditor(format, pager, new PageAllocator(format, pager, new AccessWriterOptions()));
        await SetStateAsync(editor, marked);
        Assert.Equal((reference ? 2L : 1L) * format.PageSize, counting.BytesRead);
        Assert.Equal(0, counting.BytesWritten);
        Assert.Equal(original, stream.ToArray());

        counting.Reset();
        await SetStateAsync(editor, !marked);
        Assert.Equal(format.PageSize, counting.BytesWritten);
        byte[] changed = stream.ToArray();
        Assert.NotEqual(original, changed);

        counting.Reset();
        await SetStateAsync(editor, !marked);
        Assert.Equal((reference ? 2L : 1L) * format.PageSize, counting.BytesRead);
        Assert.Equal(0, counting.BytesWritten);
        Assert.Equal(changed, stream.ToArray());
    }

    [Theory]
    [MemberData(nameof(States))]
    public async Task AlreadyDesiredBit_StillPropagatesMapOrBitmapReadFailure(DatabaseFormat kind, bool reference, bool marked)
    {
        var format = JetFormat.ForNewDatabase(kind);
        byte[] original = CreateImage(format, reference, marked);
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(original, Ct);
        await using var counting = new CountingStream(stream);
#pragma warning disable CA2000 // The awaited pager owns and disposes its codec.
        await using var pager = new Pager(counting, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), cacheSize: 0);
#pragma warning restore CA2000
        var editor = new UsageMapEditor(format, pager, new PageAllocator(format, pager, new AccessWriterOptions()));
        int writes = stream.WriteCount;
        stream.FailOnRead(reference ? 2 : 1);
        await Assert.ThrowsAsync<IOException>(() => SetStateAsync(editor, marked).AsTask());
        Assert.True(stream.Faulted);
        Assert.Equal(writes, stream.WriteCount);
        Assert.Equal(original, stream.ToArray());
    }

    [Theory]
    [InlineData("row", false)]
    [InlineData("row", true)]
    [InlineData("type", false)]
    [InlineData("type", true)]
    [InlineData("pointer", false)]
    [InlineData("pointer", true)]
    [InlineData("header", false)]
    [InlineData("header", true)]
    public async Task InvalidStorage_IsReadAndLeftUnchanged(string corruption, bool marked)
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.Jet3Mdb);
        byte[] original = CreateImage(format, reference: true, marked);
        int mapOffset = format.PageSize * 2;
        int rowStart = format.PageSize - format.UsageMapRowSize;
        switch (corruption)
        {
            case "row":
                Wu16(original, mapOffset + format.DataPage.RowsStart, format.PageSize);
                break;
            case "type":
                original[mapOffset + rowStart] = 0x77;
                break;
            case "pointer":
                Wi32(original, mapOffset + rowStart + 1, 5);
                break;
            default:
                original[format.PageSize * 3] = Constants.PageTypes.Data;
                break;
        }

        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, Ct);
        await using var counting = new CountingStream(stream);
#pragma warning disable CA2000 // The awaited pager owns and disposes its codec.
        await using var pager = new Pager(counting, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter), cacheSize: 0);
#pragma warning restore CA2000
        var editor = new UsageMapEditor(format, pager, new PageAllocator(format, pager, new AccessWriterOptions()));
        await SetStateAsync(editor, marked);
        Assert.Equal((corruption == "header" ? 2L : 1L) * format.PageSize, counting.BytesRead);
        Assert.Equal(0, counting.BytesWritten);
        Assert.Equal(original, stream.ToArray());
    }

    private static ValueTask SetStateAsync(UsageMapEditor editor, bool marked)
        => marked ? editor.MarkPageAsync(2, [0], 4, Ct) : editor.ClearPageAsync(2, 0, 4, Ct);

    private static byte[] CreateImage(JetFormat format, bool reference, bool marked)
    {
        byte[] image = new byte[format.PageSize * 5];
        byte[] map = new byte[format.PageSize];
        int rowStart = format.PageSize - format.UsageMapRowSize;
        map[0] = Constants.PageTypes.Data;
        map[1] = 1;
        Wu16(map, format.DataPage.NumRows, 1);
        Wu16(map, format.DataPage.RowsStart, rowStart);
        Wu16(map, 2, rowStart - format.DataPage.RowsStart - 2);
        if (reference)
        {
            map[rowStart] = Constants.UsageMap.ReferenceMapType;
            Wi32(map, rowStart + 1, 3);
            byte[] bitmap = new byte[format.PageSize];
            bitmap[0] = Constants.PageTypes.UsageMap;
            bitmap[1] = 1;
            Assert.True(UsageMap.TrySetReferencePageState(bitmap, format.PageSize, 4, marked));
            bitmap.CopyTo(image, format.PageSize * 3);
        }
        else
        {
            Assert.True(UsageMap.TrySetInlinePageState(map, rowStart, format.UsageMapRowSize, 4, marked));
        }

        map.CopyTo(image, format.PageSize * 2);
        return image;
    }
}
