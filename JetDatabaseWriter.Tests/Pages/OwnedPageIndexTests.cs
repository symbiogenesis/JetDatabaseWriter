namespace JetDatabaseWriter.Tests.Pages;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Checks owner-index mutation and invalidation.</summary>
public sealed class OwnedPageIndexTests
{
    /// <summary>Writes transfer ownership; rollback invalidation restores the physical index.</summary>
    [Fact]
    public async Task PendingWrites_AndRollback_KeepIndexCurrent()
    {
        var format = JetFormat.ForNewDatabase(DatabaseFormat.AceAccdb);
        await using var stream = new MemoryStream();
        stream.SetLength(format.PageSize * 4);
#pragma warning disable CA2000 // The awaited pager owns its codec.
        await using var pager = new Pager(stream, format.PageSize, new NoPageCodec(), true, typeof(AccessWriter));
#pragma warning restore CA2000
        using var index = new OwnedPageIndex(pager, format);
        pager.AddWriteObserver(index);
        byte[] page = new byte[format.PageSize];
        page[0] = 1;
        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4), 2);
        await pager.WritePageAsync(3, page, TestContext.Current.CancellationToken);
        Assert.Equal(3L, Assert.Single(await index.GetAsync(2, TestContext.Current.CancellationToken)));
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Attach(new PagerTransaction(stream.Length, format.PageSize, 4));
        }

        BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(format.DataPage.TDefOff, 4), 1);
        await pager.WritePageAsync(3, page, TestContext.Current.CancellationToken);
        Assert.Empty(await index.GetAsync(2, TestContext.Current.CancellationToken));
        Assert.Equal(3L, Assert.Single(await index.GetAsync(1, TestContext.Current.CancellationToken)));
        await pager.AppendPageAsync(page, TestContext.Current.CancellationToken);
        long[] appended = await index.GetAsync(1, TestContext.Current.CancellationToken);
        Assert.Equal([3L, 4L], appended);
        using (Pager.JournalGate gate = await pager.EnterJournalGateAsync(TestContext.Current.CancellationToken))
        {
            gate.Detach();
        }

        Assert.Equal(3L, Assert.Single(await index.GetAsync(2, TestContext.Current.CancellationToken)));
        Assert.Empty(await index.GetAsync(1, TestContext.Current.CancellationToken));
    }

    /// <summary>A session's second insert avoids repeating Northwind's physical owner scan.</summary>
    [Fact]
    public async Task Northwind_SecondInsert_ReadsFewerThan300Pages()
    {
        byte[] image = await File.ReadAllBytesAsync(TestDatabases.MdbtoolsNwind, TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(image, TestContext.Current.CancellationToken);
        await using var trace = new PageTraceStream(stream, 2048);
        await using AccessWriter writer = await AccessWriter.OpenAsync(
            trace,
            new AccessWriterOptions
            {
                UseLockFile = false,
                UseByteRangeLocks = false,
            },
            true,
            TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Shippers", new RowValues { ["CompanyName"] = "Pager first" }, TestContext.Current.CancellationToken);
        trace.Reset();
        await writer.InsertRowAsync("Shippers", new RowValues { ["CompanyName"] = "Pager second" }, TestContext.Current.CancellationToken);
        Assert.True(trace.ReadCounts(2048).Values.Sum() < 300);
    }
}
