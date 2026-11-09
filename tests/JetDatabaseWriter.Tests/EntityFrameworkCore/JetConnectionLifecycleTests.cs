namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Xunit;

public sealed class JetConnectionLifecycleTests
{
    [Fact]
    public async Task CloseReader_Waits_For_An_Outstanding_Writer_Rental()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: ct))
        {
        }

        DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using JetClientConnection connection = new(options);
        (AccessWriter writer, bool owned) = await connection.RentWriterAsync(ct);
        Task close = connection.CloseReaderAsync().AsTask();
        try
        {
            Assert.False(close.IsCompleted);
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], ct);
        }
        finally
        {
            await connection.ReleaseWriterAsync(writer, owned);
        }

        await close;
    }

    [Fact]
    public async Task File_Writer_Is_Reused_Until_Another_Connection_Needs_The_File()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"jdw-ef-lease-{Guid.NewGuid():N}.accdb");
        try
        {
            await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: ct))
            {
            }

            DbContextOptions options = new DbContextOptionsBuilder().UseJetDatabaseWriter(path).Options;
            await using JetClientConnection first = new(options);
            await using JetClientConnection second = new(options);
            (AccessWriter original, bool owned) = await first.RentWriterAsync(ct);
            await first.ReleaseWriterAsync(original, owned);
            (AccessWriter reused, bool reusedOwned) = await first.RentWriterAsync(ct);
            Assert.Same(original, reused);

            Task<(AccessWriter Writer, bool OwnedByCaller)> competingRental = second.RentWriterAsync(ct).AsTask();
            try
            {
                Assert.False(competingRental.IsCompleted);
                await reused.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], ct);
            }
            finally
            {
                await first.ReleaseWriterAsync(reused, reusedOwned);
            }

            (AccessWriter next, bool nextOwned) = await competingRental;
            try
            {
                await next.InsertRowAsync("Items", [7], ct);
            }
            finally
            {
                await second.ReleaseWriterAsync(next, nextOwned);
            }

            AccessReader reader = await first.GetReaderAsync(ct);
            int count = 0;
            await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
            {
                Assert.Equal(7, row[0]);
                count++;
            }

            Assert.Equal(1, count);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
