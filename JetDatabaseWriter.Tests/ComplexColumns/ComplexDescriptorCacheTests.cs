namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.ComplexColumns;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.ValueDecoding;
using Xunit;

/// <summary>Verified complex metadata is reusable independently of cached data pages.</summary>
public sealed class ComplexDescriptorCacheTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WarmDescriptors_WithPageCacheDisabled_ReadNoPagesAndHonorCancellation(bool strict)
    {
        await using MemoryStream database = await CreateDatabaseAsync();
        await using var stream = new CountingStream(database);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0, StrictParsing = strict }, leaveOpen: true, cancellationToken: Ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        IReadOnlyList<ComplexColumnInfo> expected = await reader.GetComplexColumnsAsync("Docs", Ct);
        Assert.Equal(2, expected.Count);
        stream.Reset();

        Assert.Equal(expected, await reader.GetComplexColumnsAsync("docs", Ct));
        Assert.Equal(expected, await reader.GetComplexColumnsAsync("DOCS", Ct));
        Assert.Equal(0, stream.BytesRead);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.GetComplexColumnsAsync("Docs", canceled.Token));
        Assert.Equal(0, stream.BytesRead);
    }

    [Fact]
    public async Task CatalogInvalidation_DiscardsVerifiedDescriptors()
    {
        await using MemoryStream database = await CreateDatabaseAsync();
        await using var stream = new CountingStream(database);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        IReadOnlyList<ComplexColumnInfo> expected = await reader.GetComplexColumnsAsync("Docs", Ct);
        reader.Services.TableCatalog.Invalidate();
        stream.Reset();

        Assert.Equal(expected, await reader.GetComplexColumnsAsync("Docs", Ct));
        Assert.True(stream.BytesRead > 0);
    }

    [Fact]
    public async Task WriterSchemaChangesAndRollback_ResolveCurrentComplexDescriptors()
    {
        await using MemoryStream database = await CreateDatabaseAsync();
        await using AccessWriter writer = await AccessWriter.OpenAsync(database, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        using var pages = new ReaderPageCache(writer.Database.Format, writer.Database.Pages, capacity: 0);
        var rows = new RowDecoder(writer.Database.Format, writer.Database.OwnedPages, pages, new LongValueDecoder(writer.Database.Format, pages), strictParsing: true);
        var catalog = new CatalogReader(writer.Database.Format, writer.Database.TableDefs, writer.Services.Catalog, writer.Services.CatalogRows, rows);
        var complex = new ComplexColumnReader(writer.Database.Format, writer.Database.TableDefs, catalog, rows, diagnosticsEnabled: false);
        Assert.Contains(await complex.GetComplexColumnsAsync("Docs", Ct), column => column.ColumnName == "Files");

        await using JetTransaction transaction = await writer.BeginTransactionAsync(Ct);
        await writer.RenameColumnAsync("Docs", "Files", "Attachments", Ct);
        IReadOnlyList<ComplexColumnInfo> renamed = await complex.GetComplexColumnsAsync("Docs", Ct);
        Assert.Contains(renamed, column => column.ColumnName == "Attachments");
        Assert.DoesNotContain(renamed, column => column.ColumnName == "Files");

        await transaction.RollbackAsync(Ct);
        IReadOnlyList<ComplexColumnInfo> restored = await complex.GetComplexColumnsAsync("Docs", Ct);
        Assert.Contains(restored, column => column.ColumnName == "Files");
        Assert.DoesNotContain(restored, column => column.ColumnName == "Attachments");
    }

    [Fact]
    public async Task WarmDescriptors_ConcurrentReadsCannotMutateTheSharedResult()
    {
        await using MemoryStream database = await CreateDatabaseAsync();
        await using AccessReader reader = await AccessReader.OpenAsync(database, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        IReadOnlyList<ComplexColumnInfo> expected = await reader.GetComplexColumnsAsync("Docs", Ct);
        IList<ComplexColumnInfo> mutableView = Assert.IsType<IList<ComplexColumnInfo>>(expected, exactMatch: false);
        Assert.Throws<NotSupportedException>(() => mutableView[0] = new ComplexColumnInfo { ColumnName = "Injected" });

        var reads = new Task<IReadOnlyList<ComplexColumnInfo>>[16];
        for (int index = 0; index < reads.Length; index++)
        {
            reads[index] = reader.GetComplexColumnsAsync("Docs", Ct).AsTask();
        }

        foreach (IReadOnlyList<ComplexColumnInfo> actual in await Task.WhenAll(reads))
        {
            Assert.Equal(expected, actual);
        }
    }

    [Fact]
    public async Task WarmDescriptors_DoNotCacheAttachmentRowsOrSuppressIoFailures()
    {
        await using MemoryStream database = await CreateDatabaseAsync();
        await using var stream = new WriteFaultStream();
        await database.CopyToAsync(stream, Ct);
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, PageCacheSize = 0 }, leaveOpen: true, cancellationToken: Ct);
        reader.Database.TableDefs.VerifyOnHit = false;
        Assert.Equal("first.txt", Assert.Single(await reader.GetAttachmentsAsync("Docs", "Files", Ct)).FileName);

        stream.FailOnRead(1);
        await Assert.ThrowsAsync<IOException>(async () => await reader.GetAttachmentsAsync("Docs", "Files", Ct));
        Assert.True(stream.Faulted);
        Assert.Equal("first.txt", Assert.Single(await reader.GetAttachmentsAsync("Docs", "Files", Ct)).FileName);
    }

    private static async Task<MemoryStream> CreateDatabaseAsync()
    {
        var database = new MemoryStream();
        try
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(database, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
            {
                await writer.CreateTableAsync("Docs", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }, new ColumnDefinition("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], Ct);
                await writer.InsertRowAsync("Docs", new RowValues { ["Id"] = 1 }, Ct);
                await writer.AddAttachmentAsync("Docs", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("first.txt", [1, 2, 3]), Ct);
            }

            database.Position = 0;
            return database;
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }
    }
}
