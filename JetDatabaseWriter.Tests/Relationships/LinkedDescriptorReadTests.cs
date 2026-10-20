namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Index and complex reads execute against the authorized linked source.</summary>
public sealed class LinkedDescriptorReadTests
{
    [Fact]
    public async Task AccessLink_IndexesAndComplexItems_UseApprovedSourceAndDisposeIt()
    {
        await using MemoryStream source = await CreateSourceAsync();
        await using MemoryStream host = await CreateHostAsync(text: false);
        var opened = new List<MemoryStream>();
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            LinkedSourceResolver = (_, _, _) =>
            {
                var stream = new MemoryStream(source.ToArray(), writable: false);
                opened.Add(stream);
                return new ValueTask<Stream>(stream);
            },
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);

        Assert.Equal("PK", Assert.Single(await reader.ListIndexesAsync("Alias", TestContext.Current.CancellationToken), index => index.Kind == IndexKind.PrimaryKey).Name);
        object[] row = Assert.Single(await reader.SeekRowsAsync("Alias", "PK", [7], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(7, row[0]);
        LinkedRow typed = Assert.Single(await reader.FromIndex<LinkedRow>("Alias", "PK").ToRowsAsync(TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(7, typed.Id);
        Assert.Equal(2, (await reader.GetComplexColumnsAsync("Alias", TestContext.Current.CancellationToken)).Count);
        AttachmentRecord attachment = Assert.Single(await reader.GetAttachmentsAsync("Alias", "Files", TestContext.Current.CancellationToken));
        Assert.Equal("test.txt", attachment.FileName);
        Assert.Equal("payload"u8.ToArray(), attachment.FileData);
        Assert.Equal("blue", Assert.Single(await reader.GetMultiValueItemsAsync("Alias", "Tags", TestContext.Current.CancellationToken)).Value);
        Assert.NotEmpty(opened);
        Assert.All(opened, stream => Assert.False(stream.CanRead));

        await using (IAsyncEnumerator<object[]> hits = reader.SeekRowsAsync("Alias", "PK", [7], TestContext.Current.CancellationToken).GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            Assert.True(await hits.MoveNextAsync());
            Assert.True(opened[^1].CanRead);
        }

        Assert.All(opened, stream => Assert.False(stream.CanRead));
    }

    [Fact]
    public async Task AccessLink_MetadataRequiresApprovedSource()
    {
        await using MemoryStream host = await CreateHostAsync(text: false);
        await using AccessReader reader = await AccessReader.OpenAsync(host, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await reader.ListIndexesAsync("Alias", TestContext.Current.CancellationToken));
        await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await reader.GetComplexColumnsAsync("Alias", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task TextLink_IndexAndComplexMetadata_IsEmptyWithoutOpeningSource()
    {
        await using MemoryStream host = await CreateHostAsync(text: true);
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            LinkedSourceResolver = (_, _, _) => throw new InvalidOperationException("Metadata must not open a text source."),
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Empty(await reader.ListIndexesAsync("Alias", TestContext.Current.CancellationToken));
        Assert.Empty(await reader.GetComplexColumnsAsync("Alias", TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("indexes")]
    [InlineData("complex")]
    [InlineData("seek")]
    [InlineData("attachments")]
    [InlineData("values")]
    public async Task AccessLink_DescriptorReads_BoundTransitiveSources(string operation)
    {
        await using var recurring = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(recurring, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateLinkedTableAsync("Data", Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "nested.accdb"), "Data", TestContext.Current.CancellationToken);
        }

        await using MemoryStream host = await CreateHostAsync(text: false);
        var opened = new List<MemoryStream>();
        var options = new AccessReaderOptions
        {
            UseLockFile = false,
            LinkedSourceMaxDepth = 2,
            LinkedSourceResolver = (_, _, _) =>
            {
                var stream = new MemoryStream(recurring.ToArray(), writable: false);
                opened.Add(stream);
                return new ValueTask<Stream>(stream);
            },
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            Task read = operation switch
            {
                "indexes" => reader.ListIndexesAsync("Alias", TestContext.Current.CancellationToken).AsTask(),
                "complex" => reader.GetComplexColumnsAsync("Alias", TestContext.Current.CancellationToken).AsTask(),
                "seek" => reader.SeekRowsAsync("Alias", "PK", [7], TestContext.Current.CancellationToken).ToListAsync(TestContext.Current.CancellationToken).AsTask(),
                "attachments" => reader.GetAttachmentsAsync("Alias", "Files", TestContext.Current.CancellationToken).AsTask(),
                "values" => reader.GetMultiValueItemsAsync("Alias", "Tags", TestContext.Current.CancellationToken).AsTask(),
                _ => throw new ArgumentException("Unknown operation.", nameof(operation)),
            };
            await read;
        });
        Assert.Equal(2, opened.Count);
        Assert.All(opened, stream => Assert.False(stream.CanRead));
    }

    private static async ValueTask<MemoryStream> CreateSourceAsync()
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int)), new("Files", typeof(byte[])) { IsAttachment = true }, new("Tags", typeof(object)) { IsMultiValue = true, MultiValueElementType = typeof(string) }], [new("PK", "Id") { IsPrimaryKey = true }], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Data", [7, DBNull.Value, DBNull.Value], TestContext.Current.CancellationToken);
            await writer.AddAttachmentAsync("Data", "Files", new Dictionary<string, object?> { ["Id"] = 7 }, new AttachmentInput("test.txt", "payload"u8.ToArray()), TestContext.Current.CancellationToken);
            await writer.AddMultiValueItemAsync("Data", "Tags", new Dictionary<string, object?> { ["Id"] = 7 }, "blue", TestContext.Current.CancellationToken);
        }

        return stream;
    }

    private static async ValueTask<MemoryStream> CreateHostAsync(bool text)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            if (text)
            {
                await writer.CreateLinkedTextTableAsync("Alias", LinkedTestPaths.GetCanonicalTempPath(), "unopened.csv", "Text;HDR=YES;FMT=Delimited", TestContext.Current.CancellationToken);
            }
            else
            {
                await writer.CreateLinkedTableAsync("Alias", Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "unopened.accdb"), "Data", TestContext.Current.CancellationToken);
            }
        }

        return stream;
    }

    private sealed class LinkedRow
    {
        public int Id { get; set; }
    }
}
