namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using Xunit;
using static JetDatabaseWriter.Tests.ComplexColumns.ComplexColumnTestSupport;

public sealed class AttachmentResourceLimitTests
{
    private const int PayloadLength = 8192;
    private const int ContentLength = PayloadLength + 20;

    [Theory]
    [InlineData("txt")]
    [InlineData("jpg")]
    public void AttachmentDecode_ObservesCancellation(string extension)
    {
        byte[] wrapped = AttachmentWrapper.Encode(extension, new byte[PayloadLength]);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        OperationCanceledException failure = Assert.Throws<OperationCanceledException>(() =>
            AttachmentWrapper.TryDecode(wrapped, out _, out _, cancellationToken: cancellation.Token));

        Assert.Equal(cancellation.Token, failure.CancellationToken);
    }

    [Fact]
    public void AttachmentInflation_ObservesCancellationAfterReadingFirstChunk()
    {
        byte[] payload = new byte[4 * PayloadLength];
#pragma warning disable CA5394 // A fixed seed keeps the incompressible regression payload reproducible; no security use.
        new Random(42).NextBytes(payload);
#pragma warning restore CA5394
        byte[] wrapped = AttachmentWrapper.Encode("txt", payload);
        using var cancellation = new CancellationTokenSource();
        using var input = new CancelDuringReadStream(wrapped.AsSpan(10, wrapped.Length - 14).ToArray(), cancellation);

        OperationCanceledException failure = Assert.Throws<OperationCanceledException>(() =>
            AttachmentWrapper.Inflate(input, payload.Length + 20, cancellation.Token));

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(2, input.ReadCalls);
    }

    [Fact]
    public void AttachmentInflation_ObservesCancellationInsideSingleReadWithEmptyBlocks()
    {
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        byte[] compressed = PrependEmptyBlocks(wrapped.AsSpan(10, wrapped.Length - 14), 65536);
        using var cancellation = new CancellationTokenSource();
        using var input = new CancelDuringReadStream(compressed, cancellation);

        OperationCanceledException failure = Assert.Throws<OperationCanceledException>(() =>
            AttachmentWrapper.Inflate(input, 23, cancellation.Token));

        Assert.Equal(cancellation.Token, failure.CancellationToken);
        Assert.Equal(2, input.ReadCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReadAllTables_SharesBudgetAcrossTables(bool strict)
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false, twoTables: true);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength, strict);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.ReadAllTablesAsync(cancellationToken: Ct));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Fact]
    public void AttachmentDecode_BoundsValidCostlyCompressedRepresentation()
    {
        byte[] wrapped = BuildCostlyWrapper();
        var budget = new AttachmentWrapper.ContentBudget(23, 4 * 23);

        JetLimitationException failure = Assert.Throws<JetLimitationException>(() =>
            AttachmentWrapper.TryDecode(wrapped, out _, out _, contentBudget: budget, cancellationToken: Ct));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string extension, out byte[] payload, contentBudget: new(23, wrapped.Length + (4 * 23)), cancellationToken: Ct));
        Assert.Equal("txt", extension);
        Assert.Equal([1, 2, 3], payload);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AttachmentRead_WorkRefusalPropagatesInBothParsingModes(bool strict)
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength, strict, 4 * ContentLength);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.GetAttachmentsAsync("Docs", "Files", Ct));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            using DataTable table = await reader.ReadTableAsync("Docs", cancellationToken: Ct);
        });
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            using DataTable table = await reader.ReadTableAsStringsAsync("Docs", cancellationToken: Ct);
        });
    }

    [Fact]
    public async Task AttachmentRead_SharesWorkBudgetAcrossIndividuallyPermittedValues()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        long valueWork = AttachmentWrapper.Encode("txt", new byte[PayloadLength]).Length + (4L * ContentLength);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength, workLimit: 2 * valueWork);

        await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.GetAttachmentsAsync("Docs", "Files", Ct));

        await using AccessReader larger = await OpenReaderAsync(database, 3 * ContentLength, workLimit: 3 * valueWork);
        Assert.Equal(3, (await larger.GetAttachmentsAsync("Docs", "Files", Ct)).Count);
    }

    [Fact]
    public void AttachmentDecode_BoundedExtensionChunksPreserveSurrogatePairs()
    {
        string extension = new string('a', 4095) + "\U0001F600" + new string('b', 4096);
        byte[] wrapped = AttachmentWrapper.Encode(extension, [1, 2, 3]);

        Assert.True(AttachmentWrapper.TryDecode(wrapped, out string decodedExtension, out byte[] payload, cancellationToken: Ct));

        Assert.Equal(extension, decodedExtension);
        Assert.Equal([1, 2, 3], payload);
    }

    [Fact]
    public async Task ReadAllTables_ExplicitLargerBudgetPermitsValuesAndResetsAfterRefusal()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false, twoTables: true);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        await Assert.ThrowsAsync<JetLimitationException>(async () => await reader.ReadAllTablesAsync(cancellationToken: Ct));
        Assert.Equal(3, (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Count);

        await using AccessReader larger = await OpenReaderAsync(database, 4 * ContentLength);
        IReadOnlyDictionary<string, DataTable> tables = await larger.ReadAllTablesAsync(cancellationToken: Ct);
        try
        {
            Assert.Equal(2, tables.Count);
            Assert.All(tables.Values, table => Assert.Single(table.Rows));
        }
        finally
        {
            foreach (DataTable table in tables.Values)
            {
                table.Dispose();
            }
        }
    }

    [Fact]
    public async Task AttachmentReads_ConcurrentOperationsHaveIndependentBudgets()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        Task<IReadOnlyList<AttachmentRecord>> first = reader.GetAttachmentsAsync("Docs", "Files", Ct).AsTask();
        Task<IReadOnlyList<AttachmentRecord>> second = reader.GetAttachmentsAsync("Docs", "Files", Ct).AsTask();

        IReadOnlyList<AttachmentRecord>[] results = await Task.WhenAll(first, second);

        Assert.All(results, attachments => Assert.Equal(3, attachments.Count));
    }

    [Fact]
    public async Task AttachmentQuery_EarlyDisposalResetsBudgetForNextEnumeration()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        IQueryable<Document> query = reader.Query<Document>("Docs");
        for (int attempt = 0; attempt < 2; attempt++)
        {
            await using IAsyncEnumerator<Document> rows = query.AsAsyncEnumerable().GetAsyncEnumerator(Ct);
            Assert.True(await rows.MoveNextAsync());
            Assert.NotNull(rows.Current.Files);
        }

        Assert.Equal(3, (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Count);
    }

    [Fact]
    public async Task AttachmentQueries_ConcurrentActiveEnumeratorsHaveIndependentBudgets()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        IQueryable<Document> query = reader.Query<Document>("Docs");
        await using IAsyncEnumerator<Document> first = query.AsAsyncEnumerable().GetAsyncEnumerator(Ct);
        await using IAsyncEnumerator<Document> second = query.AsAsyncEnumerable().GetAsyncEnumerator(Ct);

        Assert.True(await first.MoveNextAsync());
        Assert.True(await second.MoveNextAsync());
        Assert.Equal(3, (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Count);
        Assert.False(await first.MoveNextAsync());
        Assert.False(await second.MoveNextAsync());
    }

    [Fact]
    public async Task AttachmentQuery_CancellationRestoresIndependentReadBudget()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        IQueryable<Document> query = reader.Query<Document>("Docs").Concat(reader.Query<Document>("Docs"));
        await using IAsyncEnumerator<Document> rows = query.AsAsyncEnumerable().GetAsyncEnumerator(cancellation.Token);
        Assert.True(await rows.MoveNextAsync());
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await rows.MoveNextAsync());

        Assert.Equal(3, (await reader.GetAttachmentsAsync("Docs", "Files", Ct)).Count);
    }

    [Fact]
    public async Task AttachmentQuery_DisposalWaitsForActiveEnumeration()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);
        await using IAsyncEnumerator<Document> rows = reader.Query<Document>("Docs").AsAsyncEnumerable().GetAsyncEnumerator(Ct);
        Assert.True(await rows.MoveNextAsync());

        Task disposal = reader.DisposeAsync().AsTask();
        Assert.False(disposal.IsCompleted);
        await rows.DisposeAsync();
        await disposal.WaitAsync(TimeSpan.FromSeconds(10), Ct);
    }

    [Fact]
    public async Task IncludeQuery_SharesBudgetAcrossParentAndChildTables()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false, twoTables: true);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);

        await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.Query<Document>("Docs").Include(document => document.Children).ToListAsync(Ct));

        await using AccessReader larger = await OpenReaderAsync(database, 4 * ContentLength);
        Document parent = Assert.Single(await larger.Query<Document>("Docs").Include(document => document.Children).ToListAsync(Ct));
        Assert.NotNull(Assert.Single(parent.Children).Files);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LinkedMultiTableQuery_SharesBudgetAcrossSourceReaders(bool strict)
    {
        string directory = Path.Combine(JetDatabaseWriter.Tests.Relationships.LinkedTestPaths.GetCanonicalTempPath(), $"AttachmentBudgets-{Guid.NewGuid():N}");
        string sourcePath = Path.Combine(directory, "source.accdb");
        _ = Directory.CreateDirectory(directory);
        try
        {
            await using (MemoryStream source = await CreateDatabaseAsync(twoColumns: false, twoTables: true))
            {
                await File.WriteAllBytesAsync(sourcePath, source.ToArray(), Ct);
            }

            await using var host = new MemoryStream();
            await using (AccessWriter writer = await CreateWriterAsync(host))
            {
                await writer.CreateLinkedTableAsync("FirstDocs", sourcePath, "Docs", Ct);
                await writer.CreateLinkedTableAsync("SecondDocs", sourcePath, "OtherDocs", Ct);
            }

            await using AccessReader reader = await AccessReader.OpenAsync(
                host,
                new AccessReaderOptions
                {
                    UseLockFile = false,
                    StrictParsing = strict,
                    LinkedSourceResolver = JetDatabaseWriter.Tests.Relationships.LinkedTestPaths.ResolverFor(sourcePath),
                    MaxAttachmentContentBytesPerOperation = 3 * ContentLength,
                },
                leaveOpen: true,
                cancellationToken: Ct);
            IQueryable<Document> query = reader.Query<Document>("FirstDocs").Concat(reader.Query<Document>("SecondDocs"));

            await Assert.ThrowsAsync<JetLimitationException>(async () => await query.ToListAsync(Ct));
            Assert.Single(await reader.Query<Document>("FirstDocs").ToListAsync(Ct));
        }
        finally
        {
            File.Delete(sourcePath);
            Directory.Delete(directory);
        }
    }

    [Fact]
    public void AttachmentBudget_LinkedReadersShareNestedScopeAndResetAfterExit()
    {
        var root = ReaderOpenContext.CreateRoot(new AccessReaderOptions { MaxAttachmentContentBytesPerOperation = 23 });
        ReaderOpenContext linked = root.ForLinkedSource("source.accdb", new JetDatabaseWriter.Relationships.LinkedSourceTraversal(1, []));
        var rootGate = new AsyncReentrantOperationGate(typeof(AccessReader), root.AttachmentReadContext);
        var linkedGate = new AsyncReentrantOperationGate(typeof(AccessReader), linked.AttachmentReadContext);
        byte[] wrapped = AttachmentWrapper.Encode("txt", [1, 2, 3]);

        using (rootGate.Enter())
        {
            Assert.True(AttachmentWrapper.TryDecode(wrapped, out _, out _, contentBudget: root.AttachmentReadContext.Budget, cancellationToken: Ct));
            using (linkedGate.Enter())
            {
                Assert.Throws<JetLimitationException>(() => AttachmentWrapper.TryDecode(wrapped, out _, out _, contentBudget: linked.AttachmentReadContext.Budget, cancellationToken: Ct));
            }
        }

        using (linkedGate.Enter())
        {
            Assert.True(AttachmentWrapper.TryDecode(wrapped, out _, out _, contentBudget: linked.AttachmentReadContext.Budget, cancellationToken: Ct));
        }
    }

    [Fact]
    public async Task AttachmentRead_BoundsManyIndividuallyValidCompressedValues()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 2 * ContentLength);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
            await reader.GetAttachmentsAsync("Docs", "Files", Ct));

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(false, true)]
    public async Task TableScan_SharesBudgetAcrossAttachmentColumns(bool strict, bool strings)
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: true);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength, strict);

        JetLimitationException failure = await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            using DataTable table = strings
                ? await reader.ReadTableAsStringsAsync("Docs", cancellationToken: Ct)
                : await reader.ReadTableAsync("Docs", cancellationToken: Ct);
        });

        Assert.Equal(JetErrorCode.ValueTooLarge, failure.ErrorCode);
    }

    [Fact]
    public async Task AttachmentRead_ExplicitLargerBudgetPermitsValuesAndResetsBetweenReads()
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength);

        for (int attempt = 0; attempt < 2; attempt++)
        {
            IReadOnlyList<AttachmentRecord> attachments = await reader.GetAttachmentsAsync("Docs", "Files", Ct);
            Assert.Equal(3, attachments.Count);
            Assert.All(attachments, attachment => Assert.Equal(new byte[PayloadLength], attachment.FileData));
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ReaderOpen_RejectsNonpositiveOperationBudget(long limit)
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessReader reader = await OpenReaderAsync(database, limit);
        });
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task ReaderOpen_RejectsNonpositiveWorkBudget(long limit)
    {
        await using MemoryStream database = await CreateDatabaseAsync(twoColumns: false);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
        {
            await using AccessReader reader = await OpenReaderAsync(database, 3 * ContentLength, workLimit: limit);
        });
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream database, long limit, bool strict = true, long workLimit = 2L * 1024 * 1024 * 1024)
        => AccessReader.OpenAsync(
            database,
            new AccessReaderOptions
            {
                UseLockFile = false,
                MaxAttachmentContentBytes = ContentLength,
                MaxAttachmentContentBytesPerOperation = limit,
                MaxAttachmentDecodeWorkBytesPerOperation = workLimit,
                StrictParsing = strict,
            },
            leaveOpen: true,
            cancellationToken: Ct);

    private static byte[] BuildCostlyWrapper()
    {
        byte[] original = AttachmentWrapper.Encode("txt", [1, 2, 3]);
        byte[] compressed = PrependEmptyBlocks(original.AsSpan(10, original.Length - 14), 65536);
        byte[] wrapped = new byte[compressed.Length + 14];
        original.AsSpan(0, 10).CopyTo(wrapped);
        compressed.CopyTo(wrapped.AsSpan(10));
        original.AsSpan(original.Length - 4).CopyTo(wrapped.AsSpan(wrapped.Length - 4));
        return wrapped;
    }

    private static byte[] PrependEmptyBlocks(ReadOnlySpan<byte> compressed, int blockCount)
    {
        byte[] result = new byte[(blockCount * 5) + compressed.Length];
        for (int block = 0; block < blockCount; block++)
        {
            int start = block * 5;
            result[start + 3] = 0xFF;
            result[start + 4] = 0xFF;
        }

        compressed.CopyTo(result.AsSpan(blockCount * 5));
        return result;
    }

    private static async Task<MemoryStream> CreateDatabaseAsync(bool twoColumns, bool twoTables = false)
    {
        var stream = new MemoryStream();
        try
        {
            await using (AccessWriter writer = await CreateWriterAsync(stream))
            {
                var columns = new List<ColumnDefinition>
                {
                    new("Id", typeof(int)) { IsPrimaryKey = true },
                    new("Files", typeof(byte[])) { IsAttachment = true },
                };
                if (twoColumns)
                {
                    columns.Add(new("OtherFiles", typeof(byte[])) { IsAttachment = true });
                }

                await writer.CreateTableAsync("Docs", columns, Ct);
                await writer.InsertRowAsync("Docs", twoColumns ? [1, DBNull.Value, DBNull.Value] : [1, DBNull.Value], Ct);
                var key = new Dictionary<string, object?> { ["Id"] = 1 };
                byte[] payload = new byte[PayloadLength];
                for (int attachment = 0; attachment < 3; attachment++)
                {
                    await writer.AddAttachmentAsync("Docs", "Files", key, new AttachmentInput($"{attachment}.txt", payload), Ct);
                }

                if (twoColumns)
                {
                    await writer.AddAttachmentAsync("Docs", "OtherFiles", key, new AttachmentInput("other.jpg", payload), Ct);
                }

                if (twoTables)
                {
                    await writer.CreateTableAsync("OtherDocs", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true }], Ct);
                    await writer.CreateRelationshipAsync(new("FK_OtherDocs_Docs", "Docs", "Id", "OtherDocs", "Id"), Ct);
                    await writer.InsertRowAsync("OtherDocs", [1, DBNull.Value], Ct);
                    await writer.AddAttachmentAsync("OtherDocs", "Files", key, new AttachmentInput("other.txt", payload), Ct);
                }
            }

            stream.Position = 0;
            return stream;
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    private sealed class CancelDuringReadStream(byte[] bytes, CancellationTokenSource cancellation) : MemoryStream(bytes)
    {
        internal int ReadCalls { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = base.Read(buffer, offset, Math.Min(count, 1024));
            this.ReadCalls++;
            if (this.ReadCalls == 2)
            {
                cancellation.Cancel();
            }

            return read;
        }
    }

    private sealed class Document
    {
        public int Id { get; set; }

        public byte[]? Files { get; set; }

        public List<RelatedDocument> Children { get; set; } = [];
    }

#pragma warning disable CA1812 // Include creates related entities through its runtime mapper.
    [Table("OtherDocs")]
    private sealed class RelatedDocument
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public byte[]? Files { get; set; }
    }
}
