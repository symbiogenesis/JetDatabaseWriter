namespace JetDatabaseWriter.Tests.ComplexColumns;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.ComplexColumns.Models;
using JetDatabaseWriter.Exceptions;
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

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream database, long limit, bool strict = true)
        => AccessReader.OpenAsync(
            database,
            new AccessReaderOptions
            {
                UseLockFile = false,
                MaxAttachmentContentBytes = ContentLength,
                MaxAttachmentContentBytesPerOperation = limit,
                StrictParsing = strict,
            },
            leaveOpen: true,
            cancellationToken: Ct);

    private static async Task<MemoryStream> CreateDatabaseAsync(bool twoColumns)
    {
        var stream = new MemoryStream();
        try
        {
            await using (AccessWriter writer = await CreateWriterAsync(stream))
            {
                var columns = new List<ColumnDefinition>
                {
                    new("Id", typeof(int)),
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
}
