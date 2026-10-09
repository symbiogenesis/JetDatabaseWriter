namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using Xunit;

/// <summary>Exercises bounded page conversion independently of native header preparation.</summary>
public sealed class StreamingEncryptionTests
{
    /// <summary>Conversion decodes and re-encodes every data page without changing page numbers.</summary>
    [Fact]
    public async Task TransformPages_PreservesHeaderAndPageNumbers()
    {
        const int pageSize = 2048;
        byte[] sourceBytes = new byte[pageSize * 3];
        sourceBytes.AsSpan(pageSize, pageSize).Fill(17);
        sourceBytes.AsSpan(pageSize * 2, pageSize).Fill(23);
        byte[] targetHeader = new byte[pageSize];
        targetHeader.AsSpan().Fill(31);
        using var sourceCodec = new XorCodec(5);
        using var targetCodec = new XorCodec(9);
        await using var source = new MemoryStream(sourceBytes, writable: false);
        await using var target = new MemoryStream();

        await EncryptionConverter.TransformPagesAsync(source, target, targetHeader, sourceCodec, targetCodec, TestContext.Current.CancellationToken);

        byte[] result = target.ToArray();
        Assert.Equal(sourceBytes.Length, result.Length);
        Assert.Equal(targetHeader, result.AsSpan(0, pageSize).ToArray());
        Assert.Equal((byte)(17 ^ 5 ^ 9), result[pageSize]);
        Assert.Equal((byte)(23 ^ 5 ^ 9), result[pageSize * 2]);
        Assert.Equal(2, sourceCodec.Pages);
        Assert.Equal(2, targetCodec.Pages);
    }

    /// <summary>A partial final page is rejected before the destination is changed.</summary>
    [Fact]
    public async Task TransformPages_PartialPage_IsRejectedBeforeWriting()
    {
        await using var source = new MemoryStream(new byte[2049], writable: false);
        await using var target = new MemoryStream();
        using var codec = new XorCodec(0);

        await Assert.ThrowsAsync<InvalidDataException>(async () => await EncryptionConverter.TransformPagesAsync(source, target, new byte[2048], codec, codec, TestContext.Current.CancellationToken));
        Assert.Equal(0, target.Length);
    }

    /// <summary>A pre-canceled conversion does not consume the source or write the destination.</summary>
    [Fact]
    public async Task TransformPages_PreCanceled_DoesNotWrite()
    {
        await using var source = new MemoryStream(new byte[4096], writable: false);
        await using var target = new MemoryStream();
        using var codec = new XorCodec(0);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await EncryptionConverter.TransformPagesAsync(source, target, new byte[2048], codec, codec, cancellation.Token));
        Assert.Equal(0, target.Length);
        Assert.Equal(0, source.Position);
    }

    /// <summary>Staging failures preserve the original and leave unrelated temporary files alone.</summary>
    /// <param name="cancel">Whether staging is canceled instead of failing with an I/O error.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Replacement_InterruptedStaging_PreservesOriginal(bool cancel)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"StreamingEncryptionTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.mdb");
        string unrelated = path + ".reenc-unrelated.tmp";
        byte[] original = [1, 2, 3];
        using var cancellation = new CancellationTokenSource();
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            await File.WriteAllBytesAsync(unrelated, original, TestContext.Current.CancellationToken);
            async ValueTask WriteAsync(Stream destination, CancellationToken token)
            {
                await destination.WriteAsync(new byte[2048], token);
                if (cancel)
                {
                    await cancellation.CancelAsync();
                    token.ThrowIfCancellationRequested();
                }

#pragma warning disable RCS1140 // This test local function deliberately injects a staging failure.
                throw new IOException("Injected staging failure.");
#pragma warning restore RCS1140
            }

            if (cancel)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await EncryptionFileReplacement.ReplaceAsync(path, WriteAsync, cancellation.Token));
            }
            else
            {
                await Assert.ThrowsAsync<IOException>(async () => await EncryptionFileReplacement.ReplaceAsync(path, WriteAsync, cancellation.Token));
            }

            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Equal(unrelated, Assert.Single(Directory.GetFiles(directory, "*.tmp")));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class XorCodec(byte key) : IPageCodec
    {
        internal int Pages { get; private set; }

        public bool HasEncryption => true;

        public void Decode(byte[] data, int offset, long pageNumber, int pageSize)
        {
            Assert.Equal(this.Pages + 1, pageNumber);
            this.Pages++;
            for (int i = offset; i < offset + pageSize; i++)
            {
                data[i] ^= key;
            }
        }

        public void Encode(byte[] data, int offset, long pageNumber, int pageSize) => this.Decode(data, offset, pageNumber, pageSize);

        public void Dispose()
        {
        }
    }
}
