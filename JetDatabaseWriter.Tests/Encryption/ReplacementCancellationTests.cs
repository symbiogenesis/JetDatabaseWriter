namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Buffers;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using Xunit;

/// <summary>Exercises cancellation after replacement has created its owned temporary file.</summary>
public sealed class ReplacementCancellationTests
{
    /// <summary>Cancellation during payload consumption preserves the destination and removes the temporary file.</summary>
    [Fact]
    public async Task Replace_CanceledWhileConsumingPayload_PreservesOriginalAndCleansTemporaryFile()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"ReplacementCancellationTests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "database.accdb");
        byte[] original = [1, 2, 3, 4];
        using var cancellation = new CancellationTokenSource();
        try
        {
            await File.WriteAllBytesAsync(path, original, TestContext.Current.CancellationToken);
            using IMemoryOwner<byte> payload = new CancelingPayload(() =>
            {
                Assert.Single(Directory.GetFiles(directory, "*.tmp"));
                cancellation.Cancel();
            });
            OperationCanceledException error = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await EncryptionManager.ReplaceFileAtomicAsync(path, payload.Memory, cancellation.Token));
            Assert.Equal(cancellation.Token, error.CancellationToken);
            Assert.True(cancellation.IsCancellationRequested);
            Assert.Equal(original, await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetFiles(directory, "*.tmp"));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private sealed class CancelingPayload(Action consuming) : MemoryManager<byte>
    {
        private readonly byte[] bytes = new byte[8192];

        private bool consumed;

        public override Memory<byte> Memory => this.CreateMemory(this.bytes.Length);

        public override Span<byte> GetSpan()
        {
            this.OnConsuming();
            return this.bytes;
        }

        public override unsafe MemoryHandle Pin(int elementIndex = 0)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(elementIndex);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(elementIndex, this.bytes.Length);
            this.OnConsuming();
            GCHandle handle = GCHandle.Alloc(this.bytes, GCHandleType.Pinned);
            return new MemoryHandle((byte*)handle.AddrOfPinnedObject() + elementIndex, handle);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }

        private void OnConsuming()
        {
            if (!this.consumed)
            {
                this.consumed = true;
                consuming();
            }
        }
    }
}
