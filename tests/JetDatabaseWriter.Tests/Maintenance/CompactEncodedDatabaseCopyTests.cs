namespace JetDatabaseWriter.Tests.Maintenance;

using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Maintenance;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Codec admission and independent reopening protect private compact staging.</summary>
public sealed class CompactEncodedDatabaseCopyTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task HeaderMismatch_RefusesBeforeCreatingOutput()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        byte[] header = image.AsSpan(0, 2048).ToArray();
        image[0x56] ^= 1;
        bool created = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(image, writable: false)),
            _ =>
            {
                created = true;
                return ValueTask.FromResult<Stream>(new MemoryStream());
            },
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(header, default, workspace, new(), Ct));
        Assert.False(created);
    }

    [Fact]
    public async Task PageBudget_RefusesBeforeCreatingOutput()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        bool created = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(image, writable: false)),
            _ =>
            {
                created = true;
                return ValueTask.FromResult<Stream>(new MemoryStream());
            },
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new() { MaxPages = 2 }, Ct));
        Assert.False(created);
    }

    [Fact]
    public async Task ReopenedCiphertextChange_RefusesWithoutAcceptance()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        byte[] stored = [];
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(image, writable: false)),
            _ => ValueTask.FromResult<Stream>(new CapturedOutput(bytes => stored = bytes)),
            _ =>
            {
                stored[2048] ^= 1;
                return ValueTask.FromResult<Stream>(new MemoryStream(stored, writable: false));
            });
        await Assert.ThrowsAsync<JetCorruptDataException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new(), Ct));
    }

    [Fact]
    public async Task PartialReadRequests_ExhaustCumulativeAllowanceBeforeOutput()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        bool created = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new OneByteInput(image)),
            _ =>
            {
                created = true;
                return ValueTask.FromResult<Stream>(new MemoryStream());
            },
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new() { MaxReadBytes = 4096 }, Ct));
        Assert.False(created);
    }

    [Fact]
    public async Task WrongPassword_RefusesBeforeOutput()
    {
        byte[] encrypted = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeJet4Rc4.mdb"), Ct);
        bool created = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(encrypted, writable: false)),
            _ =>
            {
                created = true;
                return ValueTask.FromResult<Stream>(new MemoryStream());
            },
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetAccessDeniedException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(encrypted.AsMemory(0, 4096), "wrong".AsMemory(), workspace, new(), Ct));
        Assert.False(created);
    }

    [Fact]
    public async Task UnsupportedProvider_RefusesBeforeAnyFactory()
    {
        byte[] encrypted = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceStandard.accdb"), Ct);
        byte[] header = encrypted.AsSpan(0, 4096).ToArray();
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(0x2AF), 0xDEADBEEF);
        bool opened = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ =>
            {
                opened = true;
                return ValueTask.FromResult<Stream>(new MemoryStream(encrypted, writable: false));
            },
            _ => throw new InvalidOperationException(),
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetNotSupportedException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(header, "Password1234_".AsMemory(), workspace, new(), Ct));
        Assert.False(opened);
    }

    [Fact]
    public async Task AggregatePasswordBudget_IncludesIndependentReopening()
    {
        byte[] encrypted = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceStandard.accdb"), Ct);
        byte[] staging = new byte[4096 * 3];
        encrypted.AsSpan(0, 4096).CopyTo(staging);
        byte[] stored = [];
        bool closed = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(staging, writable: false)),
            _ => ValueTask.FromResult<Stream>(new CapturedOutput(bytes =>
            {
                stored = bytes;
                closed = true;
            })),
            _ => ValueTask.FromResult<Stream>(new MemoryStream(stored, writable: false)));
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(staging.AsMemory(0, 4096), "Password1234_".AsMemory(), workspace, new() { MaxTotalEncryptionSpinCount = 50_000 }, Ct));
        Assert.True(closed);
        Assert.Equal(staging.Length, stored.Length);
    }

    [Fact]
    public async Task CancellationAndWorkingBudget_RefuseBeforeFactories()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        bool opened = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ =>
            {
                opened = true;
                return ValueTask.FromResult<Stream>(new MemoryStream(image, writable: false));
            },
            _ => throw new InvalidOperationException(),
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new() { MaxWorkingBytes = 1 }, Ct));
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new(), cancelled.Token));
        Assert.False(opened);
    }

    [Fact]
    public async Task DirectOutputAlias_RefusesAndClosesTransferredInput()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        await using var input = new MemoryStream(image, writable: false);
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(input),
            _ => ValueTask.FromResult<Stream>(input),
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAsync<ArgumentException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new(), Ct));
        Assert.False(input.CanRead);
    }

    [Fact]
    public async Task LateFactoryCancellation_ClosesTransferredOutputAndInput()
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        await using var input = new MemoryStream(image, writable: false);
        using var cancellation = new CancellationTokenSource();
        bool closed = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(input),
            async _ =>
            {
                await cancellation.CancelAsync();
                return new CapturedOutput(_ => closed = true);
            },
            _ => throw new InvalidOperationException());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, new(), cancellation.Token));
        Assert.True(closed);
        Assert.False(input.CanRead);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CumulativeIoBudget_ChargesOutputAndIndependentComparison(bool verification)
    {
        byte[] image = TDefPageBuilder.BuildEmptyDatabase(DatabaseFormat.Jet3Mdb);
        byte[] stored = [];
        bool closed = false;
        var workspace = new CompactEncodedDatabaseWorkspace(
            _ => ValueTask.FromResult<Stream>(new MemoryStream(image, writable: false)),
            _ => ValueTask.FromResult<Stream>(new CapturedOutput(bytes =>
            {
                stored = bytes;
                closed = true;
            })),
            _ => ValueTask.FromResult<Stream>(new MemoryStream(stored, writable: false)));
        CompactEncodedDatabaseBudget budget = verification ? new() { MaxReadBytes = image.Length } : new() { MaxWriteBytes = 0 };
        await Assert.ThrowsAsync<JetLimitationException>(async () => await CompactEncodedDatabaseCopy.EncodeAndValidateAsync(image.AsMemory(0, 2048), default, workspace, budget, Ct));
        Assert.True(closed);
        Assert.Equal(verification ? image.Length : 0, stored.Length);
    }

    private sealed class OneByteInput(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => base.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
    }

    private sealed class CapturedOutput(Action<byte[]> capture) : MemoryStream
    {
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                capture(this.ToArray());
            }

            base.Dispose(disposing);
        }
    }
}
