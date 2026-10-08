namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Reads immutable native flat Agile output created by DAO.DBEngine.120 on 2026-10-06 with dbVersion120 (128), dbEncrypt (2) and password Native123.</summary>
public sealed class NativeAceEncryptionTests
{
    /// <summary>Native ACE uses a flat database with an embedded Agile descriptor.</summary>
    [Fact]
    public async Task NativeAgile_ReadsKnownRow()
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb");
        Assert.Equal(AccessEncryptionFormat.AccdbAgile, await AccessWriter.DetectEncryptionFormatAsync(path, TestContext.Current.CancellationToken));
        await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("Native123") { UseLockFile = false }, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(rows.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Id"]);
        Assert.Equal("Native encrypted row", row["Label"]);
    }

    /// <summary>The native password verifier rejects a wrong password before page decoding.</summary>
    [Fact]
    public async Task NativeAgile_WrongPasswordIsRefused()
    {
        string path = Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb");
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            await using AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions("wrong") { UseLockFile = false }, TestContext.Current.CancellationToken);
        });
    }

    /// <summary>Native encrypted pages can be updated without replacing their encryption header.</summary>
    [Fact]
    public async Task NativeAgile_WriterUpdatesEncryptedPages()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.InsertRowAsync("T", [8, "Library updated native file"], TestContext.Current.CancellationToken);
        }

        Assert.Equal(original.AsSpan(0, Constants.PageSizes.Jet4).ToArray(), stream.ToArray().AsSpan(0, Constants.PageSizes.Jet4).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 8) && Equals(row["Label"], "Library updated native file"));
    }

    /// <summary>Free-page maintenance preserves native encryption and retained rows.</summary>
    [Fact]
    public async Task NativeAgile_ScrubAndShrinkPreserveHeaderAndRows()
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        long populatedLength;
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Maintenance", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Maintenance", [1, new byte[32_000]], TestContext.Current.CancellationToken);
            populatedLength = stream.Length;
            await writer.DropTableAsync("Maintenance", TestContext.Current.CancellationToken);
            Assert.True(await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken) > 0);
            Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
        }

        Assert.True(stream.Length < populatedLength);
        Assert.Equal(original.AsSpan(0, Constants.PageSizes.Jet4).ToArray(), stream.ToArray().AsSpan(0, Constants.PageSizes.Jet4).ToArray());
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        DataRow row = Assert.Single(rows.Rows.Cast<DataRow>());
        Assert.Equal(7, row["Id"]);
        Assert.Equal("Native encrypted row", row["Label"]);
    }

    /// <summary>Native scrub rollback restores ciphertext after torn writes and flush faults.</summary>
    /// <param name="fault">The physical fault to inject.</param>
    [Theory]
    [InlineData("write")]
    [InlineData("partial")]
    [InlineData("flush")]
    public async Task NativeAgile_ScrubFailureRestoresCiphertext(string fault)
    {
        byte[] original = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new WriteFaultStream();
        await stream.WriteAsync(original, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        await writer.CreateTableAsync("Maintenance", [new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Payload", typeof(byte[]))], TestContext.Current.CancellationToken);
        await writer.InsertRowAsync("Maintenance", [1, new byte[32_000]], TestContext.Current.CancellationToken);
        await writer.DropTableAsync("Maintenance", TestContext.Current.CancellationToken);
        byte[] baseline = stream.ToArray();
        if (fault == "flush")
        {
            stream.FailOnFlush(1);
        }
        else if (fault == "partial")
        {
            stream.FailDuringWrite(2);
        }
        else
        {
            stream.FailOnWrite(2);
        }

        await Assert.ThrowsAsync<IOException>(async () => await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken));
        Assert.True(stream.Faulted);
        Assert.Equal(baseline, stream.ToArray());
        Assert.True(await writer.ScrubFreePagesAsync(TestContext.Current.CancellationToken) > 0);
        Assert.True(await writer.ShrinkDatabaseAsync(TestContext.Current.CancellationToken) > 0);
        await writer.InsertRowAsync("T", [8, "After scrub fault"], TestContext.Current.CancellationToken);
        await writer.DisposeAsync();
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions("Native123") { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using DataTable rows = await reader.ReadTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 7) && Equals(row["Label"], "Native encrypted row"));
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 8) && Equals(row["Label"], "After scrub fault"));
    }

    /// <summary>Reader and writer reject excessive native hashing before reading encrypted data pages.</summary>
    /// <param name="writer">Whether to open the writable facade.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NativeAgile_SpinBudgetStopsAtPageZero(bool writer)
    {
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(fixture, TestContext.Current.CancellationToken);
        stream.Position = 0;
        await using var counting = new CountingStream(stream);
        await Assert.ThrowsAsync<JetLimitationException>(async () =>
        {
            if (writer)
            {
                await using AccessWriter database = await AccessWriter.OpenAsync(counting, new AccessWriterOptions("Native123") { UseLockFile = false, MaxEncryptionSpinCount = 99_999 }, leaveOpen: true, TestContext.Current.CancellationToken);
            }
            else
            {
                await using AccessReader database = await AccessReader.OpenAsync(counting, new AccessReaderOptions("Native123") { UseLockFile = false, MaxEncryptionSpinCount = 99_999 }, leaveOpen: true, TestContext.Current.CancellationToken);
            }
        });

        Assert.Equal(Constants.PageSizes.Jet4, counting.BytesRead);
        Assert.Equal(0, counting.BytesWritten);
    }

    /// <summary>Unimplemented native password conversions leave every encrypted byte untouched.</summary>
    /// <param name="operation">The requested password operation.</param>
    [Theory]
    [InlineData("decrypt")]
    [InlineData("change")]
    [InlineData("encrypt")]
    public async Task NativeAgile_PasswordConversionLeavesSourceUntouched(string operation)
    {
        byte[] fixture = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream();
        await stream.WriteAsync(fixture, TestContext.Current.CancellationToken);
        stream.Position = 0;
        if (operation == "encrypt")
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await AccessWriter.EncryptAsync(stream, "Changed123".AsMemory(), AccessEncryptionFormat.AccdbAgile, TestContext.Current.CancellationToken));
        }
        else if (operation == "decrypt")
        {
            await Assert.ThrowsAsync<NotSupportedException>(async () => await AccessWriter.DecryptAsync(stream, "Native123".AsMemory(), TestContext.Current.CancellationToken));
        }
        else
        {
            await Assert.ThrowsAsync<NotSupportedException>(async () => await AccessWriter.ChangePasswordAsync(stream, "Native123".AsMemory(), "Changed123".AsMemory(), TestContext.Current.CancellationToken));
        }

        Assert.Equal(fixture, stream.ToArray());
    }
}
