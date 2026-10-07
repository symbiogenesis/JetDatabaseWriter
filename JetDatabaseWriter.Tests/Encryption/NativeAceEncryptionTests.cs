namespace JetDatabaseWriter.Tests.Encryption;

using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
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
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
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
        DataTable rows = await reader.ReadDataTableAsync("T", cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(2, rows.Rows.Count);
        Assert.Contains(rows.Rows.Cast<DataRow>(), row => Equals(row["Id"], 8) && Equals(row["Label"], "Library updated native file"));
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
}
