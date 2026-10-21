namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// The writer checks its options before it touches the file: a
/// <see cref="AccessWriterOptions.MaxTransactionPageBudget"/> of zero or less
/// fails <c>OpenAsync</c> and <c>CreateDatabaseAsync</c> with an
/// <see cref="ArgumentOutOfRangeException"/> on <c>options</c> that names the
/// property, before any file is opened, created or written and before a
/// lock-file slot is taken. A stream is neither read nor disposed, even one
/// the writer was to own. It used to open, and then fail the first
/// transaction with an error on an internal parameter, <c>maxPages</c>.
/// </summary>
public sealed class AccessWriterOptionsValidationTests : IDisposable
{
    private readonly List<string> paths = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(0, true, DatabaseFormat.Jet4Mdb)]
    [InlineData(-1, true, DatabaseFormat.Jet4Mdb)]
    [InlineData(0, true, DatabaseFormat.AceAccdb)]
    [InlineData(-1, true, DatabaseFormat.AceAccdb)]
    [InlineData(0, false, DatabaseFormat.Jet4Mdb)]
    [InlineData(-1, false, DatabaseFormat.Jet4Mdb)]
    [InlineData(0, false, DatabaseFormat.AceAccdb)]
    [InlineData(-1, false, DatabaseFormat.AceAccdb)]
    public async Task OpenAsync_NonPositiveMaxTransactionPageBudget_Throws(int budget, bool byPath, DatabaseFormat format)
    {
        string path = await this.CreateDatabaseFileAsync(format);
        byte[] before = await File.ReadAllBytesAsync(path, Ct);
        var options = new AccessWriterOptions { MaxTransactionPageBudget = budget };

        if (byPath)
        {
            AssertNamesBudget(budget, await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await AccessWriter.OpenAsync(path, options, Ct)));
        }
        else
        {
            foreach (bool leaveOpen in (bool[])[true, false])
            {
                await using var stream = new MemoryStream();
                await stream.WriteAsync(before, Ct);
                stream.Position = 0;
                AssertNamesBudget(budget, await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await AccessWriter.OpenAsync(stream, options, leaveOpen, Ct)));

                // Neither read nor disposed, even when the writer was to own it.
                Assert.True(stream.CanRead && stream.CanWrite && stream.CanSeek, $"leaveOpen {leaveOpen}: the stream was disposed.");
                Assert.Equal(before, stream.ToArray());
                Assert.Equal(0, stream.Position);
            }
        }

        Assert.Equal(before, await File.ReadAllBytesAsync(path, Ct));
        Assert.False(File.Exists(LockFilePath(path, format)), "No lock-file slot is taken.");

        // The file was never opened: it opens for writing again at once.
        await using AccessWriter writer = await AccessWriter.OpenAsync(path, new AccessWriterOptions(), Ct);
    }

    [Theory]
    [InlineData(0, DatabaseFormat.Jet3Mdb)]
    [InlineData(-1, DatabaseFormat.Jet4Mdb)]
    [InlineData(0, DatabaseFormat.AceAccdb)]
    [InlineData(int.MinValue, DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_NonPositiveBudget_CreatesNothing(int budget, DatabaseFormat format)
    {
        string path = this.NewPath(format);
        var options = new AccessWriterOptions { MaxTransactionPageBudget = budget };

        // The end state alone cannot tell a check before the create from one
        // after it, because the overload deletes a file it created when the
        // open fails; so no file may be opened at all.
        ArgumentOutOfRangeException pathError;
        using (FileStreamFactory.OpenTracker opens = FileStreamFactory.TrackOpens())
        {
            pathError = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
                async () => await AccessWriter.CreateDatabaseAsync(path, format, options, Ct));
            Assert.Empty(opens.OpenedPaths);
        }

        AssertNamesBudget(budget, pathError);
        Assert.False(File.Exists(path));
        Assert.False(File.Exists(LockFilePath(path, format)));

        foreach (bool leaveOpen in (bool[])[true, false])
        {
            await using var stream = new MemoryStream();
            AssertNamesBudget(budget, await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen, Ct)));
            Assert.True(stream.CanRead && stream.CanWrite && stream.CanSeek, $"leaveOpen {leaveOpen}: the stream was disposed.");
            Assert.Equal(0, stream.Length);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task OpenAsync_BudgetOfOne_Opens(DatabaseFormat format)
    {
        string path = await this.CreateDatabaseFileAsync(format);
        var options = new AccessWriterOptions { MaxTransactionPageBudget = 1 };

        await using (AccessWriter writer = await AccessWriter.OpenAsync(path, options, Ct))
        {
            await writer.InsertRowAsync("Items", [1], Ct);
        }

        await using var stream = new MemoryStream();
        await using (AccessWriter created = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, Ct))
        {
            await created.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        Assert.True(stream.Length > 0);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        foreach (string path in this.paths)
        {
            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static string LockFilePath(string path, DatabaseFormat format)
        => Path.ChangeExtension(path, format == DatabaseFormat.AceAccdb ? ".laccdb" : ".ldb");

    private static void AssertNamesBudget(int budget, ArgumentOutOfRangeException ex)
    {
        Assert.Equal("options", ex.ParamName);
        Assert.Contains("MaxTransactionPageBudget", ex.Message, StringComparison.Ordinal);
        Assert.Equal(budget, ex.ActualValue);
    }

    private string NewPath(DatabaseFormat format)
    {
        string path = Path.Combine(Path.GetTempPath(), $"WriterOptions_{Guid.NewGuid():N}{(format == DatabaseFormat.AceAccdb ? ".accdb" : ".mdb")}");
        this.paths.Add(path);
        this.paths.Add(LockFilePath(path, format));
        return path;
    }

    private async Task<string> CreateDatabaseFileAsync(DatabaseFormat format)
    {
        string path = this.NewPath(format);
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, format, new AccessWriterOptions(), Ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], Ct);
        }

        return path;
    }
}
