namespace JetDatabaseWriter.Tests.Relationships;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.TestSupport;
using Xunit;

/// <summary>Linked execution requires a caller-authorized source capability.</summary>
public sealed class LinkedSourceCapabilityTests
{
    [Fact]
    public async Task LinkedSource_SameDirectoryWithoutResolver_IsRefused()
    {
        string directory = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), $"linked-capability-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string source = Path.Combine(directory, "source.accdb");
        string host = Path.Combine(directory, "host.accdb");
        try
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(source, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("Data", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("Data", [1], TestContext.Current.CancellationToken);
            }

            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(host, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                await writer.CreateLinkedTableAsync("Linked", source, "Data", TestContext.Current.CancellationToken);
            }

            await using AccessReader reader = await AccessReader.OpenAsync(host, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    [InlineData(LinkedTableKind.Odbc)]
    public async Task LinkedSource_ExecutionSurfaces_RefuseUnavailableCapability(LinkedTableKind kind)
    {
        await using MemoryStream host = await CreateHostAsync(kind);
        await using AccessReader reader = await AccessReader.OpenAsync(host, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        string tableName = Assert.Single(await reader.ListLinkedTablesAsync(TestContext.Current.CancellationToken)).Name;
        Assert.True(await reader.TryLookupTableAsync(tableName, TestContext.Current.CancellationToken));
        _ = await reader.ListIndexesAsync(tableName, TestContext.Current.CancellationToken);
        _ = await reader.GetComplexColumnsAsync(tableName, TestContext.Current.CancellationToken);
        Type failure = kind == LinkedTableKind.Odbc ? typeof(NotSupportedException) : typeof(UnauthorizedAccessException);
        Func<Task>[] operations =
        [
            async () => _ = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken),
            async () => { using DataTable table = await reader.ReadTableAsync(tableName, cancellationToken: TestContext.Current.CancellationToken); },
            async () => { using DataTable table = await reader.ReadTableAsStringsAsync(tableName, cancellationToken: TestContext.Current.CancellationToken); },
            async () => _ = await reader.ReadTableAsync<CapabilityRow>(tableName, cancellationToken: TestContext.Current.CancellationToken),
            async () => _ = await reader.ReadTableAsync<CapabilityRow>(tableName, maxRows: 0, TestContext.Current.CancellationToken),
            async () => await DrainAsync(reader.Rows(tableName, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.RowsAsStrings(tableName, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.Rows<CapabilityRow>(tableName, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.Rows<CapabilityRow>(tableName, row => row.Id == 7, cancellationToken: TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.SeekRowsAsync(tableName, "unused", [7], TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.FromIndex(tableName, "unused").ToRowsAsync(TestContext.Current.CancellationToken)),
            async () => await DrainAsync(reader.FromIndex<CapabilityRow>(tableName, "unused").ToRowsAsync(TestContext.Current.CancellationToken)),
            async () => _ = await reader.GetAttachmentsAsync(tableName, "Files", TestContext.Current.CancellationToken),
            async () => _ = await reader.GetMultiValueItemsAsync(tableName, "Tags", TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CapabilityRow>(tableName).CountAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CapabilityRow>(tableName).Where(row => row.Id == 7).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CapabilityRow>(tableName).Take(0).ToListAsync(TestContext.Current.CancellationToken),
            async () => _ = await reader.Query<CapabilityRow>(tableName).Take(0).DefaultIfEmpty().ToListAsync(TestContext.Current.CancellationToken),
        ];
        foreach (Func<Task> operation in operations)
        {
            _ = await Assert.ThrowsAsync(failure, operation);
        }

        if (kind == LinkedTableKind.Odbc)
        {
            Assert.NotEmpty(await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken));
        }
        else
        {
            _ = await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken));
        }
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    public async Task LinkedSource_ExplicitStream_ReadsAcrossPublicSurfaces(LinkedTableKind kind)
    {
        byte[] source = await SourceBytesAsync(kind);
        var opened = new List<TrackedStream>();
        await using MemoryStream host = await CreateHostAsync(kind);
        var options = new AccessReaderOptions
        {
            LinkedSourceResolver = (link, hint, token) =>
            {
                token.ThrowIfCancellationRequested();
                Assert.Equal("Linked", link.Name);
                Assert.True(Path.IsPathFullyQualified(hint));
                var stream = new TrackedStream(source);
                opened.Add(stream);
                return new ValueTask<Stream>(stream);
            },
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Equal(1, await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        Assert.Single(await reader.GetColumnMetadataAsync("Linked", TestContext.Current.CancellationToken));
        Assert.Single(await reader.ReadTableAsync<CapabilityRow>("Linked", cancellationToken: TestContext.Current.CancellationToken));
        using (DataTable typed = await reader.ReadTableAsync("Linked", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Single(typed.Rows);
        }

        using (DataTable text = await reader.ReadTableAsStringsAsync("Linked", cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Single(text.Rows);
        }

        await DrainAsync(reader.Rows("Linked", cancellationToken: TestContext.Current.CancellationToken));
        await DrainAsync(reader.RowsAsStrings("Linked", cancellationToken: TestContext.Current.CancellationToken));
        await DrainAsync(reader.Rows<CapabilityRow>("Linked", cancellationToken: TestContext.Current.CancellationToken));
        await DrainAsync(reader.Rows<CapabilityRow>("Linked", row => row.Id == 7, cancellationToken: TestContext.Current.CancellationToken));
        Assert.Single(await reader.Query<CapabilityRow>("Linked").Where(row => row.Id == 7).ToListAsync(TestContext.Current.CancellationToken));
        Assert.NotEmpty(opened);
        Assert.All(opened, stream => Assert.True(stream.Disposed));
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    public async Task LinkedSource_OpenedHandle_RemainsAuthorizedAfterPathReplacement(LinkedTableKind kind)
    {
        string directory = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), $"linked-handle-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string sourcePath = Path.Combine(directory, kind == LinkedTableKind.Text ? "source.csv" : "source.accdb");
        string movedPath = Path.Combine(directory, "original");
        await File.WriteAllBytesAsync(sourcePath, await SourceBytesAsync(kind), TestContext.Current.CancellationToken);
        var authorized = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            await using MemoryStream host = await CreateHostAsync(kind, sourcePath);
            var options = new AccessReaderOptions
            {
                LinkedSourceResolver = async (_, hint, token) =>
                {
                    Assert.Equal(sourcePath, hint);
                    File.Move(sourcePath, movedPath);
                    await File.WriteAllTextAsync(sourcePath, "replacement must never be read", token);
                    await File.WriteAllTextAsync(sourcePath + ".jdw-journal", "untrusted sidecar must not be inspected", token);
                    return authorized;
                },
            };
            await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
            Assert.Equal(1, await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
            Assert.False(authorized.CanRead);
            Assert.False(File.Exists(Path.ChangeExtension(sourcePath, ".laccdb")));
            Assert.Equal("replacement must never be read", await File.ReadAllTextAsync(sourcePath, TestContext.Current.CancellationToken));
        }
        finally
        {
            await authorized.DisposeAsync();
            Directory.Delete(directory, recursive: true);
        }
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    public async Task LinkedSource_ReturnedStream_IsDisposedOnCancellation(LinkedTableKind kind)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var source = new TrackedStream(await SourceBytesAsync(kind));
        await using MemoryStream host = await CreateHostAsync(kind);
        var options = new AccessReaderOptions
        {
            LinkedSourceResolver = async (_, _, token) =>
            {
                Assert.Equal(cancellation.Token, token);
                await cancellation.CancelAsync();
                return source;
            },
        };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await reader.GetRealRowCountAsync("Linked", cancellation.Token));
        Assert.True(source.Disposed);
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    public async Task LinkedSource_ResolverIoFailure_IsPreserved(LinkedTableKind kind)
    {
        var failure = new IOException("Resolver I/O failure.");
        await using MemoryStream host = await CreateHostAsync(kind);
        var options = new AccessReaderOptions { LinkedSourceResolver = (_, _, _) => throw failure };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        IOException observed = await Assert.ThrowsAsync<IOException>(async () => await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        Assert.Same(failure, observed);
    }

    [Theory]
    [InlineData(LinkedTableKind.Access)]
    [InlineData(LinkedTableKind.Text)]
    public async Task LinkedSource_UnseekableStream_IsDisposedBeforeRefusal(LinkedTableKind kind)
    {
        await using var source = new TrackedStream([], seekable: false);
        await using MemoryStream host = await CreateHostAsync(kind);
        var options = new AccessReaderOptions { LinkedSourceResolver = (_, _, _) => new ValueTask<Stream>(source) };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        _ = await Assert.ThrowsAsync<ArgumentException>(async () => await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        Assert.True(source.Disposed);
    }

    [Theory]
    [InlineData(LinkedTableKind.Access, false)]
    [InlineData(LinkedTableKind.Access, true)]
    [InlineData(LinkedTableKind.Text, false)]
    [InlineData(LinkedTableKind.Text, true)]
    public async Task LinkedSource_SymlinkAndHardLinkHints_DoNotSelectOpenedContent(LinkedTableKind kind, bool hardLink)
    {
        string directory = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), $"linked-alias-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string target = Path.Combine(directory, "untrusted-target");
        string alias = Path.Combine(directory, kind == LinkedTableKind.Text ? "alias.csv" : "alias.accdb");
        await File.WriteAllTextAsync(target, "must never be opened", TestContext.Current.CancellationToken);
        try
        {
            if (hardLink)
            {
                PowerShellRunResult result = PowerShellProcessRunner.Run(
                    "pwsh",
                    ["-NoProfile", "-NonInteractive", "-Command", "New-Item -ItemType HardLink -Path $env:JET_LINK_ALIAS -Target $env:JET_LINK_TARGET -ErrorAction Stop | Out-Null"],
                    TimeSpan.FromSeconds(30),
                    workingDirectory: directory,
                    environmentOverrides: new Dictionary<string, string> { ["JET_LINK_ALIAS"] = alias, ["JET_LINK_TARGET"] = target });
                Assert.False(result.TimedOut);
                Assert.True(result.ExitCode == 0, result.StandardError);
            }
            else
            {
                File.CreateSymbolicLink(alias, target);
            }

            byte[] approved = await SourceBytesAsync(kind);
            await using MemoryStream host = await CreateHostAsync(kind, alias);
            var options = new AccessReaderOptions
            {
                LinkedSourceResolver = (_, hint, _) =>
                {
                    Assert.Equal(alias, hint);
                    return new ValueTask<Stream>(new MemoryStream(approved, writable: false));
                },
            };
            await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
            Assert.Equal(1, await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
            Assert.Equal("must never be opened", await File.ReadAllTextAsync(target, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(alias);
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task LinkedSource_TransitiveReads_InheritResolverAndBoundAliasTraversal()
    {
        string hint = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), "unopened-transitive.accdb");
        await using MemoryStream host = await CreateHostAsync(LinkedTableKind.Access, hint);
        byte[] nested = host.ToArray();
        int resolutions = 0;
        var returned = new List<TrackedStream>();
        var options = new AccessReaderOptions
        {
            LinkedSourceMaxDepth = 2,
            LinkedSourceResolver = (_, _, _) =>
            {
                resolutions++;
                var source = new TrackedStream(nested);
                returned.Add(source);
                return new ValueTask<Stream>(source);
            },
        };

        // Each returned database requests Data, so create the recurring link with that name.
        await using (var nestedStream = new MemoryStream())
        {
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(nestedStream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
            {
                await writer.CreateLinkedTableAsync("Data", Path.Combine("nested", "source.accdb"), "Data", TestContext.Current.CancellationToken);
            }

            nested = nestedStream.ToArray();
        }

        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        InvalidDataException failure = await Assert.ThrowsAsync<InvalidDataException>(async () => await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        Assert.Contains("depth", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, resolutions);
        Assert.All(returned, source => Assert.True(source.Disposed));
    }

    [Fact]
    public async Task LinkedSource_ParsingFailure_DisposesAuthorizedStream()
    {
        await using var source = new TrackedStream([]);
        await using MemoryStream host = await CreateHostAsync(LinkedTableKind.Access);
        var options = new AccessReaderOptions { LinkedSourceResolver = (_, _, _) => new ValueTask<Stream>(source) };
        await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
        _ = await Assert.ThrowsAsync<EndOfStreamException>(async () => await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task LinkedSource_MissingFileStreamName_IsNeverReopened()
    {
        string directory = Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), $"linked-unlinked-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string sourcePath = Path.Combine(directory, "original.accdb");
        await File.WriteAllBytesAsync(sourcePath, await SourceBytesAsync(LinkedTableKind.Access), TestContext.Current.CancellationToken);
        await using var authorized = new FileStream(sourcePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            File.Move(sourcePath, Path.Combine(directory, "renamed.accdb"));
            await using MemoryStream host = await CreateHostAsync(LinkedTableKind.Access, sourcePath);
            var options = new AccessReaderOptions { LinkedSourceResolver = (_, _, _) => new ValueTask<Stream>(authorized) };
            await using AccessReader reader = await AccessReader.OpenAsync(host, options, leaveOpen: true, TestContext.Current.CancellationToken);
            Assert.Equal(1, await reader.GetRealRowCountAsync("Linked", TestContext.Current.CancellationToken));
            Assert.False(File.Exists(sourcePath));
        }
        finally
        {
            await authorized.DisposeAsync();
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async ValueTask<byte[]> SourceBytesAsync(LinkedTableKind kind)
    {
        if (kind == LinkedTableKind.Text)
        {
            return "Id\r\n7\r\n"u8.ToArray();
        }

        await using var source = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(source, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Data", [new("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("Data", [7], TestContext.Current.CancellationToken);
        }

        return source.ToArray();
    }

    private static async ValueTask<MemoryStream> CreateHostAsync(LinkedTableKind kind, string? sourcePath = null)
    {
        if (kind == LinkedTableKind.Odbc)
        {
            return new MemoryStream(await File.ReadAllBytesAsync(TestDatabases.OdbcLinkerTestV2007, TestContext.Current.CancellationToken));
        }

        var host = new MemoryStream();
        try
        {
            sourcePath ??= Path.Combine(LinkedTestPaths.GetCanonicalTempPath(), $"unopened-{Guid.NewGuid():N}", kind == LinkedTableKind.Text ? "data.csv" : "source.accdb");
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(host, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
            {
                if (kind == LinkedTableKind.Text)
                {
                    await writer.CreateLinkedTextTableAsync("Linked", Path.GetDirectoryName(sourcePath)!, Path.GetFileName(sourcePath), "Text;HDR=YES;FMT=Delimited", TestContext.Current.CancellationToken);
                }
                else
                {
                    await writer.CreateLinkedTableAsync("Linked", sourcePath, "Data", TestContext.Current.CancellationToken);
                }
            }

            host.Position = 0;
            return host;
        }
        catch
        {
            await host.DisposeAsync();
            throw;
        }
    }

    private static async Task DrainAsync<T>(IAsyncEnumerable<T> rows)
    {
        await foreach (T row in rows)
        {
            Assert.NotNull(row);
        }
    }

    private sealed class CapabilityRow
    {
        public int Id { get; set; }
    }

    private sealed class TrackedStream(byte[] bytes, bool seekable = true) : MemoryStream(bytes, writable: false)
    {
        internal bool Disposed { get; private set; }

        public override bool CanSeek => seekable && base.CanSeek;

        protected override void Dispose(bool disposing)
        {
            this.Disposed = true;
            base.Dispose(disposing);
        }
    }
}
