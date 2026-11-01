namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Buffers.Binary;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.Transactions;
using Xunit;

/// <summary>Diagnostic listeners cannot change best-effort outcomes or receive private file details.</summary>
public sealed class DiagnosticSafetyTests
{
    /// <summary>Lock-file diagnostics exclude paths and tolerate listener failures.</summary>
    /// <param name="throwFromListener">Whether the diagnostic listener throws.</param>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BestEffortLockFileFailure_IsSafe(bool throwFromListener)
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-diagnostic-" + Guid.NewGuid().ToString("N"));
        string path = Path.Combine(directory, "private-database-name.accdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        Directory.CreateDirectory(lockPath);
        using var listener = new RecordingListener(throwFromListener);
        var scope = new TestTraceScope(listener);
        try
        {
            using var slot = LockFileSlotWriter.Open(path, nameof(AccessReader), respectExisting: false, "PrivateMachine", "PrivateUser");
            Assert.Null(slot);
            string message = listener.Messages.ToString();
            Assert.Contains("lock-file", message, StringComparison.Ordinal);
            Assert.DoesNotContain(directory, message, StringComparison.Ordinal);
            Assert.DoesNotContain("private-database-name", message, StringComparison.Ordinal);
            Assert.DoesNotContain("PrivateMachine", message, StringComparison.Ordinal);
            Assert.DoesNotContain("PrivateUser", message, StringComparison.Ordinal);
        }
        finally
        {
            scope.Dispose();
            Directory.Delete(lockPath);
            Directory.Delete(directory);
        }
    }

    /// <summary>Required lock-file I/O failures retain their original native exception.</summary>
    [Fact]
    public void RequiredLockFileFailure_PreservesIoFailure()
    {
        string directory = Path.Combine(Path.GetTempPath(), "jdw-diagnostic-" + Guid.NewGuid().ToString("N"));
        string path = Path.Combine(directory, "database.accdb");
        string lockPath = LockFileSlotWriter.GetLockFilePath(path);
        Directory.CreateDirectory(lockPath);
        using var listener = new RecordingListener(throwFromListener: true);
        var scope = new TestTraceScope(listener);
        try
        {
            Assert.Throws<UnauthorizedAccessException>(() => LockFileSlotWriter.Open(path, nameof(AccessWriter), respectExisting: true, "Machine", "User"));
            Assert.Equal(0, listener.Messages.Length);
        }
        finally
        {
            scope.Dispose();
            Directory.Delete(lockPath);
            Directory.Delete(directory);
        }
    }

    /// <summary>A listener failure cannot abort a lenient overflow-row scan.</summary>
    /// <param name="readMode">The scan's page-read mode.</param>
    [Theory]
    [InlineData(PageReadOptimizationMode.Disabled)]
    [InlineData(PageReadOptimizationMode.Enabled)]
    public async Task CorruptOverflowRow_IgnoresListenerFailure(PageReadOptimizationMode readMode)
    {
        byte[] bytes = await SyntheticOverflowRows.CreateTableAsync(DatabaseFormat.AceAccdb, "Items", 300, primaryKey: false, TestContext.Current.CancellationToken);
        _ = await SyntheticOverflowRows.MoveRowAsync(bytes, "Items", OverflowRowLayout.PointerPastEndOfFile, TestContext.Current.CancellationToken);
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, StrictParsing = false, DiagnosticsEnabled = true, PageReadOptimizationMode = readMode }, leaveOpen: true, TestContext.Current.CancellationToken);
        using var listener = new RecordingListener(throwFromListener: true);
        var scope = new TestTraceScope(listener);
        try
        {
            using DataTable rows = await reader.ReadTableAsync("Items", cancellationToken: TestContext.Current.CancellationToken);
            Assert.True(rows.Rows.Count >= 299);
            Assert.Equal(1, reader.OmittedRowCount);
            Assert.Contains("Skipped corrupt overflow row", listener.Messages.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>A listener failure cannot abort lenient complex-column discovery.</summary>
    [Fact]
    public async Task InvalidComplexTemplate_IgnoresListenerFailure()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("Docs", [new ColumnDefinition("Files", typeof(byte[])) { IsAttachment = true }], TestContext.Current.CancellationToken);
        }

        await using (AccessWriter harness = await TestDatabase.OpenWriterAsync(stream, cancellationToken: TestContext.Current.CancellationToken))
        {
            long metadataPage = await harness.Services.CatalogRows.FindSystemTableTdefPageAsync("MSysComplexColumns", TestContext.Current.CancellationToken);
            TableDef definition = Assert.IsType<TableDef>(await harness.Database.TableDefs.ReadTableDefAsync(metadataPage, TestContext.Current.CancellationToken));
            ColumnInfo template = Assert.IsType<ColumnInfo>(definition.FindColumn("ComplexTypeObjectID"));
            RowLocation location = Assert.Single(await harness.Database.GetLiveRowLocationsAsync(metadataPage, TestContext.Current.CancellationToken));
            byte[] page = await harness.Database.Pages.ReadPageCopyAsync(location.PageNumber, TestContext.Current.CancellationToken);
            BinaryPrimitives.WriteInt32LittleEndian(page.AsSpan(location.RowStart + harness.Database.Format.RowFields.NumCols + template.FixedOff, 4), int.MaxValue);
            await harness.Pager.WritePageAsync(location.PageNumber, page, TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false, DiagnosticsEnabled = true, StrictParsing = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        using var listener = new RecordingListener(throwFromListener: true);
        var scope = new TestTraceScope(listener);
        try
        {
            Assert.Empty(await reader.GetComplexColumnsAsync("Docs", TestContext.Current.CancellationToken));
            Assert.Contains("ComplexColumnReader", listener.Messages.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            scope.Dispose();
        }
    }

    private sealed class RecordingListener(bool throwFromListener) : TraceListener
    {
        internal StringBuilder Messages { get; } = new();

        public override void Write(string? message)
        {
            this.Messages.Append(message);
            if (throwFromListener)
            {
                throw new IOException("diagnostic callback failure");
            }
        }

        public override void WriteLine(string? message) => this.Write(message);
    }
}
