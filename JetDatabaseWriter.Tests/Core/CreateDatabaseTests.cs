namespace JetDatabaseWriter.Tests.Core;

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Models;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>
/// Tests for <see cref="AccessWriter.CreateDatabaseAsync(Stream, DatabaseFormat, AccessWriterOptions, bool, CancellationToken)"/> that verify the creation of new,
/// empty JET databases from scratch — both file-path and stream-based overloads.
/// </summary>
public sealed class CreateDatabaseTests
{
#pragma warning disable SA1202 //  catalog name fixtures kept adjacent to the tests that consume them.
    private static readonly string[] FullCatalogColumnNames =
    [
        "Id", "ParentId", "Name", "Type", "DateCreate", "DateUpdate", "Owner",
        "Flags", "Database", "Connect", "ForeignName", "RmtInfoShort",
        "RmtInfoLong", "Lv", "LvProp", "LvModule", "LvExtra",
    ];

    private static readonly string[] SlimCatalogColumnNames =
    [
        "Id", "ParentId", "Name", "Type", "DateCreate", "DateUpdate", "Flags",
        "ForeignName", "Database",
    ];
#pragma warning restore SA1202

    // ── CreateDatabaseAsync (Stream, Jet4Mdb) ─────────────────────────────────

    [Fact]
    public async Task CreateDatabaseAsync_Stream_Jet4Mdb_ReturnsNonNullWriter()
    {
        var ms = new MemoryStream();

        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(writer);
    }

    [Fact]
    public async Task CreateDatabaseAsync_Stream_AceAccdb_ReturnsNonNullWriter()
    {
        var ms = new MemoryStream();

        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(writer);
    }

    // ── Round-trip: create → list tables (empty) ──────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_Stream_EmptyDatabase_ListTablesReturnsEmpty(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            // No tables created — dispose writer.
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Empty(tables);
    }

    // ── Round-trip: create → create table → verify columns ────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_Stream_CreateTable_TableIsReadable(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        string tableName = "People";
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Name", typeof(string), maxLength: 100),
            new("Active", typeof(bool)),
        };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Single(tables);
        Assert.Equal(tableName, tables[0]);

        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);
        Assert.Equal(3, meta.Count);
    }

    // ── Round-trip: create → insert → read back ───────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_Stream_InsertAndReadBack_DataSurvives(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        string tableName = "Items";
        var columns = new List<ColumnDefinition>
        {
            new("Id", typeof(int)),
            new("Label", typeof(string), maxLength: 100),
        };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [1, "Alpha"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [2, "Beta"], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        long count = await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken);

        Assert.Equal(2, count);
    }

    // ── Round-trip: create → multiple tables ──────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_Stream_MultipleTables_AllVisible(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("TableA", new List<ColumnDefinition> { new("X", typeof(int)) }, TestContext.Current.CancellationToken);
            await writer.CreateTableAsync("TableB", new List<ColumnDefinition> { new("Y", typeof(string), 50) }, TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, tables.Count);
        Assert.Contains("TableA", tables);
        Assert.Contains("TableB", tables);
    }

    // ── CreateDatabaseAsync (file path) ───────────────────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, ".mdb")]
    [InlineData(DatabaseFormat.AceAccdb, ".accdb")]
    public async Task CreateDatabaseAsync_Path_CreatesFileOnDisk(DatabaseFormat format, string ext)
    {
        string path = Path.Combine(Path.GetTempPath(), $"CreateTest_{Guid.NewGuid():N}{ext}");

        try
        {
            await using (var writer = await AccessWriter.CreateDatabaseAsync(path, format, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(writer);
            }

            Assert.True(File.Exists(path));
        }
        finally
        {
            TryDeleteFile(path);
            TryDeleteFile(Path.ChangeExtension(path, ext == ".accdb" ? ".laccdb" : ".ldb"));
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet4Mdb, ".mdb")]
    [InlineData(DatabaseFormat.AceAccdb, ".accdb")]
    public async Task CreateDatabaseAsync_Path_FileIsReadableAfterDispose(DatabaseFormat format, string ext)
    {
        string path = Path.Combine(Path.GetTempPath(), $"CreateRead_{Guid.NewGuid():N}{ext}");

        try
        {
            await using (var writer = await AccessWriter.CreateDatabaseAsync(path, format, cancellationToken: TestContext.Current.CancellationToken))
            {
                await writer.CreateTableAsync("T1", new List<ColumnDefinition> { new("Col", typeof(int)) }, TestContext.Current.CancellationToken);
                await writer.InsertRowAsync("T1", [42], TestContext.Current.CancellationToken);
            }

            await using var reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, cancellationToken: TestContext.Current.CancellationToken);
            var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
            Assert.Single(tables);
            Assert.Equal("T1", tables[0]);
        }
        finally
        {
            TryDeleteFile(path);
            TryDeleteFile(Path.ChangeExtension(path, ext == ".accdb" ? ".laccdb" : ".ldb"));
        }
    }

    // ── CreateDatabaseAsync: error cases ──────────────────────────────────────

    [Fact]
    public async Task CreateDatabaseAsync_Path_ExistingFile_ThrowsIOException()
    {
        string path = Path.GetTempFileName(); // creates a file

        try
        {
            await Assert.ThrowsAsync<IOException>(() =>
                AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken).AsTask());
        }
        finally
        {
            TryDeleteFile(path);
        }
    }

    [Fact]
    public async Task CreateDatabaseAsync_Path_NullPath_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            AccessWriter.CreateDatabaseAsync((string)null!, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task CreateDatabaseAsync_Stream_NullStream_ThrowsArgumentNullException()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            AccessWriter.CreateDatabaseAsync((Stream)null!, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken).AsTask());
    }

    [Fact]
    public async Task CreateDatabaseAsync_Stream_Jet3Mdb_ReturnsNonNullWriter()
    {
        var ms = new MemoryStream();

        await using var writer = await AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet3Mdb, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotNull(writer);
        Assert.Equal(DatabaseFormat.Jet3Mdb, writer.DatabaseFormat);
    }

    [Fact]
    public async Task CreateDatabaseAsync_Path_Jet3Mdb_CreatesFileOnDisk()
    {
        string path = Path.Combine(Path.GetTempPath(), $"CreateJet3_{Guid.NewGuid():N}.mdb");

        try
        {
            await using (var writer = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet3Mdb, cancellationToken: TestContext.Current.CancellationToken))
            {
                Assert.NotNull(writer);
                Assert.Equal(DatabaseFormat.Jet3Mdb, writer.DatabaseFormat);
            }

            Assert.True(File.Exists(path));
        }
        finally
        {
            TryDeleteFile(path);
            TryDeleteFile(Path.ChangeExtension(path, ".ldb"));
        }
    }

    [Fact]
    public async Task CreateDatabaseAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var ms = new MemoryStream();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            AccessWriter.CreateDatabaseAsync(ms, DatabaseFormat.Jet4Mdb, cancellationToken: cts.Token).AsTask());
    }

    // ── Column type round-trip ────────────────────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_VariousColumnTypes_SurviveRoundTrip(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        string tableName = "TypeTest";
        var columns = new List<ColumnDefinition>
        {
            new("IntCol", typeof(int)),
            new("ShortCol", typeof(short)),
            new("DoubleCol", typeof(double)),
            new("FloatCol", typeof(float)),
            new("DateCol", typeof(DateTime)),
            new("BoolCol", typeof(bool)),
            new("TextCol", typeof(string), maxLength: 200),
            new("ByteCol", typeof(byte)),
        };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(tableName, columns, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync(tableName, [99, (short)7, 3.14, 1.5f, new DateTime(2025, 6, 15), true, "Hello", (byte)42], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var meta = await reader.GetColumnMetadataAsync(tableName, TestContext.Current.CancellationToken);

        Assert.Equal(8, meta.Count);
        Assert.Equal(1, await reader.GetRealRowCountAsync(tableName, TestContext.Current.CancellationToken));
    }

    // ── Drop table on newly created database ──────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_DropTable_RemovesTable(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("ToDrop", new List<ColumnDefinition> { new("X", typeof(int)) }, TestContext.Current.CancellationToken);
            await writer.DropTableAsync("ToDrop", TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);

        Assert.Empty(tables);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    // ──  WriteFullCatalogSchema ───────────────────────────────

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_FullCatalogSchema_DefaultEmits17ColumnMSysObjects(DatabaseFormat format)
    {
        var ms = new MemoryStream();

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            // Default: WriteFullCatalogSchema = true
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        TableDef? msys = await reader.GetMSysObjectsTableDefAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(msys);
        Assert.Equal(FullCatalogColumnNames, msys!.Columns.ConvertAll(c => c.Name));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_FullCatalogSchema_OptedOutEmitsLegacy9ColumnMSysObjects(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        var opts = new AccessWriterOptions { UseLockFile = false, WriteFullCatalogSchema = false };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, opts, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            // Opt-out: WriteFullCatalogSchema = false retains the historical layout.
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
        TableDef? msys = await reader.GetMSysObjectsTableDefAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(msys);
        Assert.Equal(SlimCatalogColumnNames, msys!.Columns.ConvertAll(c => c.Name));
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task CreateDatabaseAsync_FullCatalogSchema_CreateTable_RoundTripsAcrossOpenClose(DatabaseFormat format)
    {
        var ms = new MemoryStream();
        var defs = new[] { new ColumnDefinition("Id", typeof(int)), new ColumnDefinition("Name", typeof(string), 50) };

        await using (var writer = await AccessWriter.CreateDatabaseAsync(ms, format, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("People", defs, TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("People", [1, "Alice"], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("People", [2, "Bob"], TestContext.Current.CancellationToken);
        }

        ms.Position = 0;
        await using var reader = await AccessReader.OpenAsync(ms, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);

        var tables = await reader.ListTablesAsync(TestContext.Current.CancellationToken);
        Assert.Equal(["People"], tables);

        var meta = await reader.GetColumnMetadataAsync("People", TestContext.Current.CancellationToken);
        Assert.Equal(["Id", "Name"], meta.ConvertAll(c => c.Name));

        // None of the new property fields are populated yet.
        Assert.All(meta, m =>
        {
            Assert.Null(m.DefaultValueExpression);
            Assert.Null(m.ValidationRuleExpression);
            Assert.Null(m.ValidationText);
            Assert.Null(m.Description);
        });

        var rows = new List<object[]>();
        await foreach (object[] row in reader.Rows("People", cancellationToken: TestContext.Current.CancellationToken))
        {
            rows.Add(row);
        }

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0][0]);
        Assert.Equal("Alice", rows[0][1]);
        Assert.Equal(2, rows[1][0]);
        Assert.Equal("Bob", rows[1][1]);
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (IOException)
        {
            // Best-effort cleanup.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup.
        }
    }
}
