namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Complex-column publication failures restore bytes and warmed writer state before retry.</summary>
public sealed class TableRewriteFailureTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ComplexRewrite_FaultOrCancellationRestoresOriginalAndCanRetry(bool drop)
    {
        byte[] original = await CreateSeedAsync();
        Snapshot expected = await ReadSnapshotAsync(original, "Files");
        await using WriteFaultStream baseline = Copy(original);
        int writes;
        int reads;
        await using (AccessWriter writer = await OpenAsync(baseline))
        {
            await WarmAsync(writer);
            int initialWrites = baseline.WriteCount;
            int initialReads = baseline.ReadCount;
            await RewriteAsync(writer, drop, Ct);
            writes = baseline.WriteCount - initialWrites;
            reads = baseline.ReadCount - initialReads;
        }

        Assert.True(writes > 0);
        Assert.True(reads > 2);
        for (int point = 1; point <= writes; point++)
        {
            await AssertRecoveryAsync(original, expected, drop, stream => stream.FailOnWrite(point));
            await AssertRecoveryAsync(original, expected, drop, stream => stream.FailDuringWrite(point));
        }

        foreach (int point in new[] { 1, reads / 2, reads - 1 }.Distinct())
        {
            await AssertRecoveryAsync(original, expected, drop, arm: null, cancelAfterRead: point);
        }
    }

    private static async Task AssertRecoveryAsync(byte[] original, Snapshot expected, bool drop, Action<WriteFaultStream>? arm, int cancelAfterRead = 0)
    {
        await using WriteFaultStream stream = Copy(original);
        await using AccessWriter writer = await OpenAsync(stream);
        await WarmAsync(writer);
        using var cancellation = new CancellationTokenSource();
        if (cancelAfterRead > 0)
        {
            stream.CancelAfterReads(cancelAfterRead, cancellation);
        }
        else
        {
            Assert.NotNull(arm);
            arm(stream);
        }

        CancellationToken token = cancelAfterRead > 0 ? cancellation.Token : Ct;
        bool failed = false;
        try
        {
            await RewriteAsync(writer, drop, token);
        }
        catch (Exception exception) when (stream.Faulted && exception is IOException or InvalidDataException or OperationCanceledException)
        {
            failed = true;
        }

        Assert.True(stream.Faulted);
        if (cancelAfterRead > 0)
        {
            Assert.True(cancellation.IsCancellationRequested);
        }

        if (failed)
        {
            Assert.Equal<byte>(original.AsSpan(), stream.GetBuffer().AsSpan(0, checked((int)stream.Length)));
            Assert.Null(writer.Services.Transactions.ActiveTransaction);
            ResolvedTable restored = await writer.Services.Catalog.ResolveRequiredTableAsync("Documents", Ct);
            Assert.Equal(expected.Root, restored.Entry.TDefPage);
            Assert.Equal(["Id", "Files", "KeepFiles", "Payload"], restored.Definition.Columns.Select(column => column.Name));
            AssertSnapshotEqual(expected, await ReadSnapshotAsync(stream.ToArray(), "Files"));

            // Reuse the same writer to exercise restored allocation hints, constraints and complex caches.
            await RewriteAsync(writer, drop, Ct);
        }

        Snapshot completed = await ReadSnapshotAsync(stream.ToArray(), drop ? null : "Renamed");
        Assert.Equal(expected.KeepAttachment, completed.KeepAttachment);
        Assert.Equal(expected.Payload, completed.Payload);
        if (drop)
        {
            Assert.Empty(completed.TargetAttachment);
            Assert.Single(completed.ComplexColumns);
            Assert.DoesNotContain(completed.ComplexColumns, column => column.StartsWith("Files:", StringComparison.Ordinal));
        }
        else
        {
            Assert.Equal(expected.TargetAttachment, completed.TargetAttachment);
            Assert.Equal(2, completed.ComplexColumns.Length);
            Assert.Contains(completed.ComplexColumns, column => column.StartsWith("Renamed:", StringComparison.Ordinal));
        }

        Assert.Equal("Documents", Assert.Single(completed.TableNames));
        await writer.AddAttachmentAsync("Documents", "KeepFiles", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("retry.txt", [21, 34]), Ct);
        Snapshot afterMutation = await ReadSnapshotAsync(stream.ToArray(), drop ? null : "Renamed");
        Assert.Equal(2, afterMutation.KeepAttachment.Length);
    }

    private static async Task WarmAsync(AccessWriter writer)
    {
        writer.Database.TableDefs.VerifyOnHit = true;
        ResolvedTable table = await writer.Services.Catalog.ResolveRequiredTableAsync("Documents", Ct);
        _ = await writer.Services.Catalog.GetSchemaAsync(table.Entry.TDefPage, cancellationToken: Ct);
    }

    private static ValueTask RewriteAsync(AccessWriter writer, bool drop, CancellationToken token)
        => drop ? writer.DropColumnAsync("Documents", "Files", token) : writer.RenameColumnAsync("Documents", "Files", "Renamed", token);

    private static ValueTask<AccessWriter> OpenAsync(WriteFaultStream stream)
    {
        stream.Position = 0;
        return TestDatabase.OpenWriterAsync(stream, new AccessWriterOptions { UseLockFile = false, PageCacheSize = 0 }, cancellationToken: Ct);
    }

    private static WriteFaultStream Copy(byte[] bytes)
    {
        var stream = new WriteFaultStream();
        stream.Write(bytes, 0, bytes.Length);
        stream.Position = 0;
        return stream;
    }

    private static async Task<byte[]> CreateSeedAsync()
    {
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, Ct))
        {
            await writer.CreateTableAsync("Documents", [new("Id", typeof(int)) { IsPrimaryKey = true }, new("Files", typeof(byte[])) { IsAttachment = true }, new("KeepFiles", typeof(byte[])) { IsAttachment = true }, new("Payload", typeof(string))], Ct);
            await writer.InsertRowAsync("Documents", [1, DBNull.Value, DBNull.Value, new string('p', 35000)], Ct);
            await writer.AddAttachmentAsync("Documents", "Files", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("original.txt", [1, 2, 3]), Ct);
            await writer.AddAttachmentAsync("Documents", "KeepFiles", new Dictionary<string, object?> { ["Id"] = 1 }, new AttachmentInput("keep.txt", [5, 8, 13]), Ct);
        }

        return stream.ToArray();
    }

    private static async Task<Snapshot> ReadSnapshotAsync(byte[] bytes, string? target)
    {
        await using var stream = new MemoryStream(bytes, writable: false);
        await using AccessReader reader = await TestDatabase.OpenReaderAsync(stream, cancellationToken: Ct);
        CatalogEntry entry = Assert.IsType<CatalogEntry>(await reader.GetCatalogEntryAsync("Documents", Ct));
        using DataTable objects = await reader.ReadTableAsync("MSysObjects", cancellationToken: Ct);
        DataRow table = Assert.Single(objects.AsEnumerable(), row => Equals(row["Name"], "Documents"));
        string[] catalog = [.. objects.Columns.Cast<DataColumn>().Select(column => column.ColumnName + "=" + Cell(table[column]))];
        using DataTable aces = await reader.ReadTableAsync("MSysACEs", cancellationToken: Ct);
        string[] permissions = [.. aces.AsEnumerable().Where(row => Equals(row["ObjectId"], checked((int)entry.TDefPage)))
            .Select(row => string.Join("|", row.ItemArray.Select(Cell))).Order(StringComparer.Ordinal),];
        Assert.NotEmpty(permissions);
        string[] complex = [.. (await reader.GetComplexColumnsAsync("Documents", Ct))
            .Select(column => $"{column.ColumnName}:{column.ComplexId}:{column.FlatTableId}:{column.ConceptualTableId}").Order(StringComparer.Ordinal),];
        string[] kept = [.. (await reader.GetAttachmentsAsync("Documents", "KeepFiles", Ct)).Select(Attachment).Order(StringComparer.Ordinal)];
        string[] changed = target is null ? [] : [.. (await reader.GetAttachmentsAsync("Documents", target, Ct)).Select(Attachment).Order(StringComparer.Ordinal)];
        using DataTable rows = await reader.ReadTableAsync("Documents", cancellationToken: Ct);
        string payload = Assert.IsType<string>(Assert.Single(rows.AsEnumerable())["Payload"]);
        return new Snapshot(entry.TDefPage, catalog, permissions, complex, kept, changed, payload, [.. await reader.ListTablesAsync(Ct)]);
    }

    private static string Attachment(AttachmentRecord attachment) => attachment.FileName + ":" + Convert.ToHexString(attachment.FileData);

    private static string Cell(object? value) => value switch
    {
        byte[] bytes => Convert.ToHexString(bytes),
        DateTime date => date.Ticks.ToString(CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };

    private static void AssertSnapshotEqual(Snapshot expected, Snapshot actual)
    {
        Assert.Equal(expected.Root, actual.Root);
        Assert.Equal(expected.Catalog, actual.Catalog);
        Assert.Equal(expected.Permissions, actual.Permissions);
        Assert.Equal(expected.ComplexColumns, actual.ComplexColumns);
        Assert.Equal(expected.KeepAttachment, actual.KeepAttachment);
        Assert.Equal(expected.TargetAttachment, actual.TargetAttachment);
        Assert.Equal(expected.Payload, actual.Payload);
        Assert.Equal(expected.TableNames, actual.TableNames);
    }

    private sealed record Snapshot(long Root, string[] Catalog, string[] Permissions, string[] ComplexColumns, string[] KeepAttachment, string[] TargetAttachment, string Payload, string[] TableNames);
}
