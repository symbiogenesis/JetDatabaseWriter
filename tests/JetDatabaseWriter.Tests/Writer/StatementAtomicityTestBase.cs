namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using JetDatabaseWriter.Tests.Relationships;
using Xunit;

/// <summary>Shares complete statement fault sweeps and isolated database seeds across format partitions.</summary>
/// <param name="db">Caches Access-authored fixtures with relationship catalogs.</param>
/// <param name="generated">Caches generated statement fault seeds.</param>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Public xUnit test classes require a public base type.")]
public abstract class StatementAtomicityTestBase(DatabaseCache db, GeneratedDatabaseCache generated) : IClassFixture<DatabaseCache>, IClassFixture<GeneratedDatabaseCache>
{
    /// <summary>Bounds each theory row while retaining every fault position across the shards.</summary>
    private const int FaultShardCount = 8;

    private protected static readonly string UpdatedNote = new('c', 36000);

    private static readonly string InsertedNote = new('b', 35000);

    private protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Gets every mutation and fault shard within one database format.</summary>
    public static TheoryData<string, int> Cases
    {
        get
        {
            var cases = new TheoryData<string, int>();
            foreach (string statement in new[] { "Insert", "Update", "Delete", "CascadeDelete", "AddColumn", "CreateTable" })
            {
                for (int shard = 0; shard < FaultShardCount; shard++)
                {
                    cases.Add(statement, shard);
                }
            }

            return cases;
        }
    }

    private protected static async Task AssertFaultAsync(WriteFaultStream stream, byte[] original, byte[] completed, string statement, Action<WriteFaultStream> arm, string? password = null)
    {
        // Reuse the expandable buffer, but restore every byte and reopen the writer for each fault.
        stream.Position = 0;
        stream.SetLength(0);
        await stream.WriteAsync(original, Ct);
        stream.Position = 0;
        await using AccessWriter writer = await OpenAsync(stream, password);
        arm(stream);
        bool failed = false;
        try
        {
            await MutateAsync(writer, statement);
        }
        catch (Exception exception) when (stream.Faulted && exception is IOException or InvalidDataException)
        {
            failed = true;
        }

        Assert.True(stream.Faulted);
        if (failed)
        {
            Assert.Equal<byte>(original.AsSpan(), stream.GetBuffer().AsSpan(0, checked((int)stream.Length)));

            // Retry exercises restored catalog entries, allocation hints, index roots and constraints.
            await MutateAsync(writer, statement);
        }

        if (statement is "AddColumn" or "CreateTable")
        {
            await AssertCompletedAsync(stream, statement, password);
        }
        else
        {
            // Byte equality implies the reader assertions already checked on the baseline.
            Assert.Equal<byte>(completed.AsSpan(), stream.GetBuffer().AsSpan(0, checked((int)stream.Length)));
        }
    }

    private protected static ValueTask<AccessWriter> OpenAsync(WriteFaultStream stream, string? password = null)
    {
        stream.Position = 0;
        return AccessWriter.OpenAsync(
            stream,
            new AccessWriterOptions { Password = password.AsMemory(), UseLockFile = false, PageCacheSize = 0, SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages },
            leaveOpen: true,
            cancellationToken: Ct);
    }

    private protected static WriteFaultStream Copy(byte[] image)
    {
        var stream = new WriteFaultStream();
        stream.Write(image, 0, image.Length);
        stream.Position = 0;
        return stream;
    }

    private protected static async ValueTask MutateAsync(AccessWriter writer, string statement)
    {
        switch (statement)
        {
            case "Insert":
                _ = await writer.InsertRowsAsync("T", Enumerable.Range(100, 8).Select(static id => new object?[] { id, InsertedNote }), Ct);
                break;
            case "Update":
                _ = await writer.UpdateRowsAsync("T", RowCriteria.All(), new RowValues { ["Note"] = UpdatedNote }, Ct);
                break;
            case "Delete":
                _ = await writer.DeleteRowsAsync("T", RowCriteria.All(), Ct);
                break;
            case "CascadeDelete":
                _ = await writer.DeleteRowsAsync("P", RowCriteria.All(), Ct);
                break;
            case "AddColumn":
                await writer.AddColumnAsync("T", new ColumnDefinition("Extra", typeof(int)), Ct);
                break;
            case "CreateTable":
                await writer.CreateTableAsync("NewTable", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(statement));
        }
    }

    private protected static async Task AssertCompletedAsync(MemoryStream stream, string statement, string? password)
    {
        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { Password = password.AsMemory(), UseLockFile = false }, leaveOpen: true, cancellationToken: Ct);
        long expected = statement switch { "Insert" => 16, "Delete" => 0, _ => 8 };
        Assert.Equal(expected, await reader.GetRealRowCountAsync("T", Ct));
        if (statement == "CascadeDelete")
        {
            Assert.Equal(0, await reader.GetRealRowCountAsync("P", Ct));
            Assert.Equal(0, await reader.GetRealRowCountAsync("C", Ct));
        }

        if (statement == "AddColumn")
        {
            Assert.Contains(await reader.GetColumnMetadataAsync("T", Ct), column => column.Name == "Extra");
        }

        if (statement == "CreateTable")
        {
            Assert.Contains("NewTable", await reader.ListTablesAsync(Ct));
        }
    }

    /// <summary>Every physical write, partial write, flush and read either completes or restores the statement.</summary>
    /// <param name="format">The database format.</param>
    /// <param name="statement">The mutation.</param>
    /// <param name="shard">The independent subset of fault positions.</param>
    /// <returns>The test completion.</returns>
    private protected async Task AssertEveryFaultAsync(DatabaseFormat format, string statement, int shard)
    {
        byte[] original = await this.CreateSourceAsync(format);
        await using WriteFaultStream baseline = Copy(original);
        int writes;
        int reads;
        await using (AccessWriter writer = await OpenAsync(baseline))
        {
            int initialWrites = baseline.WriteCount;
            int initialReads = baseline.ReadCount;
            await MutateAsync(writer, statement);
            writes = baseline.WriteCount - initialWrites;
            reads = baseline.ReadCount - initialReads;
        }

        Assert.True(writes > 0);
        byte[] completed = baseline.ToArray();
        await AssertCompletedAsync(baseline, statement, password: null);
        await using var faultStream = new WriteFaultStream();
        for (int point = shard + 1; point <= writes; point += FaultShardCount)
        {
            await AssertFaultAsync(faultStream, original, completed, statement, stream => stream.FailOnWrite(point));
            await AssertFaultAsync(faultStream, original, completed, statement, stream => stream.FailDuringWrite(point));
        }

        if (shard == 0)
        {
            await AssertFaultAsync(faultStream, original, completed, statement, stream => stream.FailOnFlush(1));
        }

        for (int point = shard + 1; point <= reads; point += FaultShardCount)
        {
            await AssertFaultAsync(faultStream, original, completed, statement, stream => stream.FailOnRead(point));
        }
    }

    private protected async Task<byte[]> CreateSourceAsync(DatabaseFormat format) =>
        await generated.CopyToArrayAsync(format.ToString(), () => this.CreateSeedAsync(format));

    private async Task<byte[]> CreateSeedAsync(DatabaseFormat format)
    {
        await using MemoryStream stream = await ForeignKeyTestDatabase.CreateEmptyAsync(db, format);
        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: Ct))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Note", typeof(string))], Ct);
            string note = new('a', 35000);
            _ = await writer.InsertRowsAsync("T", Enumerable.Range(1, 8).Select(id => new object?[] { id, note }), Ct);
            await writer.CreateTableAsync("P", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }], Ct);
            await writer.CreateTableAsync("C", [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("ParentId", typeof(int)), new ColumnDefinition("Note", typeof(string))], Ct);
            await writer.InsertRowAsync("P", [1], Ct);
            _ = await writer.InsertRowsAsync("C", Enumerable.Range(1, 8).Select(id => new object?[] { id, 1, note }), Ct);
            await writer.CreateRelationshipAsync(new RelationshipDefinition("FK_C_P", "P", "Id", "C", "ParentId") { CascadeDeletes = true }, Ct);
        }

        return stream.ToArray();
    }
}
