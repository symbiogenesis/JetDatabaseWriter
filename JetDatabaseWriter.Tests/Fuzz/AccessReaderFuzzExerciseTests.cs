namespace JetDatabaseWriter.Tests.Fuzz;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks the safety and reachability contracts of the hosted reader exercise.</summary>
public sealed class AccessReaderFuzzExerciseTests
{
    [Fact]
    public async Task IndexedSeed_ReachesSchemaAndSeek_WithBoundedRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: ct))
        {
            await writer.CreateTableAsync(
                "T",
                [new ColumnDefinition("Id", typeof(int))],
                [new IndexDefinition("IX_1", "Id"), new IndexDefinition("IX_2", "Id"), new IndexDefinition("IX_3", "Id"), new IndexDefinition("IX_4", "Id"), new IndexDefinition("IX_5", "Id")],
                ct);
            var rows = new List<object[]>();
            for (int i = 0; i < 16; i++)
            {
                rows.Add([i]);
            }

            await writer.InsertRowsAsync("T", rows, ct);
        }

        var messages = new List<string>();
        await AccessReaderFuzzExercise.RunAsync(stream.ToArray(), messages.Add, ct);

        Assert.Contains("[FuzzCoverage] reader:schema:completed", messages);
        Assert.Contains("[FuzzCoverage] reader:seek:completed", messages);
        Assert.Equal(4, messages.Count(static message => message == "[FuzzCoverage] reader:seek:completed"));
        Assert.Contains("[Fuzzing] rows consumed: 8", messages);
    }

    [Fact]
    public async Task CatalogSeed_VisitsAtMostEightTables()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: true, cancellationToken: ct))
        {
            for (int i = 0; i < 9; i++)
            {
                await writer.CreateTableAsync($"T{i}", [new ColumnDefinition("Id", typeof(int))], ct);
            }
        }

        var messages = new List<string>();
        await AccessReaderFuzzExercise.RunAsync(stream.ToArray(), messages.Add, ct);

        Assert.Equal(8, messages.Count(static message => message == "[FuzzCoverage] reader:schema:completed"));
        Assert.Equal(8, messages.Count(static message => message == "[FuzzCoverage] reader:rows:completed"));
    }

    [Fact]
    public async Task NativeEncryptedSeed_ReachesDecryptedRows()
    {
        byte[] bytes = await File.ReadAllBytesAsync(Path.Combine(TestDatabases.EncryptedRoot, "NativeAceAgile.accdb"), TestContext.Current.CancellationToken);
        var messages = new List<string>();

        await AccessReaderFuzzExercise.RunAsync(bytes, messages.Add, TestContext.Current.CancellationToken);

        Assert.Contains("[FuzzCoverage] reader:encryption-attempt", messages);
        Assert.Contains("[FuzzCoverage] reader:encrypted:completed", messages);
        Assert.Contains("[FuzzCoverage] reader:rows:completed", messages);
    }

    [Fact]
    public async Task LinkedTextSeed_DeniesSourceBeforeOpeningIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string directory = Path.Combine(Path.GetTempPath(), $"JetDatabaseWriter-fuzz-link-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        string path = Path.Combine(directory, "source.csv");
        byte[] original = "Id\r\n7\r\n"u8.ToArray();
        await File.WriteAllBytesAsync(path, original, ct);
        try
        {
            await AssertDeniedAsync();
            Assert.Equal(original, await File.ReadAllBytesAsync(path, ct));
        }
        finally
        {
            File.Delete(path);
            Directory.Delete(directory);
        }

        async Task AssertDeniedAsync()
        {
            await using var stream = new MemoryStream();
            await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.AceAccdb, leaveOpen: true, cancellationToken: ct))
            {
                await writer.CreateLinkedTextTableAsync("Denied", directory, "source.csv", "Text;HDR=YES;FMT=Delimited", ct);
            }

            var messages = new List<string>();
            AccessReaderOptions options = AccessReaderFuzzExercise.CreateOptions([], ReadOnlyMemory<char>.Empty, new EncryptionWorkBudget(300_000), messages.Add);
            stream.Position = 0;
            await using AccessReader reader = await AccessReader.OpenAsync(stream, options, leaveOpen: true, cancellationToken: ct);
            await using var protectedSource = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);

            await Assert.ThrowsAsync<UnauthorizedAccessException>(async () => await reader.GetColumnMetadataAsync("Denied", ct));
            Assert.Contains("[FuzzCoverage] reader:link-denied", messages);

            messages.Clear();
            await AccessReaderFuzzExercise.RunAsync(stream.ToArray(), messages.Add, ct);
            Assert.Contains("[FuzzCoverage] reader:link-denied", messages);
        }
    }

    [Fact]
    public async Task RowLimit_DisposesWithoutReadingAnExtraRow()
    {
        int consumed = 0;
        bool disposed = false;
        object[]? first = await AccessReaderFuzzExercise.ReadBoundedRowsAsync(Rows(), 3, TestContext.Current.CancellationToken);

        Assert.Equal(3, consumed);
        Assert.True(disposed);
        Assert.Equal(0, Assert.IsType<int>(Assert.Single(Assert.IsType<object[]>(first))));

        async IAsyncEnumerable<object[]> Rows()
        {
            await Task.CompletedTask;
            try
            {
                for (int i = 0; i < 10; i++)
                {
                    consumed++;
                    yield return [i];
                }
            }
            finally
            {
                disposed = true;
            }
        }
    }

    [Fact]
    public async Task RowLimit_CancellationEscapes()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await AccessReaderFuzzExercise.ReadBoundedRowsAsync(Rows(), 3, cancellation.Token));

        static async IAsyncEnumerable<object[]> Rows()
        {
            await Task.CompletedTask;
            yield return [1];
        }
    }

    [Fact]
    public void ProgrammingExceptions_AreNotAcceptedAsMalformedInput()
    {
        foreach (Exception exception in (Exception[])[new ArgumentException(), new InvalidOperationException(), new FormatException(), new OverflowException(), new OperationCanceledException()])
        {
            Assert.False(AccessReaderFuzzExercise.IsExpectedInputException(exception));
        }

#pragma warning disable CA2201 // Construct a runtime failure only to verify that the harness refuses to accept it.
        Assert.False(AccessReaderFuzzExercise.IsExpectedInputException(new NullReferenceException()));
#pragma warning restore CA2201

        Assert.True(AccessReaderFuzzExercise.IsExpectedInputException(new JetCorruptDataException()));
        Assert.True(AccessReaderFuzzExercise.IsExpectedInputException(new InvalidDataException()));
        Assert.True(AccessReaderFuzzExercise.IsExpectedInputException(new JetLimitationException()));
    }
}
