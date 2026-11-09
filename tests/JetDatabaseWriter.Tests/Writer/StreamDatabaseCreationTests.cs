namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks destination admission and ownership throughout stream database creation.</summary>
public sealed class StreamDatabaseCreationTests
{
    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.AceAccdb)]
    public async Task EmptyStreamWithNonzeroPosition_CreatesReadableDatabaseAtStart(DatabaseFormat format)
    {
        await using var stream = new CreationStream { Position = 37 };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
            await writer.InsertRowAsync("T", [42], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Equal(42, Assert.Single(await reader.Rows("T", cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken))[0]);
        Assert.Equal(0, stream.DisposeCalls);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 19)]
    [InlineData(true, 0)]
    [InlineData(true, 19)]
    public async Task NonemptyStream_RefusesWithoutChangingOrOwningDestination(bool leaveOpen, int position)
    {
        await using var stream = new CreationStream();
        stream.WriteByte(0xA5);
        stream.Position = position;
        byte[] original = stream.ToArray();
        int writes = stream.WriteCalls;

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, leaveOpen: leaveOpen, cancellationToken: TestContext.Current.CancellationToken).AsTask());

        Assert.Equal("stream", exception.ParamName);
        Assert.Equal(original, stream.ToArray());
        Assert.Equal(position, stream.Position);
        Assert.Equal(writes, stream.WriteCalls);
        Assert.Equal(0, stream.DisposeCalls);
    }

    [Theory]
    [InlineData("write", false)]
    [InlineData("write", true)]
    [InlineData("flush", false)]
    [InlineData("flush", true)]
    [InlineData("open", false)]
    [InlineData("open", true)]
    [InlineData("initialize", false)]
    [InlineData("initialize", true)]
    public async Task CreationFailure_DisposesOnlyAnOwnedStreamExactlyOnce(string phase, bool leaveOpen)
    {
        await using var stream = new CreationStream { FailurePhase = phase };

        IOException exception = await Assert.ThrowsAsync<IOException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen, TestContext.Current.CancellationToken).AsTask());

        Assert.Same(stream.InjectedFailure, exception);
        Assert.True(stream.Faulted);
        Assert.Equal(leaveOpen ? 0 : 1, stream.DisposeCalls);
        Assert.Equal(leaveOpen, stream.CanRead);
        if (leaveOpen)
        {
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }
    }

    [Theory]
    [InlineData("write", false)]
    [InlineData("write", true)]
    [InlineData("flush", false)]
    [InlineData("flush", true)]
    [InlineData("open", false)]
    [InlineData("open", true)]
    [InlineData("initialize", false)]
    [InlineData("initialize", true)]
    public async Task CancellationAfterCreationStarts_DisposesOnlyAnOwnedStreamExactlyOnce(string phase, bool leaveOpen)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        await using var stream = new CreationStream { FailurePhase = phase, Cancellation = cancellation };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen, cancellation.Token).AsTask());

        Assert.True(stream.Faulted);
        Assert.Equal(leaveOpen ? 0 : 1, stream.DisposeCalls);
        Assert.Equal(leaveOpen, stream.CanRead);
        if (leaveOpen)
        {
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.Jet3Mdb, false)]
    [InlineData(DatabaseFormat.Jet3Mdb, true)]
    [InlineData(DatabaseFormat.Jet4Mdb, false)]
    [InlineData(DatabaseFormat.Jet4Mdb, true)]
    [InlineData(DatabaseFormat.AceAccdb, false)]
    [InlineData(DatabaseFormat.AceAccdb, true)]
    public async Task CreateDatabase_StreamFaultSweep_RestoresEmptyStreamAndRetriesCleanly(DatabaseFormat format, bool encrypted)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var options = new AccessWriterOptions(encrypted ? "StreamCreate123" : null)
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            PageCacheSize = 0,
        };

        int writes;
        int reads;
        await using (var baseline = new WriteFaultStream())
        {
            await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(baseline, format, options, leaveOpen: true, ct);
            writes = baseline.WriteCount;
            reads = baseline.ReadCount;
        }

        Assert.True(writes >= 2);
        Assert.True(reads >= 2);

        await using var stream = new WriteFaultStream();
        for (int point = 1; point <= writes; point++)
        {
            stream.FailOnWrite(point);
            await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct));
            Assert.True(stream.Faulted);
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);

            stream.FailDuringWrite(point);
            await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct));
            Assert.True(stream.Faulted);
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }

        for (int flush = 1; flush <= 2; flush++)
        {
            stream.FailOnFlush(flush);
            await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct));
            Assert.True(stream.Faulted);
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }

        for (int point = 1; point <= reads; point++)
        {
            stream.FailOnRead(point);
            await Assert.ThrowsAsync<IOException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct));
            Assert.True(stream.Faulted);
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);

            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
            stream.CancelAfterReads(point, cancellation);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, cancellation.Token));
            Assert.True(stream.Faulted);
            Assert.Equal(0, stream.Length);
            Assert.Equal(0, stream.Position);
        }

        // When both initialization and SetLength(0) fail, both exceptions are reported.
        stream.FailOnWrite(1);
        stream.FailOnTruncate(afterTruncate: false);
        AggregateException aggregate = await Assert.ThrowsAsync<AggregateException>(async () => await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct));
        Assert.Equal(2, aggregate.InnerExceptions.Count);
        Assert.All(aggregate.InnerExceptions, inner => Assert.IsType<IOException>(inner));
        stream.SetLength(0);
        stream.Position = 0;

        // Without manual stream reset after the restored faults above, retry creation and run DML + DDL.
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, format, options, leaveOpen: true, ct))
        {
            await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int)) { IsAutoIncrement = true, IsPrimaryKey = true }, new ColumnDefinition("Label", typeof(string), 50)], ct);
            _ = await writer.InsertRowsAsync("Items", [[DBNull.Value, "alpha"], [DBNull.Value, "beta"], [DBNull.Value, "gamma"]], ct);
            _ = await writer.UpdateRowsAsync("Items", RowCriteria.Where("Id", 2), new RowValues { ["Label"] = "beta-updated" }, ct);
            _ = await writer.DeleteRowsAsync("Items", RowCriteria.Where("Id", 1), ct);
            await writer.AddColumnAsync("Items", new ColumnDefinition("Extra", typeof(int)) { DefaultValue = 9 }, ct);
            await writer.InsertRowAsync("Items", [DBNull.Value, "delta", DbDefault.Value], ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions(encrypted ? "StreamCreate123" : null) { UseLockFile = false }, leaveOpen: true, ct);
        string[] rows = [.. (await reader.Rows("Items", cancellationToken: ct).ToArrayAsync(ct)).Select(r => $"{r[0]}|{r[1]}|{r[2]}")];
        Assert.Equal(["2|beta-updated|", "3|gamma|", "4|delta|9"], rows);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SuccessfulCreation_DisposesOnlyAnOwnedStreamExactlyOnce(bool leaveOpen)
    {
        await using var stream = new CreationStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { UseLockFile = false }, leaveOpen, TestContext.Current.CancellationToken))
        {
            Assert.Equal(0, stream.DisposeCalls);
        }

        Assert.Equal(leaveOpen ? 0 : 1, stream.DisposeCalls);
        Assert.Equal(leaveOpen, stream.CanRead);
    }

    [Fact]
    public async Task InvalidOptions_DoNotTransferOwnership()
    {
        await using var stream = new CreationStream { Position = 37 };

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, new AccessWriterOptions { MaxTransactionPageBudget = 0 }, cancellationToken: TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(37, stream.Position);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.WriteCalls);
        Assert.Equal(0, stream.DisposeCalls);
    }

    [Theory]
    [InlineData(nameof(AccessOptions.MaxTableDefinitionBytes), 0, false)]
    [InlineData(nameof(AccessOptions.MaxTableDefinitionBytes), -1, false)]
    [InlineData(nameof(AccessOptions.MaxEncryptionSpinCount), -1, false)]
    [InlineData(nameof(AccessOptions.MaxEncryptionSpinCount), 10_000_001, false)]
    [InlineData(nameof(AccessOptions.MaxEncryptionInfoBytes), 0, false)]
    [InlineData(nameof(AccessOptions.MaxEncryptionInfoBytes), -1, false)]
    [InlineData(nameof(AccessOptions.LockTimeoutMilliseconds), -1, false)]
    [InlineData(nameof(AccessOptions.LockTimeoutMilliseconds), -1, true)]
    [InlineData(nameof(AccessWriterOptions.MaxSchemaRewriteRows), 0, false)]
    [InlineData(nameof(AccessWriterOptions.MaxSchemaRewriteRows), -1, false)]
    [InlineData(nameof(AccessWriterOptions.MaxSchemaRewriteBytes), 0, false)]
    [InlineData(nameof(AccessWriterOptions.MaxSchemaRewriteBytes), -1, false)]
    public async Task InvalidOptionDomains_RefuseBeforeStreamAccess(string optionName, int value, bool useByteRangeLocks)
    {
        AccessWriterOptions options = optionName switch
        {
            nameof(AccessOptions.MaxTableDefinitionBytes) => new() { MaxTableDefinitionBytes = value },
            nameof(AccessOptions.MaxEncryptionSpinCount) => new() { MaxEncryptionSpinCount = value },
            nameof(AccessOptions.MaxEncryptionInfoBytes) => new() { MaxEncryptionInfoBytes = value },
            nameof(AccessOptions.LockTimeoutMilliseconds) => new() { LockTimeoutMilliseconds = value, UseByteRangeLocks = useByteRangeLocks },
            nameof(AccessWriterOptions.MaxSchemaRewriteRows) => new() { MaxSchemaRewriteRows = value },
            nameof(AccessWriterOptions.MaxSchemaRewriteBytes) => new() { MaxSchemaRewriteBytes = value },
            _ => throw new ArgumentOutOfRangeException(nameof(optionName)),
        };

        string path = Path.Combine(Path.GetTempPath(), $"InvalidCreation_{Guid.NewGuid():N}.mdb");
        using (FileStreamFactory.OpenTracker opens = FileStreamFactory.TrackOpens())
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.Jet4Mdb, options, TestContext.Current.CancellationToken).AsTask());
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => AccessWriter.OpenAsync(path, options, TestContext.Current.CancellationToken).AsTask());
            Assert.Empty(opens.OpenedPaths);
        }

        Assert.False(File.Exists(path));
        foreach (bool create in (bool[])[false, true])
        {
            foreach (bool leaveOpen in (bool[])[false, true])
            {
                await using var stream = new CreationStream { Position = 37 };
                ArgumentOutOfRangeException exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                {
                    await using AccessWriter writer = create
                        ? await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, options, leaveOpen, TestContext.Current.CancellationToken)
                        : await AccessWriter.OpenAsync(stream, options, leaveOpen, TestContext.Current.CancellationToken);
                });

                Assert.Equal("options", exception.ParamName);
                object expectedActual = optionName == nameof(AccessWriterOptions.MaxSchemaRewriteBytes) ? (object)(long)value : value;
                Assert.Equal(expectedActual, exception.ActualValue);
                Assert.Contains(optionName, exception.Message, StringComparison.Ordinal);
                Assert.Equal(37, stream.Position);
                Assert.Equal(0, stream.Length);
                Assert.Equal(0, stream.ReadCalls);
                Assert.Equal(0, stream.WriteCalls);
                Assert.Equal(0, stream.DisposeCalls);
            }
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(10_000_000)]
    public async Task ValidBoundaryBudgets_AllowUnencryptedCreation(int spinCount)
    {
        await using var stream = new CreationStream();
        var options = new AccessWriterOptions
        {
            UseLockFile = false,
            UseByteRangeLocks = false,
            LockTimeoutMilliseconds = 0,
            MaxEncryptionInfoBytes = 1,
            MaxEncryptionSpinCount = spinCount,
        };
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, options, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync("T", [new ColumnDefinition("Id", typeof(int))], TestContext.Current.CancellationToken);
        }

        await using AccessReader reader = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
        Assert.Equal("T", Assert.Single(await reader.ListTablesAsync(TestContext.Current.CancellationToken)));
    }

    [Fact]
    public async Task CancellationBeforeCreation_DoesNotTransferOwnership()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await using var stream = new CreationStream { Position = 37 };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, cancellationToken: cancellation.Token).AsTask());

        Assert.Equal(37, stream.Position);
        Assert.Equal(0, stream.Length);
        Assert.Equal(0, stream.WriteCalls);
        Assert.Equal(0, stream.DisposeCalls);
    }

    [Theory]
    [InlineData(".jdw-journal")]
    [InlineData(".jdw-journal.preparing")]
    [InlineData(".jdw-owner")]
    [InlineData(".jdw-replacement")]
    public async Task FileRecoveryEvidence_RefusesBeforeChangingOrOwningDestination(string suffix)
    {
        string path = Path.Combine(Path.GetTempPath(), $"Creation_{Guid.NewGuid():N}.mdb");
        string evidence = path + suffix;
        byte[] originalEvidence = [0xA5];
        try
        {
            await File.WriteAllBytesAsync(evidence, originalEvidence, TestContext.Current.CancellationToken);
            await using var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);
            stream.Position = 37;

            JetOperationException exception = await Assert.ThrowsAsync<JetOperationException>(() => AccessWriter.CreateDatabaseAsync(stream, DatabaseFormat.Jet4Mdb, cancellationToken: TestContext.Current.CancellationToken).AsTask());

            Assert.Equal(JetErrorCode.HotJournalConflict, exception.ErrorCode);
            Assert.Equal(0, stream.Length);
            Assert.Equal(37, stream.Position);
            Assert.True(stream.CanWrite);
            Assert.Equal(originalEvidence, await File.ReadAllBytesAsync(evidence, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(path);
            File.Delete(evidence);
        }
    }

    private sealed class CreationStream : MemoryStream
    {
        internal string? FailurePhase { get; init; }

        internal CancellationTokenSource? Cancellation { get; init; }

        internal IOException InjectedFailure { get; } = new("Injected stream creation failure.");

        internal bool Faulted { get; private set; }

        internal int WriteCalls { get; private set; }

        internal int ReadCalls { get; private set; }

        internal int DisposeCalls { get; private set; }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.WriteCalls++;
            bool fail = (this.FailurePhase == "write" && this.WriteCalls == 1)
                || (this.FailurePhase == "initialize" && this.WriteCalls == 2);
            if (fail && this.Cancellation is null)
            {
                base.Write(buffer, offset, count / 2);
                this.Interrupt();
            }

            base.Write(buffer, offset, count);
            if (fail)
            {
                this.Interrupt();
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int read = base.Read(buffer, offset, count);
            this.ReadCalls++;
            if (this.ReadCalls == 1 && this.FailurePhase == "open")
            {
                this.Interrupt();
            }

            return read;
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            if (this.FailurePhase == "flush" && !this.Faulted)
            {
                this.Interrupt();
            }

            return base.FlushAsync(cancellationToken);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.DisposeCalls++;
            }

            base.Dispose(disposing);
        }

        private void Interrupt()
        {
            this.Faulted = true;
            if (this.Cancellation is not null)
            {
                this.Cancellation.Cancel();
                this.Cancellation.Token.ThrowIfCancellationRequested();
            }

            throw this.InjectedFailure;
        }
    }
}
