namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Diagnostics;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Xunit;

public sealed class JetQueryDiagnosticsTests
{
    [Fact]
    public async Task QueryPaths_AreVisibleThroughEfLogTo()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = CreateOptions(stream, messages);
        await using DiagnosticContext context = new(options);
        await SeedAsync(context, ct);
        _ = await context.Set<DiagnosticRow>().Where(x => x.Id == 1).Select(x => x.Name).ToListAsync(ct);
        _ = await context.Set<DiagnosticRow>().OrderBy(x => x.Id).Select(x => x.Name).ToListAsync(ct);
        _ = await context.Set<DiagnosticRow>().Where(x => x.Name == "secret-diagnostic-value").Select(x => x.Id).ToListAsync(ct);
        _ = await context.Set<DiagnosticRow>().OrderBy(x => x.Name).Select(x => x.Id).ToListAsync(ct);
        _ = await context.Set<OwnedRow>().AsNoTracking().ToListAsync(ct);
        string text = string.Join(Environment.NewLine, messages);
        Assert.Contains("IndexSeek", text, StringComparison.Ordinal);
        Assert.Contains("IndexOrder", text, StringComparison.Ordinal);
        Assert.Contains("Scan", text, StringComparison.Ordinal);
        Assert.Contains("Sort", text, StringComparison.Ordinal);
        Assert.Contains("MaterializedEntities", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ProviderEvents_DoNotIncludeSensitiveQueryValues(bool sensitiveLogging)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = CreateOptions(stream, messages, sensitiveLogging);
        await using DiagnosticContext context = new(options);
        await SeedAsync(context, ct);
        string secret = string.Concat("secret-", "diagnostic-value");
        _ = await context.Set<DiagnosticRow>().Where(x => x.Name == secret).Select(x => x.Id).ToListAsync(ct);
        Assert.NotEmpty(messages);
        Assert.All(messages, message => Assert.DoesNotContain(secret, message, StringComparison.Ordinal));
    }

    [Fact]
    public async Task DiagnosticListener_ReceivesStructuredPathAndRowMeasurements()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        await using DiagnosticContext context = new(CreateOptions(stream, messages));
        await SeedAsync(context, ct);
        using DiagnosticCapture capture = new(context);
        _ = await context.Set<DiagnosticRow>().Where(x => x.Id == 1).Select(x => x.Name).ToListAsync(ct);
        JetQueryEventData access = Assert.Single(capture.Events, data => data.EventId == JetEventId.TableRead);
        Assert.Equal("IndexSeek", access.ExecutionPath);
        Assert.Equal("AdmittedPredicateIndex", access.Reason);
        Assert.Equal("DiagnosticRows", access.TableName);
        JetQueryEventData completion = Assert.Single(capture.Events, data => data.EventId == JetEventId.TableReadCompleted);
        Assert.Equal(1, completion.RowsRead);
        Assert.Equal(1, completion.RowsReturned);
        Assert.True(completion.Completed);
        Assert.True(completion.Duration >= TimeSpan.Zero);
        Assert.All(capture.Events, data => Assert.Same(context, data.Context));
        Assert.All(capture.Events, data => Assert.DoesNotContain("secret-diagnostic-value", data.ToString(), StringComparison.Ordinal));
    }

    [Fact]
    public async Task DiagnosticListener_ReportsEarlyDisposalWithoutClaimingCompletion()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        await using DiagnosticContext context = new(CreateOptions(stream, messages));
        await SeedAsync(context, ct);
        using DiagnosticCapture capture = new(context);
        IAsyncEnumerator<int> enumerator = context.Set<DiagnosticRow>().Select(x => x.Id).AsAsyncEnumerable().GetAsyncEnumerator(ct);
        try
        {
            Assert.True(await enumerator.MoveNextAsync());
        }
        finally
        {
            await enumerator.DisposeAsync();
        }

        JetQueryEventData completion = Assert.Single(capture.Events, data => data.EventId == JetEventId.TableReadCompleted);
        Assert.Equal(1, completion.RowsRead);
        Assert.Equal(1, completion.RowsReturned);
        Assert.False(completion.Completed);
    }

    [Fact]
    public async Task ConfigureWarnings_IgnoreSuppressesProviderLogging()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = new DbContextOptionsBuilder<DiagnosticContext>(CreateOptions(stream, messages))
            .ConfigureWarnings(warnings => warnings.Ignore(JetEventId.QueryExecution, JetEventId.TableRead, JetEventId.TableReadCompleted)).Options;
        await using DiagnosticContext context = new(options);
        await SeedAsync(context, ct);
        _ = await context.Set<DiagnosticRow>().Select(x => x.Id).ToListAsync(ct);
        Assert.Empty(messages);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SortFallback_MeasuresReadBuffersAndActualYields(bool enumerateAll)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        await using DiagnosticContext context = new(CreateOptions(stream, messages));
        await SeedAsync(context, ct);
        using DiagnosticCapture capture = new(context);
        IQueryable<int> query = context.Set<DiagnosticRow>().OrderBy(row => row.Name).Select(row => row.Id);
        int[] actual = await (enumerateAll ? query : query.Take(1)).ToArrayAsync(ct);
        Assert.Equal(enumerateAll ? 2 : 1, actual.Length);
        Assert.Equal(2, actual[0]);
        JetQueryEventData completion = Assert.Single(capture.Events, data => data.EventId == JetEventId.TableReadCompleted);
        Assert.Equal("ScanSort", completion.ExecutionPath);
        Assert.Equal(2, completion.RowsRead);
        Assert.Equal(actual.Length, completion.RowsReturned);
        Assert.Equal(enumerateAll, completion.Completed);
    }

    [Fact]
    public async Task ConfigureWarnings_ThrowAppliesToProviderEvents()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = new DbContextOptionsBuilder<DiagnosticContext>(CreateOptions(stream, messages))
            .ConfigureWarnings(warnings => warnings.Throw(JetEventId.TableRead)).Options;
        await using DiagnosticContext context = new(options);
        await SeedAsync(context, ct);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.Set<DiagnosticRow>().Select(x => x.Id).ToListAsync(ct));
        Assert.Contains("TableRead", exception.Message, StringComparison.Ordinal);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(timeout.Token);
        await transaction.RollbackAsync(timeout.Token);
    }

    [Fact(DisableParallelization = true)]
    public async Task ThrowingDiagnosticPredicate_ReleasesTheConnectionLease()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        await using DiagnosticContext context = new(CreateOptions(stream, messages));
        await SeedAsync(context, ct);
        using DiagnosticCapture capture = new(context, throwOnMeasurements: true);
        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => context.Set<DiagnosticRow>().Select(x => x.Id).ToListAsync(ct));
        Assert.Equal("diagnostic-predicate", exception.Message);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        await using IDbContextTransaction transaction = await context.Database.BeginTransactionAsync(timeout.Token);
        await transaction.RollbackAsync(timeout.Token);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AggregatesAndKeyLoads_ReportActualReaderAndWriterPaths(bool useTransaction)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = CreateOptions(stream, messages);
        await using (DiagnosticContext seed = new(options))
        {
            await SeedAsync(seed, ct);
        }

        await using DiagnosticContext context = new(options);
        await using IDbContextTransaction? transaction = useTransaction ? await context.Database.BeginTransactionAsync(ct) : null;
        using DiagnosticCapture capture = new(context);
        Assert.Equal(1, await context.Set<DiagnosticRow>().CountAsync(x => x.Id == 1, ct));
        Assert.Equal(1, await context.Set<DiagnosticRow>().Where(x => x.Name == "secret-diagnostic-value").SumAsync(x => x.Id, ct));
        Assert.Contains(capture.Events, data => data.EventId == JetEventId.TableRead && data.ExecutionPath == "IndexSeek");
        Assert.Contains(capture.Events, data => data.EventId == JetEventId.TableRead && data.ExecutionPath == "Scan");
        _ = await context.Set<DiagnosticRow>().Where(x => x.Id == 1)
            .Join(context.Set<DiagnosticRow>(), outer => outer.Id, inner => inner.Id, (_, inner) => inner.Name).ToListAsync(ct);
        Assert.Contains(capture.Events, data => data.EventId == JetEventId.TableRead && data.Reason == "AdmittedKeyIndex");
        _ = await context.Set<DiagnosticRow>().Where(x => x.Id == 1)
            .Join(context.Set<DiagnosticRow>(), outer => outer.Name, inner => inner.Name, (_, inner) => inner.Id).ToListAsync(ct);
        Assert.Contains(capture.Events, data => data.EventId == JetEventId.TableRead && data.Reason == "NoAdmissibleKeyIndex");
    }

    [Fact]
    public async Task ProviderDebugEvents_RespectLogLevelFilter()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        ConcurrentQueue<string> messages = new();
        DbContextOptions<DiagnosticContext> options = new DbContextOptionsBuilder<DiagnosticContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .LogTo(messages.Enqueue, static (eventId, level) => eventId.Id >= CoreEventId.ProviderBaseId && level >= LogLevel.Information).Options;
        await using DiagnosticContext context = new(options);
        await SeedAsync(context, ct);
        _ = await context.Set<DiagnosticRow>().Select(x => x.Id).ToListAsync(ct);
        Assert.Empty(messages);
    }

    private static DbContextOptions<DiagnosticContext> CreateOptions(MemoryStream stream, ConcurrentQueue<string> messages, bool sensitiveLogging = false)
        => new DbContextOptionsBuilder<DiagnosticContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .EnableSensitiveDataLogging(sensitiveLogging)
            .LogTo(messages.Enqueue, (eventId, _) => eventId.Id is >= CoreEventId.ProviderBaseId and < CoreEventId.ProviderBaseId + 10)
            .Options;

    private static async Task SeedAsync(DiagnosticContext context, CancellationToken cancellationToken)
    {
        Assert.True(await context.Database.EnsureCreatedAsync(cancellationToken));
        context.AddRange(
            new DiagnosticRow { Id = 1, Name = "secret-diagnostic-value" },
            new DiagnosticRow { Id = 2, Name = "another-value" },
            new OwnedRow { Id = 1, Details = new OwnedDetails { Text = "owned" } });
        Assert.Equal(3, await context.SaveChangesAsync(cancellationToken));
        context.ChangeTracker.Clear();
    }

    private sealed class DiagnosticContext(DbContextOptions<DiagnosticContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<DiagnosticRow>(entity =>
            {
                _ = entity.ToTable("DiagnosticRows");
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.Property(x => x.Name).HasMaxLength(100);
            });
            _ = modelBuilder.Entity<OwnedRow>(entity =>
            {
                _ = entity.ToTable("DiagnosticOwnedRows");
                _ = entity.Property(x => x.Id).ValueGeneratedNever();
                _ = entity.OwnsOne(x => x.Details, owned => owned.Property(x => x.Text).HasMaxLength(100));
            });
        }
    }

    private sealed class DiagnosticCapture : IObserver<DiagnosticListener>, IObserver<KeyValuePair<string, object?>>, IDisposable
    {
        private readonly DbContext context;
        private readonly bool throwOnMeasurements;
        private readonly ConcurrentBag<IDisposable> subscriptions = [];
        private readonly IDisposable allListeners;

        internal DiagnosticCapture(DbContext context, bool throwOnMeasurements = false)
        {
            this.context = context;
            this.throwOnMeasurements = throwOnMeasurements;
            this.allListeners = DiagnosticListener.AllListeners.Subscribe(this);
        }

        internal ConcurrentQueue<JetQueryEventData> Events { get; } = [];

        public void OnNext(DiagnosticListener value)
        {
            if (value.Name == "Microsoft.EntityFrameworkCore")
            {
                this.subscriptions.Add(value.Subscribe(this, name =>
                {
                    if (this.throwOnMeasurements && name == JetEventId.TableReadCompleted.Name)
                    {
                        throw new InvalidOperationException("diagnostic-predicate");
                    }

                    return name.StartsWith("JetDatabaseWriter.Query.", StringComparison.Ordinal);
                }));
            }
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Value is JetQueryEventData data && ReferenceEquals(data.Context, this.context))
            {
                this.Events.Enqueue(data);
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void Dispose()
        {
            this.allListeners.Dispose();
            foreach (IDisposable subscription in this.subscriptions)
            {
                subscription.Dispose();
            }
        }
    }

    private sealed class DiagnosticRow
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class OwnedRow
    {
        public int Id { get; set; }

        public OwnedDetails Details { get; set; } = new();
    }

    private sealed class OwnedDetails
    {
        public string Text { get; set; } = string.Empty;
    }
}
