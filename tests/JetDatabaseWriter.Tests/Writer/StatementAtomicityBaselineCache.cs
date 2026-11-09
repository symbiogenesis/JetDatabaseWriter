namespace JetDatabaseWriter.Tests.Writer;

using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;

/// <summary>Caches one immutable successful statement baseline per format and mutation within a test class.</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit IClassFixture<T> requires public accessibility")]
public sealed class StatementAtomicityBaselineCache
{
    private readonly ConcurrentDictionary<(DatabaseFormat Format, string Statement), Lazy<Task<StatementAtomicityBaseline>>> baselines = new();

    internal async ValueTask<StatementAtomicityBaseline> GetAsync(DatabaseFormat format, string statement, Func<Task<StatementAtomicityBaseline>> createBaseline)
        => await this.baselines.GetOrAdd((format, statement), static (_, factory) => new Lazy<Task<StatementAtomicityBaseline>>(factory), createBaseline).Value;
}
