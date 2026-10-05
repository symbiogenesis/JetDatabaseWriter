namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using Xunit;

/// <summary>
/// The write-mode matrix: the three ways a test drives the writer
/// (<see cref="WriteMode"/>), the writer options and the transaction scope
/// each one needs, and the theory data that crosses them with database
/// formats. Tests whose behaviour may differ between a write that goes
/// straight to the file, one that <see cref="AccessWriterOptions.UseTransactionalWrites"/>
/// wraps, and one inside an explicit transaction run every case.
/// </summary>
internal static class WriteModes
{
    /// <summary>Returns every pair of <paramref name="formats"/> and <see cref="WriteMode"/>, formats outermost.</summary>
    /// <param name="formats">The database formats.</param>
    /// <returns>The format and write-mode pairs.</returns>
    public static TheoryData<DatabaseFormat, WriteMode> Combine(params DatabaseFormat[] formats)
    {
        var data = new TheoryData<DatabaseFormat, WriteMode>();
        foreach (DatabaseFormat format in formats)
        {
            foreach (WriteMode mode in Enum.GetValues<WriteMode>())
            {
                data.Add(format, mode);
            }
        }

        return data;
    }

    /// <summary>
    /// Returns writer options for <paramref name="mode"/>: no lock file and no
    /// byte-range locks, and <see cref="AccessWriterOptions.UseTransactionalWrites"/>
    /// for <see cref="WriteMode.AutoCommit"/>.
    /// </summary>
    /// <param name="mode">The write mode.</param>
    /// <returns>The options.</returns>
    public static AccessWriterOptions WriterOptions(WriteMode mode) => new()
    {
        UseLockFile = false,
        UseByteRangeLocks = false,
        UseTransactionalWrites = mode == WriteMode.AutoCommit,
    };

    /// <summary>
    /// Runs <paramref name="work"/> directly, or for <see cref="WriteMode.ExplicitCommit"/>
    /// inside one explicit transaction that is committed after it.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="mode">The write mode.</param>
    /// <param name="work">The writes.</param>
    /// <param name="cancellationToken">A token used to cancel the transaction.</param>
    /// <returns>A task that completes when the work, and its commit, have run.</returns>
    public static async Task RunAsync(AccessWriter writer, WriteMode mode, Func<Task> work, CancellationToken cancellationToken)
    {
        if (mode != WriteMode.ExplicitCommit)
        {
            await work();
            return;
        }

        await using JetTransaction tx = await writer.BeginTransactionAsync(cancellationToken);
        await work();
        await tx.CommitAsync(cancellationToken);
    }
}

/// <summary>How a test drives the writer.</summary>
[SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "Theory parameters of public xUnit test methods must be public.")]
public enum WriteMode
{
    /// <summary>Default atomic statements, with no device flush requested.</summary>
    Direct = 0,

    /// <summary><see cref="AccessWriterOptions.UseTransactionalWrites"/> requests a durable flush for each atomic statement.</summary>
    AutoCommit = 1,

    /// <summary>The mutations run inside one explicit transaction that is committed.</summary>
    ExplicitCommit = 2,
}
