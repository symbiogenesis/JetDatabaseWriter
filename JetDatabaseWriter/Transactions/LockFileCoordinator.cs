namespace JetDatabaseWriter.Transactions;

using System;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using JetDatabaseWriter.Infrastructure;

/// <summary>
/// Bundles the configuration and runtime state required to maintain a JET
/// lock-file (<c>.ldb</c> / <c>.laccdb</c>) slot for the lifetime of an
/// <see cref="AccessReader"/> or <see cref="AccessWriter"/>.
/// </summary>
/// <remarks>
/// The coordinator is a no-op when <see cref="IsEnabled"/> is <c>false</c>
/// (e.g. for stream-only opens with no backing path, or when the caller
/// disabled lock-file maintenance via options). This consolidates the four
/// or five lock-file fields that previously lived directly on the reader
/// and writer into a single composed object.
/// </remarks>
/// <remarks>
/// Initializes a new instance of the <see cref="LockFileCoordinator"/> class.
/// </remarks>
/// <param name="databasePath">Path to the database whose sibling lock-file should be maintained. Empty disables the coordinator.</param>
/// <param name="ownerTypeName">Display name of the owning type (e.g. <c>nameof(AccessReader)</c>); used in diagnostics.</param>
/// <param name="settings">Lock-file behaviour switches and identity strings. See <see cref="LockFileSettings"/>.</param>
internal sealed class LockFileCoordinator(string databasePath, string ownerTypeName, LockFileSettings settings) : IDisposable
{
    private LockFileSlotWriter? slot;

    /// <summary>Creates a coordinator wired up from <see cref="AccessReaderOptions"/>.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="options">The options.</param>
    public static LockFileCoordinator ForReader(string databasePath, AccessReaderOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new LockFileCoordinator(
            databasePath,
            nameof(AccessReader),
            options.CreateLockFileSettings(respectExisting: false));
    }

    /// <summary>Creates a coordinator wired up from <see cref="AccessWriterOptions"/>.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="options">The options.</param>
    public static LockFileCoordinator ForWriter(string databasePath, AccessWriterOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new LockFileCoordinator(
            databasePath,
            nameof(AccessWriter),
            options.CreateLockFileSettings(options.RespectExistingLockFile));
    }

    /// <summary>
    /// Creates a coordinator suitable for the writer's static re-encryption helpers,
    /// where <paramref name="options"/> may be <see langword="null"/> and the defaults
    /// honour any existing lock-file.
    /// </summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="options">The options.</param>
    public static LockFileCoordinator ForReencrypt(string databasePath, AccessWriterOptions? options)
    {
        LockFileSettings settings = options is null
            ? AccessOptions.CreateDefaultLockFileSettings(respectExisting: true)
            : options.CreateLockFileSettings(options.RespectExistingLockFile);

        return new LockFileCoordinator(databasePath, nameof(AccessWriter), settings);
    }

    /// <summary>Gets a value indicating whether the coordinator will maintain a lock-file slot.</summary>
    public bool IsEnabled { get; } = settings.Enabled && !string.IsNullOrEmpty(databasePath);

    /// <summary>
    /// Claims a slot in the sibling lock-file. No-op when <see cref="IsEnabled"/> is
    /// <c>false</c> or a slot is already held. Use together with <c>using</c> /
    /// <c>try-finally</c> for scoped, RAII-style ownership.
    /// </summary>
    public void Acquire()
    {
        if (!this.IsEnabled || this.slot is not null)
        {
            return;
        }

        this.slot = LockFileSlotWriter.Open(
            databasePath,
            ownerTypeName,
            respectExisting: settings.RespectExisting,
            machineName: settings.MachineName,
            userName: settings.UserName);
    }

    /// <summary>
    /// Convenience overload that prepends a <see cref="Task"/> wait-step
    /// (typically the operation-gate drain) to <paramref name="steps"/>, sparing
    /// callers the <c>() =&gt; new ValueTask(task)</c> wrapper.
    /// </summary>
    /// <param name="waitForOperations">A task to await before running the disposal steps.</param>
    /// <param name="steps">Disposal steps to run after the wait completes and before releasing the slot.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once every step and the slot release have run.</returns>
    public ValueTask DisposeAfterAsync(Task waitForOperations, params Func<ValueTask>[] steps)
    {
        Guard.NotNull(waitForOperations, nameof(waitForOperations));
        Guard.NotNull(steps, nameof(steps));

        var combined = new Func<ValueTask>[steps.Length + 1];
        combined[0] = () => new ValueTask(waitForOperations);
        Array.Copy(steps, 0, combined, 1, steps.Length);
        return this.DisposeAfterAsync(combined);
    }

    /// <summary>
    /// Runs each of <paramref name="steps"/> in order, capturing failures without
    /// short-circuiting subsequent steps, then unconditionally releases the slot.
    /// Re-throws the captured failure after every step has completed, preserving
    /// the original exception when only one cleanup path fails and throwing an
    /// <see cref="AggregateException"/> when multiple failures occur. This
    /// collapses the "always release the .ldb / .laccdb regardless of which
    /// earlier dispose step threw" pattern that the reader and writer would
    /// otherwise duplicate.
    /// </summary>
    /// <param name="steps">Disposal steps to run before releasing the slot.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once every step and the slot release have run.</returns>
    /// <exception cref="AggregateException">Thrown when multiple disposal or release steps fail.</exception>
    public async ValueTask DisposeAfterAsync(params Func<ValueTask>[] steps)
    {
        Guard.NotNull(steps, nameof(steps));

        List<Exception>? failures = null;

        foreach (Func<ValueTask> step in steps)
        {
            try
            {
                await step().ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Disposal aggregates failures and re-throws once, after all cleanup runs.
            catch (Exception ex)
            {
                failures ??= [];
                failures.Add(ex);
            }
#pragma warning restore CA1031
        }

        try
        {
            this.Dispose();
        }
#pragma warning disable CA1031 // See above — disposal aggregates failures.
        catch (Exception ex)
        {
            failures ??= [];
            failures.Add(ex);
        }
#pragma warning restore CA1031

        if (failures is null)
        {
            return;
        }

        if (failures.Count == 1)
        {
            ExceptionDispatchInfo.Capture(failures[0]).Throw();
            return;
        }

        throw new AggregateException("One or more disposal steps failed.", failures);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        this.slot?.Dispose();
        this.slot = null;
    }
}

/// <summary>
/// Bundles the four lock-file knobs (enabled flag, respect-existing flag,
/// user / machine identity strings) into a single parameter object so
/// <see cref="LockFileCoordinator"/>'s constructor doesn't grow a long list
/// of positional / named arguments at every call site.
/// </summary>
/// <param name="Enabled">Whether lock-file maintenance is requested by the caller.</param>
/// <param name="RespectExisting">When <c>true</c>, opening fails if a lock-file already exists.</param>
/// <param name="UserName">User name to record in the slot.</param>
/// <param name="MachineName">Machine name to record in the slot.</param>
internal readonly record struct LockFileSettings(
    bool Enabled,
    bool RespectExisting,
    string UserName,
    string MachineName);
