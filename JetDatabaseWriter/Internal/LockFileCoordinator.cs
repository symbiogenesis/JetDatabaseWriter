namespace JetDatabaseWriter.Internal;

using System;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Helpers;

/// <summary>
/// Bundles the configuration and runtime state required to maintain a JET
/// lock-file (<c>.ldb</c> / <c>.laccdb</c>) slot for the lifetime of an
/// <see cref="Core.AccessReader"/> or <see cref="Core.AccessWriter"/>.
/// </summary>
/// <remarks>
/// The coordinator is a no-op when <see cref="IsEnabled"/> is <c>false</c>
/// (e.g. for stream-only opens with no backing path, or when the caller
/// disabled lock-file maintenance via options). This consolidates the four
/// or five lock-file fields that previously lived directly on the reader
/// and writer into a single composed object.
/// </remarks>
internal sealed class LockFileCoordinator : IDisposable
{
    private readonly string _databasePath;
    private readonly string _ownerTypeName;
    private readonly LockFileSettings _settings;
    private LockFileSlotWriter? _slot;

    /// <summary>
    /// Initializes a new instance of the <see cref="LockFileCoordinator"/> class.
    /// </summary>
    /// <param name="databasePath">Path to the database whose sibling lock-file should be maintained. Empty disables the coordinator.</param>
    /// <param name="ownerTypeName">Display name of the owning type (e.g. <c>nameof(AccessReader)</c>); used in diagnostics.</param>
    /// <param name="settings">Lock-file behaviour switches and identity strings. See <see cref="LockFileSettings"/>.</param>
    public LockFileCoordinator(string databasePath, string ownerTypeName, in LockFileSettings settings)
    {
        _databasePath = databasePath;
        _ownerTypeName = ownerTypeName;
        _settings = settings;
        IsEnabled = settings.Enabled && !string.IsNullOrEmpty(databasePath);
    }

    /// <summary>Creates a coordinator wired up from <see cref="AccessReaderOptions"/>.</summary>
    public static LockFileCoordinator ForReader(string databasePath, AccessReaderOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new LockFileCoordinator(
            databasePath,
            nameof(AccessReader),
            new LockFileSettings(
                Enabled: options.UseLockFile,
                RespectExisting: false,
                UserName: options.LockFileUserName,
                MachineName: options.LockFileMachineName));
    }

    /// <summary>Creates a coordinator wired up from explicit writer construction parameters.</summary>
    public static LockFileCoordinator ForWriter(
        string databasePath,
        bool useLockFile,
        bool respectExistingLockFile,
        string? lockFileUserName,
        string? lockFileMachineName)
        => new(
            databasePath,
            nameof(AccessWriter),
            new LockFileSettings(
                Enabled: useLockFile,
                RespectExisting: respectExistingLockFile,
                UserName: lockFileUserName,
                MachineName: lockFileMachineName));

    /// <summary>
    /// Creates a coordinator suitable for the writer's static re-encryption helpers,
    /// where <paramref name="options"/> may be <see langword="null"/> and the defaults
    /// honour any existing lock-file.
    /// </summary>
    public static LockFileCoordinator ForReencrypt(string databasePath, AccessWriterOptions? options)
        => ForWriter(
            databasePath,
            useLockFile: options?.UseLockFile ?? true,
            respectExistingLockFile: options?.RespectExistingLockFile ?? true,
            lockFileUserName: options?.LockFileUserName,
            lockFileMachineName: options?.LockFileMachineName);

    /// <summary>Gets a value indicating whether the coordinator will maintain a lock-file slot.</summary>
    public bool IsEnabled { get; }

    /// <summary>
    /// Claims a slot in the sibling lock-file. No-op when <see cref="IsEnabled"/> is
    /// <c>false</c> or a slot is already held. Use together with <c>using</c> /
    /// <c>try-finally</c> for scoped, RAII-style ownership; use
    /// <see cref="AcquireWithRollback"/> instead inside a constructor that hands
    /// ownership to the surrounding instance.
    /// </summary>
    public void Acquire()
    {
        if (!IsEnabled || _slot is not null)
        {
            return;
        }

        _slot = LockFileSlotWriter.Open(
            _databasePath,
            _ownerTypeName,
            respectExisting: _settings.RespectExisting,
            machineName: _settings.MachineName,
            userName: _settings.UserName);
    }

    /// <summary>
    /// Claims the slot and returns a commit-style scope guard (the C++/Rust
    /// "scope-fail" idiom). Pattern:
    /// <code>
    /// using var guard = _lockFile.AcquireWithRollback();
    /// // ... post-acquire initialisation that may throw ...
    /// guard.Commit(); // success — slot ownership transfers to the coordinator
    /// </code>
    /// If <c>Commit</c> is not called before disposal, the slot is released.
    /// Use this from a constructor whose <c>OpenAsync</c> catch only disposes
    /// the underlying stream and never sees the half-built reader / writer —
    /// without it, a populated <c>.ldb</c> / <c>.laccdb</c> would outlive the
    /// failed open.
    /// </summary>
    /// <returns>A scope guard that releases the slot on dispose unless <see cref="RollbackGuard.Commit"/> is called.</returns>
    public RollbackGuard AcquireWithRollback()
    {
        Acquire();
        return new RollbackGuard(this);
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
        return DisposeAfterAsync(combined);
    }

    /// <summary>
    /// Runs each of <paramref name="steps"/> in order, capturing the first failure
    /// without short-circuiting subsequent steps, then unconditionally releases the
    /// slot. Re-throws the first captured failure (lock-file release errors included)
    /// after every step has completed. This collapses the "always release the .ldb /
    /// .laccdb regardless of which earlier dispose step threw" pattern that the
    /// reader and writer would otherwise duplicate.
    /// </summary>
    /// <param name="steps">Disposal steps to run before releasing the slot.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once every step and the slot release have run.</returns>
    public async ValueTask DisposeAfterAsync(params Func<ValueTask>[] steps)
    {
        Guard.NotNull(steps, nameof(steps));

        Exception? failure = null;

        foreach (Func<ValueTask> step in steps)
        {
            try
            {
                await step().ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Disposal aggregates failures and re-throws once, after all cleanup runs.
            catch (Exception ex)
            {
                failure ??= ex;
            }
#pragma warning restore CA1031
        }

        try
        {
            Dispose();
        }
#pragma warning disable CA1031 // See above — disposal aggregates failures.
        catch (Exception ex)
        {
            failure ??= ex;
        }
#pragma warning restore CA1031

        if (failure != null)
        {
            throw failure;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _slot?.Dispose();
        _slot = null;
    }
}

/// <summary>
/// Commit-style scope guard returned by <see cref="LockFileCoordinator.AcquireWithRollback"/>.
/// Implements the well-known C++/Rust "scope-fail" idiom: releases the slot on
/// dispose unless <see cref="Commit"/> has been called, in which case ownership
/// stays with the coordinator. Mutable so a single guard tracks commit state
/// across the <c>using</c>-block; do not copy.
/// </summary>
internal struct RollbackGuard : IDisposable
{
    private LockFileCoordinator? _coordinator;

    internal RollbackGuard(LockFileCoordinator coordinator)
    {
        _coordinator = coordinator;
    }

    /// <summary>Marks the surrounding setup as successful so the slot is retained when this guard disposes.</summary>
    public void Commit() => _coordinator = null;

    /// <inheritdoc/>
    public void Dispose()
    {
        LockFileCoordinator? c = _coordinator;
        _coordinator = null;
        c?.Dispose();
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
/// <param name="UserName">Optional user name to record in the slot.</param>
/// <param name="MachineName">Optional machine name to record in the slot.</param>
internal readonly record struct LockFileSettings(
    bool Enabled,
    bool RespectExisting = false,
    string? UserName = null,
    string? MachineName = null);
