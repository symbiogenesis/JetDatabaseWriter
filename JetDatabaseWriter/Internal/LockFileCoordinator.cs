namespace JetDatabaseWriter.Internal;

using System;
using System.Threading.Tasks;
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
    private readonly bool _respectExisting;
    private readonly string? _userName;
    private readonly string? _machineName;
    private LockFileSlotWriter? _slot;

    /// <summary>
    /// Initializes a new instance of the <see cref="LockFileCoordinator"/> class.
    /// </summary>
    /// <param name="databasePath">Path to the database whose sibling lock-file should be maintained. Empty disables the coordinator.</param>
    /// <param name="ownerTypeName">Display name of the owning type (e.g. <c>nameof(AccessReader)</c>); used in diagnostics.</param>
    /// <param name="enabled">Whether lock-file maintenance is requested by the caller.</param>
    /// <param name="respectExisting">When <c>true</c>, opening fails if a lock-file already exists.</param>
    /// <param name="userName">Optional user name to record in the slot.</param>
    /// <param name="machineName">Optional machine name to record in the slot.</param>
    public LockFileCoordinator(
        string databasePath,
        string ownerTypeName,
        bool enabled,
        bool respectExisting = false,
        string? userName = null,
        string? machineName = null)
    {
        _databasePath = databasePath;
        _ownerTypeName = ownerTypeName;
        _respectExisting = respectExisting;
        _userName = userName;
        _machineName = machineName;
        IsEnabled = enabled && !string.IsNullOrEmpty(databasePath);
    }

    /// <summary>Gets a value indicating whether the coordinator will maintain a lock-file slot.</summary>
    public bool IsEnabled { get; }

    /// <summary>
    /// Claims a slot in the sibling lock-file. No-op when <see cref="IsEnabled"/> is
    /// <c>false</c> or a slot is already held. Use together with <c>using</c> /
    /// <c>try-finally</c> for scoped, RAII-style ownership; use
    /// <see cref="AcquireThen"/> instead inside a constructor that hands ownership
    /// to the surrounding instance.
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
            respectExisting: _respectExisting,
            machineName: _machineName,
            userName: _userName);
    }

    /// <summary>
    /// Constructor-friendly wrapper over <see cref="Acquire"/>: claims the slot,
    /// runs <paramref name="setup"/>, and releases the slot if (and only if)
    /// <paramref name="setup"/> throws. Use this from a constructor whose
    /// <c>OpenAsync</c> catch only disposes the underlying stream and never sees
    /// the half-built reader / writer — without it, a populated <c>.ldb</c> /
    /// <c>.laccdb</c> would outlive the failed open.
    /// </summary>
    /// <param name="setup">The post-acquire initialisation step to run under the slot.</param>
    public void AcquireThen(Action setup)
    {
        Guard.NotNull(setup, nameof(setup));

        Acquire();
        try
        {
            setup();
        }
        catch
        {
            Dispose();
            throw;
        }
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
