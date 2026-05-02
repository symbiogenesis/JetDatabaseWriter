namespace JetDatabaseWriter.Internal;

using System;

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
    /// <c>false</c> or a slot is already held.
    /// </summary>
    public void Acquire()
    {
        if (!IsEnabled || _slot != null)
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

    /// <inheritdoc/>
    public void Dispose()
    {
        _slot?.Dispose();
        _slot = null;
    }
}
