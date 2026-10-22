namespace JetDatabaseWriter.Maintenance;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

/// <summary>Caller-provisioned private storage factories supporting disk spill and independent reopening.</summary>
/// <param name="Create">Creates empty readable writable seekable storage and transfers the stream to the coordinator.</param>
/// <param name="Open">Independently reopens immutable readable seekable storage and transfers the stream to the coordinator.</param>
/// <param name="ObserveUnassignedRow">Required when any table opts into active membership; receives every pinned unassigned row during independent admission and repeated raw scans.</param>
/// <remarks>
/// Every role/owner pair has distinct backing storage, including through wrappers, and is separate from the source.
/// Factories are explicitly serialized. Open is called only after every construction stream is flushed and closed.
/// Destination and allocation-ledger owners are zero; table sidecar owners are their original pinned TDEF identities.
/// Provider I/O and stream storage remain caller-owned work outside coordinator admission. The caller retains private
/// partial output after any failure, controls cleanup, and never publishes it based on this private evidence alone.
/// </remarks>
internal sealed record CompactPinnedDatabaseWorkspace(
    Func<CompactPinnedDatabaseFile, long, CancellationToken, ValueTask<Stream>> Create,
    Func<CompactPinnedDatabaseFile, long, CancellationToken, ValueTask<Stream>> Open,
    Func<CompactActiveTableRowRecord, CancellationToken, ValueTask>? ObserveUnassignedRow = null);
