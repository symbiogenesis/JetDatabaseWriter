namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using JetDatabaseWriter.Indexes.Models;
using JetDatabaseWriter.Pages.Models;

/// <summary>Independently completed immutable stored entries for one original physical descriptor, in slot order.</summary>
/// <remarks>
/// The caller establishes complete physical identity, traversal, ownership and live-bookmark evidence independently.
/// Keys and enumerations remain immutable, replayable and strictly ordered. Reopened validation receives original
/// observations independently of builder output, never entries decoded from the destination or builder fences.
/// Logical descriptors sharing one physical descriptor retain their original raw metadata. Aliased physical roots
/// or physical map rows are refused; observations do not establish unknown incoming ownership or semantic row/key agreement.
/// </remarks>
/// <param name="RootPage">The observed original physical root.</param>
/// <param name="OwnedPages">The observed original physical ownership-map row.</param>
/// <param name="Entries">The independently verified complete original stored-key/bookmark sequence.</param>
internal sealed record CompactPinnedTableIndexSource(uint RootPage, UsageMapPointer OwnedPages, IAsyncEnumerable<IndexEntry> Entries);
