namespace JetDatabaseWriter.Maintenance;

using System.Collections.Generic;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>Original pinned definition evidence and complete independently supplied replacement references.</summary>
/// <param name="RootPage">The original physical root identity, which remains unchanged.</param>
/// <param name="Original">The verified immutable original logical image, including its original padding.</param>
/// <param name="OwnedPages">The expected replacement table-owned-pages map.</param>
/// <param name="FreePages">The expected replacement table-free-space map.</param>
/// <param name="Indexes">Every original physical index's expected root/map pair in original slot order.</param>
/// <param name="LongValues">Every original LVAL trailer record's expected map pair in original order.</param>
/// <remarks>
/// These inputs must remain immutable and originate independently of the destination bytes under comparison.
/// This evidence contains no installed patched bytes. Referenced allocations require their own validation.
/// </remarks>
internal sealed record CompactPinnedTDefReferences(
    long RootPage,
    TDefImage Original,
    UsageMapPointer OwnedPages,
    UsageMapPointer FreePages,
    IReadOnlyList<CompactTDefIndexReferences> Indexes,
    IReadOnlyList<CompactTDefLongValueReferences> LongValues);
