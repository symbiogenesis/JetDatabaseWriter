namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;

/// <summary>
/// Decoded view of a single real-idx physical descriptor's
/// <c>flags</c> byte and <c>first_dp</c> field offset, returned by
/// <see cref="Indexes.IndexLayout.TryReadRealIdxSlot"/>.
/// </summary>
/// <param name="PhysStart">The phys start.</param>
/// <param name="FirstDpOffset">The first data page offset.</param>
/// <param name="Flags">The flags.</param>
internal readonly record struct RealIdxSlot(int PhysStart, int FirstDpOffset, byte Flags)
{
    /// <summary>Gets a value indicating whether the unique flag bit (0x01) is set.</summary>
    public bool IsUnique => (this.Flags & Constants.TableDefinition.UniqueIndexFlag) != 0;

    /// <summary>
    /// Lifts this raw slot into a <see cref="RealIdxEntry"/> by attaching
    /// the decoded <paramref name="keyColumns"/>. By default the entry's
    /// <see cref="RealIdxEntry.IsUnique"/> mirrors this slot's
    /// <see cref="IsUnique"/> (the real-idx <c>flags &amp; 0x01</c> bit);
    /// pass <paramref name="overrideUnique"/> to substitute (e.g.
    /// <c>false</c> when the caller will resolve uniqueness later, or
    /// <c>true</c> when an associated logical-idx PK promotes the slot).
    /// </summary>
    /// <param name="keyColumns">The key columns.</param>
    /// <param name="overrideUnique">The override unique.</param>
    public RealIdxEntry ToEntry(IReadOnlyList<KeyColumn> keyColumns, bool? overrideUnique = null)
        => new(keyColumns, this.FirstDpOffset, overrideUnique ?? this.IsUnique);
}
