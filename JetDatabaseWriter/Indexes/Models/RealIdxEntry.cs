namespace JetDatabaseWriter.Indexes.Models;

using System.Collections.Generic;

/// <summary>
/// One real-idx slot decoded into its full <c>col_map</c> key list, the
/// byte offset of its <c>first_dp</c> field within the logical TDEF
/// buffer, and the resolved unique flag (real-idx <c>flags &amp; 0x01</c>
/// OR an associated logical-idx with <c>index_type = 0x01</c>). Used by
/// the writer's index-maintenance and unique-check paths to carry
/// per-real-idx state without re-decoding the TDEF block.
/// </summary>
/// <param name="IndexKeyColumns">Decoded key-column map for this real index.</param>
/// <param name="FirstDpOffset">Byte offset of the <c>first_dp</c> field in the logical TDEF buffer (the whole page chain stitched together; on a wide table it lies past the first page, so write it back through <see cref="DatabaseFile.WriteTDefChainInPlaceAsync"/> or <see cref="DatabaseFile.WriteTDefInt32Async"/>).</param>
/// <param name="IsUnique">Whether the real index enforces uniqueness.</param>
internal readonly record struct RealIdxEntry(
    IReadOnlyList<KeyColumn> IndexKeyColumns,
    int FirstDpOffset,
    bool IsUnique);
