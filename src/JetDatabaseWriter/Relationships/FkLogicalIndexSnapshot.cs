namespace JetDatabaseWriter.Relationships;

using System.Collections.Generic;

/// <summary>
/// One FK logical-index entry (<c>index_type = 0x02</c>) read from a TDEF
/// before a copy-and-swap schema rewrite, so it can be re-emitted on the
/// rebuilt table.
/// </summary>
/// <param name="Name">The logical-index name (the relationship name on the child side, a hidden <c>.rB</c>-style name on the parent side).</param>
/// <param name="IndexNumber">The entry's <c>index_num</c>.</param>
/// <param name="ColumnNames">The key columns of the backing real index, in key order.</param>
/// <param name="RealIndexNumber">The source physical-index identity, independent of its key columns.</param>
/// <param name="Flags">The physical flags that control uniqueness, null membership and required keys during rebuilding.</param>
/// <param name="Ascending">The backing index's direction for each key column.</param>
/// <param name="SharedIndexName">An ordinary logical index sharing that exact physical identity, or null for an FK-only tree.</param>
/// <param name="RelTblType">The <c>rel_tbl_type</c> byte (parent or child side).</param>
/// <param name="RelIdxNum">The partner entry's <c>index_num</c> (<c>rel_idx_num</c>).</param>
/// <param name="RelTblPage">The partner table's TDEF page (<c>rel_tbl_page</c>).</param>
/// <param name="CascadeUps">The raw <c>cascade_ups</c> byte.</param>
/// <param name="CascadeDels">The raw <c>cascade_dels</c> byte.</param>
internal sealed record FkLogicalIndexSnapshot(
    string Name,
    int IndexNumber,
    IReadOnlyList<string> ColumnNames,
    int RealIndexNumber,
    byte Flags,
    IReadOnlyList<bool> Ascending,
    string? SharedIndexName,
    byte RelTblType,
    int RelIdxNum,
    long RelTblPage,
    byte CascadeUps,
    byte CascadeDels);
