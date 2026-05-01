namespace JetDatabaseWriter.Internal.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Aggregated operation against an intermediate page: replace, insert
/// after, or remove a summary entry at <paramref name="OriginalIndex"/>.
/// Original index always refers to the position in the LIVE intermediate's
/// entry list (not the post-mutation list). When two ops share an
/// original index, declaration order is preserved (a Replace for the
/// original entry followed by an InsertAfter that produces a leaf split).
/// </summary>
internal readonly record struct IntermediateOp(
    int OriginalIndex,
    IntermediateOpType Type,
    DecodedIntermediateEntry NewEntry);
