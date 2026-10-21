namespace JetDatabaseWriter.Indexes.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Decoded view of a single logical-idx entry's format-invariant fields,
/// returned by <see cref="Indexes.IndexLayout.TryReadLogicalEntry"/>. <see cref="IndexType"/>
/// is exposed as <see cref="IndexKind"/> rather than the raw byte so
/// consumers compare against enum values directly.
/// </summary>
/// <param name="FieldsOffset">The fields offset.</param>
/// <param name="IndexNum">The index number of.</param>
/// <param name="IndexNum2">The index num2.</param>
/// <param name="RelIdxNum">The relationship index number of.</param>
/// <param name="RelTblPage">The relationship table page.</param>
/// <param name="CascadeUps">The cascade ups.</param>
/// <param name="CascadeDels">The cascade dels.</param>
/// <param name="IndexType">The index type.</param>
internal readonly record struct LogicalIdxEntry(
    int FieldsOffset,
    int IndexNum,
    int IndexNum2,
    int RelIdxNum,
    int RelTblPage,
    byte CascadeUps,
    byte CascadeDels,
    IndexKind IndexType);
