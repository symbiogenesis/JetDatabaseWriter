namespace JetDatabaseWriter.Maintenance;

using System;
using JetDatabaseWriter.Pages.Models;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;

/// <summary>One borrowed physical field slot, including retained bytes behind an unset assignment bit.</summary>
/// <param name="Column">The original definition's column identity and storage descriptor.</param>
/// <param name="Location">The header bookmark and resolved payload location of its row.</param>
/// <param name="Kind">Whether the field is absent, unassigned, a null-mask Boolean, or assigned storage.</param>
/// <param name="RowOffset">The slot's offset within the supplied row, or zero when no slot is present.</param>
/// <param name="RawBytes">The complete physical slot; borrowed until its awaited visitor returns.</param>
/// <param name="BoolValue">The stored null-mask value for an ordinary Boolean.</param>
internal readonly record struct CompactRawField(ColumnInfo Column, RowLocation Location, CompactRawFieldKind Kind, int RowOffset, ReadOnlyMemory<byte> RawBytes, bool BoolValue)
{
    /// <summary>Gets a value indicating whether the field's cached storage type is known without guessing a calculated result type.</summary>
    internal bool IsStorageTypeKnown => !this.Column.IsCalculated || this.Column.CalculatedResultType != default;

    /// <summary>Gets a value indicating whether the verified stored value type uses a MEMO/OLE descriptor.</summary>
    /// <remarks>Only <see cref="CompactRawFieldKind.Stored"/> fields identify assigned descriptors to audit. Unassigned bytes remain opaque.</remarks>
    internal bool IsLongValue => this.IsStorageTypeKnown && JetTypeInfo.UsesLongValueStorage(this.Column);
}
