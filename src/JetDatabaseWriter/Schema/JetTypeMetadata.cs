namespace JetDatabaseWriter.Schema;

using System;
using JetDatabaseWriter.Models;

/// <summary>Verified size, projection and physical-storage facts for one known JET discriminator.</summary>
/// <param name="FixedSize">The intrinsic fixed-slot byte width, or zero when no fixed payload exists.</param>
/// <param name="VariableSlotPayloadSize">The fixed payload width when the descriptor uses a variable slot, or zero.</param>
/// <param name="ClrType">The default public CLR projection.</param>
/// <param name="DisplayName">The Access display name.</param>
/// <param name="ColumnSizeKind">The public metadata-size projection.</param>
/// <param name="AlwaysVariableLength">Whether authoring defaults to a variable slot; fixed Text and Binary descriptors remain possible.</param>
/// <param name="UsesDeclaredFixedSize">Whether a fixed slot takes its width from the column descriptor.</param>
/// <param name="IsLongValue">Whether values use the MEMO/OLE long-value representation.</param>
/// <param name="UsesNullMaskValue">Whether the null bitmap stores the value itself instead of its presence.</param>
internal sealed record JetTypeMetadata(
    int FixedSize,
    int VariableSlotPayloadSize,
    Type ClrType,
    string DisplayName,
    JetColumnSizeKind ColumnSizeKind = JetColumnSizeKind.FixedBytes,
    bool AlwaysVariableLength = false,
    bool UsesDeclaredFixedSize = false,
    bool IsLongValue = false,
    bool UsesNullMaskValue = false)
{
    /// <summary>Projects the verified public size using the descriptor's declared width where required.</summary>
    /// <param name="declaredSize">The original descriptor size.</param>
    /// <returns>The user-facing column size.</returns>
    internal ColumnSize GetColumnSize(int declaredSize) => this.ColumnSizeKind switch
    {
        JetColumnSizeKind.BooleanBit => ColumnSize.FromBits(1),
        JetColumnSizeKind.TextCharacters => ColumnSize.FromChars(declaredSize > 0 ? declaredSize / 2 : 255),
        JetColumnSizeKind.LongValue => ColumnSize.Lval,
        JetColumnSizeKind.DeclaredBytes => declaredSize > 0 ? ColumnSize.FromBytes(declaredSize) : ColumnSize.Variable,
        JetColumnSizeKind.FixedBytes or _ => ColumnSize.FromBytes(this.FixedSize),
    };
}
