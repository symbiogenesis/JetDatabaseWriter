namespace JetDatabaseWriter.Schema.Models;

using JetDatabaseWriter.Enums;

/// <summary>Mutable builder for a single property entry within a target.</summary>
internal sealed class ColumnPropertyEntryBuilder
{
    /// <summary>Gets or sets the property name (e.g. <c>"DefaultValue"</c>).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the Jet column-type code (see <see cref="ColumnType"/>).</summary>
    public ColumnType DataType { get; set; }

    /// <summary>Gets or sets the flag byte at entry offset 2.</summary>
    public byte DdlFlag { get; set; }

    /// <summary>Gets or sets the raw value bytes per <see cref="DataType"/>'s encoding.</summary>
    public byte[] Value { get; set; } = [];

    /// <summary>Gets or sets the opaque bytes following the declared property value.</summary>
    internal byte[] Padding { get; set; } = [];

    /// <summary>Gets or sets the original name-pool index.</summary>
    internal ushort? SourceNameIndex { get; set; }
}
