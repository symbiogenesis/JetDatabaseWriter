namespace JetDatabaseWriter.Schema.Models;

using System.Collections.Generic;
using System.Text;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;

/// <summary>Mutable builder for a single property target (table or column).</summary>
internal sealed class ColumnPropertyTargetBuilder
{
    /// <summary>Gets or sets the target name (column name, or table name for the table-level target).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the chunk-type code. Defaults to <see cref="ColumnPropertyChunkType.PropertyBlock"/> (<c>0x0000</c>), the subtype this library emits for new targets.</summary>
    public ColumnPropertyChunkType ChunkType { get; set; }

    /// <summary>Gets the mutable list of property entries in emission order.</summary>
    public List<ColumnPropertyEntryBuilder> Entries { get; } = [];

    /// <summary>Adds a Text-typed (<c>0x0A</c>) string property using the supplied database format's encoding.</summary>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The value.</param>
    /// <param name="format">The format.</param>
    public void AddText(string propertyName, string value, DatabaseFormat format)
    {
        Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
        Guard.NotNull(value, nameof(value));
        Encoding enc = format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;
        this.Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = propertyName,
            DataType = ColumnType.TextType,
            DdlFlag = 0x00,
            Value = enc.GetBytes(value),
        });
    }

    /// <summary>Adds a Memo-typed (<c>0x0C</c>) string property using the supplied database format's encoding.</summary>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The value.</param>
    /// <param name="format">The format.</param>
    public void AddMemoText(string propertyName, string value, DatabaseFormat format)
    {
        Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
        Guard.NotNull(value, nameof(value));
        Encoding enc = format == DatabaseFormat.Jet3Mdb ? Encoding.GetEncoding(1252) : Encoding.Unicode;
        this.Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = propertyName,
            DataType = ColumnType.MemoType,
            DdlFlag = 0x00,
            Value = enc.GetBytes(value),
        });
    }

    /// <summary>Adds a Byte-typed (<c>0x02</c>) property.</summary>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The value.</param>
    public void AddByte(string propertyName, byte value)
    {
        Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
        this.Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = propertyName,
            DataType = ColumnType.ByteType,
            DdlFlag = 0x01,
            Value = [value],
        });
    }

    /// <summary>
    /// Adds a Boolean-typed (<c>0x01</c>) property. Stored on disk as a single
    /// byte: <c>0xFF</c> = true, <c>0x00</c> = false. Matches the wire format
    /// DAO/Access emit for Boolean column properties such as <c>Required</c>.
    /// </summary>
    /// <param name="propertyName">The property name.</param>
    /// <param name="value">The value.</param>
    public void AddBoolean(string propertyName, bool value)
    {
        Guard.NotNullOrEmpty(propertyName, nameof(propertyName));
        this.Entries.Add(new ColumnPropertyEntryBuilder
        {
            Name = propertyName,
            DataType = ColumnType.BooleanType,
            DdlFlag = 0x01,
            Value = [value ? (byte)0xFF : (byte)0x00],
        });
    }
}
