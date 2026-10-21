namespace JetDatabaseWriter.Models;

using JetDatabaseWriter.Enums;

/// <summary>
/// Structured column size: a numeric <see cref="Value"/> paired with a <see cref="ColumnSizeUnit"/>.
/// Use <see cref="ToString"/> for a human-readable description.
/// </summary>
public readonly record struct ColumnSize
{
    /// <summary>Variable-length with no declared maximum.</summary>
    public static readonly ColumnSize Variable = new(null, ColumnSizeUnit.Variable);

    /// <summary>Large-value data stored on LVAL pages (MEMO / OLE).</summary>
    public static readonly ColumnSize Lval = new(null, ColumnSizeUnit.Lval);

    private ColumnSize(int? value, ColumnSizeUnit unit)
    {
        this.Value = value;
        this.Unit = unit;
    }

    /// <summary>Gets the numeric count; <c>null</c> for <see cref="ColumnSizeUnit.Variable"/> and <see cref="ColumnSizeUnit.Lval"/>.</summary>
    public int? Value { get; }

    /// <summary>Gets the unit in which <see cref="Value"/> is expressed.</summary>
    public ColumnSizeUnit Unit { get; }

    /// <summary>Creates a fixed size expressed in bits.</summary>
    /// <param name="count">The count.</param>
    /// <returns>A column size with the specified count expressed in bits.</returns>
    public static ColumnSize FromBits(int count) => new(count, ColumnSizeUnit.Bits);

    /// <summary>Creates a fixed size expressed in bytes.</summary>
    /// <param name="count">The count.</param>
    /// <returns>A column size with the specified count expressed in bytes.</returns>
    public static ColumnSize FromBytes(int count) => new(count, ColumnSizeUnit.Bytes);

    /// <summary>Creates a maximum character count for a text column.</summary>
    /// <param name="count">The count.</param>
    /// <returns>A column size with the specified maximum character count.</returns>
    public static ColumnSize FromChars(int count) => new(count, ColumnSizeUnit.Chars);

    /// <inheritdoc/>
    public override string ToString() => this.Unit switch
    {
        ColumnSizeUnit.Bits => this.Value == 1 ? "1 bit" : $"{this.Value} bits",
        ColumnSizeUnit.Bytes => this.Value == 1 ? "1 byte" : $"{this.Value} bytes",
        ColumnSizeUnit.Chars => $"{this.Value} chars",
        ColumnSizeUnit.Variable => "variable",
        ColumnSizeUnit.Lval => "LVAL",
        _ => string.Empty,
    };
}
