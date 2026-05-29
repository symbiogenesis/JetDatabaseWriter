namespace JetDatabaseWriter.Enums;

/// <summary>Unit of measurement for a <see cref="Models.ColumnSize"/> value.</summary>
public enum ColumnSizeUnit
{
    /// <summary>Size in bits (e.g., Yes/No stores 1 bit in the null mask).</summary>
    Bits = 0,

    /// <summary>Size in bytes.</summary>
    Bytes = 1,

    /// <summary>Maximum character count for text columns.</summary>
    Chars = 2,

    /// <summary>Variable-length with no declared maximum.</summary>
    Variable = 3,

    /// <summary>Large-value data stored on LVAL pages (MEMO / OLE).</summary>
    Lval = 4,
}
