namespace JetDatabaseWriter.Schema;

/// <summary>The public size projection associated with a known column type.</summary>
internal enum JetColumnSizeKind
{
    /// <summary>The intrinsic fixed byte width.</summary>
    FixedBytes = 0,

    /// <summary>The single Boolean value bit in the null bitmap.</summary>
    BooleanBit = 1,

    /// <summary>The declared text byte width expressed as characters.</summary>
    TextCharacters = 2,

    /// <summary>A logical value backed by long-value or complex storage.</summary>
    LongValue = 3,

    /// <summary>The original descriptor byte width, or an unspecified variable width.</summary>
    DeclaredBytes = 4,
}
