namespace JetDatabaseWriter.Pages.Models;

/// <summary>Parsed row-trailer metadata - see <see cref="AccessBase.TryParseRowLayout"/>.</summary>
/// <param name="NumCols">The number of cols.</param>
/// <param name="NullMaskPos">The null mask pos.</param>
/// <param name="VarLen">The var len.</param>
/// <param name="VarTableStart">The var table start.</param>
/// <param name="Eod">The end-of-data marker size.</param>
internal readonly record struct RowLayout(
    int NumCols,
    int NullMaskPos,
    int VarLen,
    int VarTableStart,
    int Eod);
