namespace JetDatabaseWriter.Pages.Models;

/// <summary>Parsed row-trailer metadata - see <see cref="ValueDecoding.RowDecodePlan.TryParseRowLayout"/>.</summary>
/// <param name="NumCols">The number of cols.</param>
/// <param name="NullMaskPos">The null mask pos.</param>
/// <param name="VarLen">The var len.</param>
/// <param name="VarTableStart">The var table start.</param>
/// <param name="Eod">The row-relative offset where the variable-length data ends, with any Jet3 jump-table high part applied.</param>
/// <param name="JumpTableEnd">
/// Jet3 only: the row-relative position just past the jump table, which is
/// the <c>var_len</c> byte; jump entry <c>k</c> sits at <c>JumpTableEnd - 1 - k</c>.
/// See <see cref="Jet3JumpTable"/>.
/// </param>
/// <param name="JumpCount">Jet3 only: the number of jump entries in use, a trailing dummy already dropped; 0 when offsets need no high part.</param>
internal readonly record struct RowLayout(
    int NumCols,
    int NullMaskPos,
    int VarLen,
    int VarTableStart,
    int Eod,
    int JumpTableEnd = 0,
    int JumpCount = 0);
