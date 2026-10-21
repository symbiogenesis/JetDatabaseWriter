namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// DTO bound to the synthetic <c>Memos</c> table — one int + one MEMO column
/// whose payload mixes inline, single-LVAL-page, and chained-LVAL-page sizes.
/// Used by the LVAL-decode benchmarks.
/// </summary>
public sealed class MemoRow
{
    public int Id { get; set; }

    public string? Body { get; set; }
}
