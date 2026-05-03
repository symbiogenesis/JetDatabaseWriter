namespace JetDatabaseWriter.Benchmarks.Models;

using System;

/// <summary>
/// DTO that mirrors the synthetic <c>Numeric</c> table (see
/// <see cref="SyntheticDatabases"/>). Used by the typed
/// <c>Rows&lt;T&gt;</c> decode benchmark.
/// </summary>
public sealed class NumericRow
{
    public int? Id { get; set; }

    public int? OrderId { get; set; }

    public int? ProductId { get; set; }

    public short? Quantity { get; set; }

    public decimal? UnitPrice { get; set; }

    public float? Discount { get; set; }

    public int? StatusId { get; set; }

    public DateTime? AddedOn { get; set; }

    public DateTime? ModifiedOn { get; set; }
}
