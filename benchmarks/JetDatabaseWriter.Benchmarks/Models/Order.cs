namespace JetDatabaseWriter.Benchmarks.Models;

using System;
using System.ComponentModel.DataAnnotations.Schema;

/// <summary>
/// Entity bound to the synthetic relational <c>Orders</c> table (see
/// <see cref="Infrastructure.SyntheticDatabases"/>). <see cref="Customer"/> is the
/// reference navigation the <c>Include</c> benchmarks load; the seek benchmarks
/// map rows to it directly.
/// </summary>
[Table("Orders")]
internal sealed class Order
{
    public int OrderId { get; set; }

    public int CustomerId { get; set; }

    public DateTime OrderDate { get; set; }

    public decimal Amount { get; set; }

    public Customer? Customer { get; set; }
}
