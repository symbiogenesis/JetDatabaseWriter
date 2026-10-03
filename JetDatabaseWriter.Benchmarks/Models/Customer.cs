namespace JetDatabaseWriter.Benchmarks.Models;

using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;

/// <summary>
/// Entity bound to the synthetic relational <c>Customers</c> table (see
/// <see cref="Infrastructure.SyntheticDatabases"/>). <see cref="Orders"/> is the
/// collection navigation the <c>Include</c> benchmarks load through the
/// <c>FK_Orders_Customers</c> relationship.
/// </summary>
[Table("Customers")]
internal sealed class Customer
{
    public int CustomerId { get; set; }

    public string? Name { get; set; }

    public List<Order> Orders { get; set; } = [];
}
