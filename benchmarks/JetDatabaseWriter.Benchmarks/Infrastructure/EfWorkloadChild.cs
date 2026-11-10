namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System;

#pragma warning disable CA1812 // EF materializes the child entity from the shared stored fixture.
/// <summary>A child with a GUID primary key and integer foreign key.</summary>
internal sealed class EfWorkloadChild
{
    public Guid Id { get; set; }

    public int ParentId { get; set; }
}
#pragma warning restore CA1812
