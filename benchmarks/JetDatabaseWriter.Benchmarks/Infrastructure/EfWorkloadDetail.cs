namespace JetDatabaseWriter.Benchmarks.Infrastructure;

#pragma warning disable CA1812 // EF materializes the owned entity from the shared stored fixture.
/// <summary>The table-split owned reference.</summary>
internal sealed class EfWorkloadDetail
{
    public string? Note { get; set; }
}
#pragma warning restore CA1812
