namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System.Collections.Generic;

/// <summary>A parent with an owned reference and indexed collection relationship.</summary>
internal sealed class EfWorkloadParent
{
    public int Id { get; set; }

    public int Score { get; set; }

    public string Name { get; set; } = string.Empty;

    public EfWorkloadDetail? Detail { get; set; }

    public List<EfWorkloadChild> Children { get; set; } = [];
}
