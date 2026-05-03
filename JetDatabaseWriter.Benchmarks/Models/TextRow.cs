namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// DTO that mirrors the synthetic <c>TextHeavy</c> table (see
/// <see cref="SyntheticDatabases"/>).
/// </summary>
public sealed class TextRow
{
    public int? Id { get; set; }

    public string? FirstName { get; set; }

    public string? LastName { get; set; }

    public string? Email { get; set; }

    public string? City { get; set; }

    public string? Notes { get; set; }
}
