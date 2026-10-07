namespace JetDatabaseWriter.Schema.Models;

using JetDatabaseWriter.Schema.Expressions;

/// <summary>A persisted whole-row Access validation rule and its cached expression plan.</summary>
/// <param name="Expression">The persisted expression.</param>
/// <param name="ValidationText">The user-facing validation text.</param>
internal sealed record TableValidationConstraint(string Expression, string? ValidationText)
{
    /// <summary>Gets or sets the parsed expression plan.</summary>
    public CalculatedExpressionPlan? Plan { get; set; }
}
