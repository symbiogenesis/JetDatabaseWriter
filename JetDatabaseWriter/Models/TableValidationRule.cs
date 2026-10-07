namespace JetDatabaseWriter.Models;

/// <summary>An Access table validation expression and its optional validation message.</summary>
/// <param name="Expression">The expression checked against each complete inserted or updated row.</param>
/// <param name="ValidationText">The message returned when the expression rejects a row.</param>
public sealed record TableValidationRule(string Expression, string? ValidationText = null);
