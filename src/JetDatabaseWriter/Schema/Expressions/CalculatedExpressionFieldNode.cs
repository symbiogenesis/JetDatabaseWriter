namespace JetDatabaseWriter.Schema.Expressions;

using System;

/// <summary>A field reference whose spelling cannot be mistaken for a constant.</summary>
/// <param name="name">The field name.</param>
/// <param name="qualifier">The optional table qualifier.</param>
/// <param name="rowLocalQualifier">Whether a bare qualifier binds to the current row regardless of its spelling.</param>
internal sealed class CalculatedExpressionFieldNode(string name, string? qualifier = null, bool rowLocalQualifier = false) : CalculatedExpressionNode
{
    internal string Name => name;

    internal override bool HasStaleQualifier(string tableName)
        => qualifier is not null && !rowLocalQualifier && !string.Equals(qualifier, tableName, StringComparison.OrdinalIgnoreCase);

    public void ValidateDeclaredQualifier(string tableName)
    {
        if (qualifier is not null && !rowLocalQualifier && !string.Equals(qualifier, tableName, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException($"A declared calculated column cannot reference bracketed table '{qualifier}' outside its own table '{tableName}'.");
        }
    }

    public override object Evaluate(CalculatedExpressionEvaluationContext context)
        => context.GetFieldValue(name, qualifier, rowLocalQualifier);
}
