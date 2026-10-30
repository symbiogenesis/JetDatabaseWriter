namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;

internal static class CalculatedExpressionEvaluator
{
    public static void Apply(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, string? tableName = null, JetDatabaseWriter.Indexes.Collation.JetTextCollation? textCollation = null)
    {
        var context = new CalculatedExpressionEvaluationContext(tableDef, constraints, values, tableName, textCollation);
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint constraint = constraints[i];
            if (constraint.IsCalculated)
            {
                values[i] = context.EvaluateColumn(i);
            }
        }
    }
}
