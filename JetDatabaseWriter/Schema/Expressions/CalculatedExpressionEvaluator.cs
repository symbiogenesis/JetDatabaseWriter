namespace JetDatabaseWriter.Schema.Expressions;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Schema.Models;

using static JetDatabaseWriter.Schema.Expressions.CalculatedExpressionCoercion;

internal static class CalculatedExpressionEvaluator
{
    public static void Apply(TableDef tableDef, IReadOnlyList<ColumnConstraint> constraints, object[] values, bool force)
    {
        var context = new CalculatedExpressionEvaluationContext(tableDef, constraints, values, force);
        for (int i = 0; i < constraints.Count; i++)
        {
            ColumnConstraint constraint = constraints[i];
            if (constraint.IsCalculated && (force || IsNull(values[i])))
            {
                values[i] = context.EvaluateColumn(i);
            }
        }
    }
}
