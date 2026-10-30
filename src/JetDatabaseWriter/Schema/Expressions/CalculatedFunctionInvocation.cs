namespace JetDatabaseWriter.Schema.Expressions;

using System;
using System.Collections.Generic;
using JetDatabaseWriter.Models;

internal readonly struct CalculatedFunctionInvocation(
    string name,
    IReadOnlyList<CalculatedExpressionNode> args,
    CalculatedExpressionEvaluationContext context)
{
    public string Name { get; } = name;

    public int Count => args.Count;

    public CalculatedExpressionEvaluationContext Context => context;

    public object Arg(int index)
    {
        object value = index < args.Count ? args[index].Evaluate(context) : DBNull.Value;
        if (value is CalculatedError error)
        {
            throw new CalculatedErrorPropagationException(error);
        }

        if (context.IsCalculatingColumn && value is DBNull)
        {
            string function = this.Name.ToUpperInvariant();
            bool financial = function is "FV" or "PV" or "PMT" or "NPER" or "IPMT" or "PPMT" or "DDB" or "SLN" or "SYD" or "RATE";
            bool due = financial && index == (function is "IPMT" or "PPMT" ? 5 : 4) && function is not ("DDB" or "SLN" or "SYD");
            bool nullStringCharacter = function == "STRING$" && index == 1;
            if (financial || function is "ASC" or "ATN" or "COS" or "EXP" or "LOG" or "SIN" or "SQR" or "TAN" or "CDBL"
                || function == "STR$" || nullStringCharacter || (function == "INSTR" && index is 0 or 3))
            {
                int code = due || function is "CDBL" or "STR$" || nullStringCharacter || (function == "INSTR" && index == 0) ? 94 : 13;
                throw new CalculatedErrorPropagationException(new CalculatedError(code));
            }
        }

        if (context.IsCalculatingColumn && this.Name.Equals("ASC", StringComparison.OrdinalIgnoreCase) && value is string { Length: 0 })
        {
            throw new CalculatedErrorPropagationException(new CalculatedError(5));
        }

        return value;
    }
}
