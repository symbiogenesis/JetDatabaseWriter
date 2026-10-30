namespace JetDatabaseWriter.Schema.Expressions;

using JetDatabaseWriter.Exceptions;

/// <summary>Native table calculations have a smaller language than defaults and queries.</summary>
internal static class CalculatedColumnExpressionPolicy
{
    internal static void ValidateFunction(string name)
    {
        // Keep the original spelling: only the measured intrinsic string functions
        // have native $ aliases. Shared interpreter aliases remain separate.
        if (name.ToUpperInvariant() is not (
            "ABS" or "ASC" or "ATN" or "CDBL" or "CHOOSE" or "COS" or "DATESERIAL" or "DAY" or "DDB" or
            "EXP" or "FIX" or "FV" or "HOUR" or "IIF" or "INSTR" or "INT" or "IPMT" or "ISNULL" or "ISEMPTY" or
            "LCASE" or "LCASE$" or "LEFT" or "LEFT$" or "LEN" or "LOG" or "MID" or "MID$" or "MINUTE" or
            "MONTH" or "MONTHNAME" or "NPER" or "PMT" or "PPMT" or "PV" or "RATE" or "RIGHT" or "RIGHT$" or
            "ROUND" or "SECOND" or "SGN" or "SIN" or "SLN" or "SPACE" or "SPACE$" or "SQR" or "STR" or "STR$" or
            "STRING" or "STRING$" or "SYD" or "TAN" or "TIMESERIAL" or "TRIM" or "TRIM$" or "UCASE" or "UCASE$" or
            "WEEKDAY" or "WEEKDAYNAME" or "YEAR"))
        {
            throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Access table calculated columns do not accept the {name} function.");
        }
    }

    internal static void ValidateOperator(string name)
    {
        if (name is "BETWEEN" or "MOD" or "INTDIV" or "XOR" or "EQV" or "IMP")
        {
            throw new JetNotSupportedException(JetErrorCode.UnsupportedExpression, $"Access table calculated columns do not accept the {name} operator.");
        }
    }
}
