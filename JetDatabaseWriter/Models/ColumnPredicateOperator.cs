namespace JetDatabaseWriter.Models;

/// <summary>
/// The comparison applied by a single <see cref="ColumnPredicate"/>.
/// </summary>
public enum ColumnPredicateOperator
{
    /// <summary>Column value equals the operand (database null equals database null).</summary>
    Equal = 0,

    /// <summary>Column value does not equal the operand.</summary>
    NotEqual = 1,

    /// <summary>Column value is greater than the operand.</summary>
    GreaterThan = 2,

    /// <summary>Column value is greater than or equal to the operand.</summary>
    GreaterThanOrEqual = 3,

    /// <summary>Column value is less than the operand.</summary>
    LessThan = 4,

    /// <summary>Column value is less than or equal to the operand.</summary>
    LessThanOrEqual = 5,

    /// <summary>Column value lies within an inclusive lower/upper range.</summary>
    Between = 6,

    /// <summary>Column value equals one of a set of operands.</summary>
    In = 7,

    /// <summary>Column value is database null.</summary>
    IsNull = 8,

    /// <summary>Column value is not database null.</summary>
    IsNotNull = 9,
}
