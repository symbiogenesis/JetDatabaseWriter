namespace JetDatabaseWriter.ValueDecoding.Models;

using System.Collections.Generic;
using System.IO;

/// <summary>
/// Cell value that the writer's row snapshots hold for a MEMO / OLE value whose
/// stored bytes cannot be read (a truncated descriptor or an LVAL row or chain
/// that cannot be located). Reads, criteria matching and index rebuilds can
/// carry it, but a write that would store it throws, so updates, cascades and
/// schema rewrites never replace the stored value with a placeholder.
/// </summary>
/// <param name="columnName">The column whose value cannot be read.</param>
/// <param name="reason">Why the value cannot be read.</param>
internal sealed class UnreadableLongValue(string columnName, string reason)
{
    /// <summary>Gets the column whose value cannot be read.</summary>
    internal string ColumnName => columnName;

    /// <summary>Gets why the value cannot be read.</summary>
    internal string Reason => reason;

    /// <summary>
    /// Throws when <paramref name="values"/> holds an <see cref="UnreadableLongValue"/>.
    /// Writers call this before they delete the row being rewritten.
    /// </summary>
    /// <param name="values">The row values about to be written.</param>
    /// <param name="tableName">The table being written, for the message.</param>
    /// <exception cref="InvalidDataException">A value cannot be read.</exception>
    internal static void ThrowIfAny(IReadOnlyList<object?> values, string? tableName)
    {
        for (int i = 0; i < values.Count; i++)
        {
            if (values[i] is UnreadableLongValue unreadable)
            {
                throw unreadable.CreateException(tableName);
            }
        }
    }

    /// <inheritdoc/>
    public override string ToString() => $"(unreadable long value in column '{columnName}': {reason})";

    private InvalidDataException CreateException(string? tableName)
    {
        string where = string.IsNullOrEmpty(tableName) ? $"column '{columnName}'" : $"column '{columnName}' of table '{tableName}'";
        return new InvalidDataException(
            $"The MEMO / OLE value in {where} cannot be read ({reason}), so the row cannot be rewritten without losing that value.");
    }
}
