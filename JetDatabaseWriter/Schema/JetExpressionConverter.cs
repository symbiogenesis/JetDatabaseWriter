namespace JetDatabaseWriter.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Infrastructure;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;

/// <summary>
/// Conversions from CLR literal default values to the string a Jet expression engine
/// would parse (the form persisted in <c>MSysObjects.LvProp</c> as the
/// <c>DefaultValue</c> property), plus the helper that composes an
/// <see cref="ColumnPropertyBlockBuilder"/> from a column-definition list.
/// </summary>
internal static class JetExpressionConverter
{
    /// <summary>
    /// Converts a CLR literal default value to its Jet expression representation,
    /// or returns <see langword="null"/> when the value is null / <see cref="DBNull"/>.
    /// </summary>
    /// <param name="value">The value.</param>
    /// <exception cref="NotSupportedException">If the value's type cannot be expressed as a Jet literal (e.g. <c>byte[]</c>).</exception>
    public static string? ToJetExpression(object? value)
    {
        if (value is null or DBNull)
        {
            return null;
        }

        return value switch
        {
            string s => "\"" + s.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"",
            bool b => b ? "True" : "False",
            byte u8 => u8.ToString(CultureInfo.InvariantCulture),
            sbyte i8 => i8.ToString(CultureInfo.InvariantCulture),
            short i16 => i16.ToString(CultureInfo.InvariantCulture),
            ushort u16 => u16.ToString(CultureInfo.InvariantCulture),
            int i32 => i32.ToString(CultureInfo.InvariantCulture),
            uint u32 => u32.ToString(CultureInfo.InvariantCulture),
            long i64 => i64.ToString(CultureInfo.InvariantCulture),
            ulong u64 => u64.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            DateTime dt => "#" + dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "#",
            Guid g => "{guid " + g.ToString("D", CultureInfo.InvariantCulture) + "}",
            byte[] => throw new NotSupportedException("byte[] is not a supported DefaultValue type for Jet expression conversion."),
            _ => throw new NotSupportedException($"DefaultValue type '{value.GetType()}' cannot be converted to a Jet expression."),
        };
    }

    /// <summary>
    /// Builds a property blob from the supplied <paramref name="columns"/> by emitting
    /// a per-column <see cref="ColumnPropertyTargetBuilder"/> for every
    /// column that declares any persisted property — <c>Required</c> (NOT NULL),
    /// text-column <c>AllowZeroLength</c>, <c>DefaultValueExpression</c> /
    /// <c>DefaultValue</c>, <c>ValidationRuleExpression</c>, <c>ValidationText</c>,
    /// or <c>Description</c>. Returns <see langword="null"/>
    /// when no column declares a persisted property.
    /// </summary>
    /// <param name="columns">Column definitions. May be <see langword="null"/>.</param>
    /// <param name="format">Target database format (selects Jet3 codepage vs Jet4 UTF-16LE).</param>
    public static byte[]? BuildLvPropBlob(IReadOnlyList<ColumnDefinition>? columns, DatabaseFormat format)
    {
        if (columns is null || columns.Count == 0)
        {
            return null;
        }

        var builder = new ColumnPropertyBlockBuilder();
        foreach (ColumnDefinition col in columns)
        {
            ApplyColumn(builder, col, format);
        }

        return builder.ToBytes(format);
    }

    /// <summary>
    /// Adds (or updates) a column-level target on <paramref name="builder"/> using the
    /// persisted-property fields of <paramref name="col"/>. No-op when the column declares
    /// no persisted properties (and is nullable, since <c>Required</c> is otherwise emitted).
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="col">The column descriptor.</param>
    /// <param name="format">The format.</param>
    public static void ApplyColumn(ColumnPropertyBlockBuilder builder, ColumnDefinition col, DatabaseFormat format)
    {
        Guard.NotNull(builder, nameof(builder));
        Guard.NotNull(col, nameof(col));

        string? defaultExpr = col.DefaultValueExpression
            ?? ToJetExpression(col.DefaultValue);

        // Required = true is persisted explicitly to match how DAO/Access surface
        // NOT NULL constraints. AutoNumber columns are the DAO-observed exception:
        // even when marked required, their non-null semantics come from the auto
        // column flag and DAO does not emit a Required property for them.
        bool emitAllowZeroLength = IsTextLikeColumn(col);
        bool emitRequired = !col.IsAutoIncrement && (!col.IsNullable || emitAllowZeroLength);
        bool any = emitRequired
            || emitAllowZeroLength
            || defaultExpr is not null
            || col.ValidationRuleExpression is not null
            || col.ValidationText is not null
            || col.Description is not null
            || col.IsCalculated;

        if (!any)
        {
            return;
        }

        ColumnPropertyTargetBuilder target = builder.GetOrAddTarget(col.Name);
        if (emitAllowZeroLength)
        {
            target.AddBoolean(Constants.ColumnPropertyNames.AllowZeroLength, false);
        }

        if (emitRequired)
        {
            target.AddBoolean(Constants.ColumnPropertyNames.Required, !col.IsNullable);
        }

        if (defaultExpr is not null)
        {
            target.AddText(Constants.ColumnPropertyNames.DefaultValue, defaultExpr, format);
        }

        if (col.ValidationRuleExpression is not null)
        {
            target.AddText(Constants.ColumnPropertyNames.ValidationRule, col.ValidationRuleExpression, format);
        }

        if (col.ValidationText is not null)
        {
            target.AddText(Constants.ColumnPropertyNames.ValidationText, col.ValidationText, format);
        }

        if (col.Description is not null)
        {
            target.AddText(Constants.ColumnPropertyNames.Description, col.Description, format);
        }

        if (col.IsCalculated)
        {
            target.AddMemoText(Constants.ColumnPropertyNames.Expression, col.CalculationExpression ?? string.Empty, format);
            target.AddByte(Constants.ColumnPropertyNames.ResultType, (byte)JetTypeInfo.TypeCodeFromDefinition(col));
        }
    }

    private static bool IsTextLikeColumn(ColumnDefinition col) =>
        col.ClrType == typeof(string) || col.ClrType == typeof(Hyperlink);
}
