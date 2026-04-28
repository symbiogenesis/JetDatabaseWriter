namespace JetDatabaseWriter.Internal;

using System;
using System.Collections.Generic;
using System.Globalization;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal.Builders;
using JetDatabaseWriter.Internal.Helpers;
using JetDatabaseWriter.Models;

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
    /// <exception cref="NotSupportedException">If the value's type cannot be expressed as a Jet literal (e.g. <c>byte[]</c>).</exception>
    public static string? ToJetExpression(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        switch (value)
        {
            case string s:
                return "\"" + s.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
            case bool b:
                return b ? "True" : "False";
            case byte u8:
                return u8.ToString(CultureInfo.InvariantCulture);
            case sbyte i8:
                return i8.ToString(CultureInfo.InvariantCulture);
            case short i16:
                return i16.ToString(CultureInfo.InvariantCulture);
            case ushort u16:
                return u16.ToString(CultureInfo.InvariantCulture);
            case int i32:
                return i32.ToString(CultureInfo.InvariantCulture);
            case uint u32:
                return u32.ToString(CultureInfo.InvariantCulture);
            case long i64:
                return i64.ToString(CultureInfo.InvariantCulture);
            case ulong u64:
                return u64.ToString(CultureInfo.InvariantCulture);
            case float f:
                return f.ToString("R", CultureInfo.InvariantCulture);
            case double d:
                return d.ToString("R", CultureInfo.InvariantCulture);
            case decimal m:
                return m.ToString(CultureInfo.InvariantCulture);
            case DateTime dt:
                return "#" + dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "#";
            case Guid g:
                return "{guid " + g.ToString("D", CultureInfo.InvariantCulture) + "}";
            case byte[]:
                throw new NotSupportedException("byte[] is not a supported DefaultValue type for Jet expression conversion.");
            default:
                throw new NotSupportedException($"DefaultValue type '{value.GetType()}' cannot be converted to a Jet expression.");
        }
    }

    /// <summary>
    /// Builds a property blob from the supplied <paramref name="columns"/> by emitting
    /// a per-column <see cref="ColumnPropertyBlockBuilder.TargetBuilder"/> for every
    /// column that declares any of the four persisted properties
    /// (<c>DefaultValueExpression</c> / <c>DefaultValue</c>, <c>ValidationRuleExpression</c>,
    /// <c>ValidationText</c>, <c>Description</c>). Returns <see langword="null"/> when no
    /// column declares a persisted property.
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
    /// none of the four persisted properties.
    /// </summary>
    public static void ApplyColumn(ColumnPropertyBlockBuilder builder, ColumnDefinition col, DatabaseFormat format)
    {
        Guard.NotNull(builder, nameof(builder));
        Guard.NotNull(col, nameof(col));

        string? defaultExpr = col.DefaultValueExpression
            ?? ToJetExpression(col.DefaultValue);

        bool any = defaultExpr is not null
            || col.ValidationRuleExpression is not null
            || col.ValidationText is not null
            || col.Description is not null;

        if (!any)
        {
            return;
        }

        ColumnPropertyBlockBuilder.TargetBuilder target = builder.GetOrAddTarget(col.Name);
        if (defaultExpr is not null)
        {
            target.AddText(ColumnPropertyNames.DefaultValue, defaultExpr, format);
        }

        if (col.ValidationRuleExpression is not null)
        {
            target.AddText(ColumnPropertyNames.ValidationRule, col.ValidationRuleExpression, format);
        }

        if (col.ValidationText is not null)
        {
            target.AddText(ColumnPropertyNames.ValidationText, col.ValidationText, format);
        }

        if (col.Description is not null)
        {
            target.AddText(ColumnPropertyNames.Description, col.Description, format);
        }
    }
}
