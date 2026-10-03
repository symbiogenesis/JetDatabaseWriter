namespace JetDatabaseWriter.Mapping;

using System;
using System.Globalization;
using System.Reflection;
using JetDatabaseWriter.Models;

/// <summary>
/// The one conversion from a decoded cell value to the type of the property it maps to,
/// shared by every typed read: the compiled row mapper behind <c>Rows&lt;T&gt;</c>,
/// <c>ReadTableAsync&lt;T&gt;</c>, <c>FromIndex&lt;T&gt;</c>, <c>Query&lt;T&gt;</c> and
/// linked-table reads, and the runtime mapper that materializes <c>Include</c> targets.
/// </summary>
/// <remarks>
/// <para>The rules, applied in order:</para>
/// <list type="number">
/// <item><description>A value the property can already hold passes through unchanged.</description></item>
/// <item><description>Text converts to a <see cref="Hyperlink"/> property through <see cref="Hyperlink.Parse(string?)"/>, and a <see cref="Hyperlink"/> converts to a text property through <see cref="Hyperlink.ToString"/>. Empty text parses to no hyperlink, which leaves the property unassigned.</description></item>
/// <item><description>An enum property takes an integral value, converted with overflow checking to the enum's underlying type (a value with no named member still maps, as a C# cast does), or text, parsed as a member name ignoring case or as a number.</description></item>
/// <item><description>A <see cref="Guid"/> property takes text in any format <see cref="Guid.Parse(string)"/> accepts, braces included, or a 16-byte array in <see cref="Guid.ToByteArray()"/> order.</description></item>
/// <item><description>Anything else goes through <see cref="Convert.ChangeType(object, Type, IFormatProvider)"/> with the invariant culture. It rounds a number to the target's precision: a fraction to an integral type as the nearest integer, halves to even as Access's <c>CInt</c> does (2.5 to 2, 3.5 to 4), and a <see cref="double"/> to the nearest <see cref="float"/>; only a value outside the target's range throws.</description></item>
/// </list>
/// <para>
/// A value no rule converts throws <see cref="InvalidCastException"/> naming the column, the
/// property, the value's type and the property type, with the original exception as
/// <see cref="Exception.InnerException"/>. There is no lenient mode: a value is never
/// silently left at the property's default.
/// </para>
/// </remarks>
internal static class ValueCoercer
{
    /// <summary>
    /// Gets <see cref="Coerce(object, Type, string, PropertyInfo)"/>, for the compiled row
    /// mappers that call it from an expression tree.
    /// </summary>
    internal static MethodInfo CoerceMethod { get; } =
        typeof(ValueCoercer).GetMethod(nameof(Coerce), BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException("Failed to get method info for ValueCoercer.Coerce.");

    /// <summary>
    /// Converts <paramref name="value"/>, read from <paramref name="columnName"/>, to
    /// <paramref name="targetType"/>, the type of <paramref name="property"/>.
    /// </summary>
    /// <param name="value">The decoded cell value. Callers leave the property unassigned for <see langword="null"/> and <see cref="DBNull"/> before they call this.</param>
    /// <param name="targetType">The property type; a <see cref="Nullable{T}"/> converts to its underlying type.</param>
    /// <param name="columnName">The column the value was read from, for the error message.</param>
    /// <param name="property">The property the value maps to, for the error message.</param>
    /// <returns>
    /// The converted value, or <see langword="null"/> when the property is to be left
    /// unassigned (empty text for a <see cref="Hyperlink"/> property).
    /// </returns>
    /// <exception cref="InvalidCastException"><paramref name="value"/> cannot be converted to <paramref name="targetType"/>.</exception>
    public static object? Coerce(object value, Type targetType, string columnName, PropertyInfo property)
    {
        Type target = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (target.IsInstanceOfType(value))
        {
            return value;
        }

        try
        {
            return ConvertValue(value, target);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            string propertyName = (property.ReflectedType ?? property.DeclaringType)?.Name + "." + property.Name;
            throw new InvalidCastException(
                $"Column '{columnName}' cannot be mapped to property '{propertyName}': a value of type '{value.GetType()}' cannot be converted to '{target}'.",
                ex);
        }
    }

    private static object? ConvertValue(object value, Type target)
    {
        if (target == typeof(Hyperlink) && value is string hyperlinkText)
        {
            return Hyperlink.Parse(hyperlinkText);
        }

        if (target == typeof(string) && value is Hyperlink hyperlink)
        {
            return hyperlink.ToString();
        }

        if (target.IsEnum)
        {
            if (value is string enumText)
            {
                return Enum.Parse(target, enumText, ignoreCase: true);
            }

            if (value is sbyte or byte or short or ushort or int or uint or long or ulong)
            {
                // Convert.ChangeType checks for overflow, so 300 never wraps into a byte enum.
                object underlying = Convert.ChangeType(value, Enum.GetUnderlyingType(target), CultureInfo.InvariantCulture);
                return Enum.ToObject(target, underlying);
            }
        }

        if (target == typeof(Guid))
        {
            if (value is string guidText)
            {
                return Guid.Parse(guidText);
            }

            if (value is byte[] { Length: 16 } guidBytes)
            {
                return new Guid(guidBytes);
            }
        }

        return Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
    }
}
