namespace JetDatabaseWriter.TestSupport;

using System;
using System.Reflection;

/// <summary>Inspects private state in tests without adding hooks to production facades.</summary>
internal static class PrivateFieldAccess
{
    /// <summary>Reads a named private instance field from a test subject.</summary>
    /// <typeparam name="T">The field's expected type.</typeparam>
    /// <param name="subject">The test subject.</param>
    /// <param name="name">The declared field name.</param>
    /// <returns>The field value.</returns>
    /// <exception cref="MissingFieldException">The field does not exist.</exception>
    /// <exception cref="InvalidOperationException">The field is null or has another type.</exception>
    internal static T ReadPrivateField<T>(this object subject, string name)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(subject);
        FieldInfo field = subject.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingFieldException(subject.GetType().FullName, name);
        return field.GetValue(subject) as T
            ?? throw new InvalidOperationException($"Field '{name}' is not a non-null {typeof(T).Name}.");
    }
}
