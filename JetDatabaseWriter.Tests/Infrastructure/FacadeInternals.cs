namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.Reflection;

/// <summary>
/// Reads private state behind the public facades, for the few tests that check
/// how <see cref="AccessReader.OpenAsync(string, AccessReaderOptions?, System.Threading.CancellationToken)"/>
/// wires its internals (page-cache allocation, random-access reads). The
/// facades expose no members for tests, so this goes through reflection; tests
/// that only need pages, the catalog, or a service open <see cref="ReaderHarness"/>
/// or <see cref="WriterHarness"/> instead.
/// </summary>
internal static class FacadeInternals
{
    /// <summary>Returns the database file a reader or writer owns.</summary>
    /// <param name="facade">The reader or writer.</param>
    /// <exception cref="MissingMemberException">Thrown when <see cref="AccessBase"/> no longer declares the property.</exception>
    public static DatabaseFile Database(AccessBase facade)
    {
        PropertyInfo property = typeof(AccessBase).GetProperty("Database", BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new MissingMemberException(typeof(AccessBase).FullName, "Database");
        return (DatabaseFile)property.GetValue(facade)!;
    }

    /// <summary>Returns the service graph a reader built at open.</summary>
    /// <param name="reader">The reader.</param>
    public static ReaderServices Services(AccessReader reader) =>
        (ReaderServices)ReadPrivateField(reader, "services")!;

    /// <summary>Reads a private instance field declared on <paramref name="instance"/>'s type or a base type.</summary>
    /// <param name="instance">The object to read from.</param>
    /// <param name="fieldName">The field name.</param>
    /// <exception cref="MissingFieldException">Thrown when no type in <paramref name="instance"/>'s hierarchy declares the field.</exception>
    public static object? ReadPrivateField(object instance, string fieldName)
    {
        for (Type? type = instance.GetType(); type is not null; type = type.BaseType)
        {
            FieldInfo? field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            if (field is not null)
            {
                return field.GetValue(instance);
            }
        }

        throw new MissingFieldException(instance.GetType().FullName, fieldName);
    }
}
