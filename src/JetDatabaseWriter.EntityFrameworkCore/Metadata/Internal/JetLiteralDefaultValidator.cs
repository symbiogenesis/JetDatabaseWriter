namespace JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Schema.Models;
using JetDatabaseWriter.ValueEncoding;
using Microsoft.EntityFrameworkCore.Metadata;

/// <summary>
/// Admits literal defaults using the same physical rounding and range checks as row storage.
/// </summary>
internal static class JetLiteralDefaultValidator
{
    /// <summary>Validates literal defaults for a model against its configured database format.</summary>
    /// <param name="model">The mapped model.</param>
    /// <param name="format">The target database format.</param>
    internal static void ValidateModel(IModel model, DatabaseFormat format)
    {
        foreach (IEntityType entityType in model.GetEntityTypes())
        {
            foreach (IProperty property in entityType.GetFlattenedDeclaredProperties())
            {
                Validate(property, format);
            }
        }
    }

    /// <summary>Validates a property's configured literal default against physical storage.</summary>
    /// <param name="property">The model property.</param>
    /// <param name="format">The target database format.</param>
    internal static void Validate(IProperty property, DatabaseFormat format)
    {
        if (JetEntityTypeMetadata.TryGetDefaultValue(property, out object? defaultValue) && defaultValue is not null)
        {
            _ = NormalizeProviderValue(property, JetEntityTypeMetadata.ConvertToProvider(property, defaultValue), format);
        }
    }

    /// <summary>Returns the rounded provider value that the physical column stores for a literal default.</summary>
    /// <param name="property">The model property.</param>
    /// <param name="providerValue">The already converted provider default.</param>
    /// <param name="format">The target database format.</param>
    /// <returns>The value after physical storage rounding.</returns>
    /// <exception cref="InvalidOperationException">The default cannot be stored by the mapped physical column.</exception>
    internal static object? NormalizeProviderValue(IProperty property, object? providerValue, DatabaseFormat format)
    {
        ColumnDefinition column = JetDatabaseCreator.CreateColumnDefinition(property, format);
        if (providerValue is null && (!property.IsNullable || !column.IsNullable))
        {
            throw new InvalidOperationException(
                $"The literal default for required property '{property.DeclaringType.DisplayName()}.{property.Name}' converts to a null storage value.");
        }

        if (providerValue is string { Length: 0 } && !column.AllowZeroLength)
        {
            throw new InvalidOperationException(
                $"The literal default for property '{property.DeclaringType.DisplayName()}.{property.Name}' is empty text but its physical column refuses zero-length values.");
        }

        try
        {
            return providerValue switch
            {
                decimal number when column.IsCurrency || format == DatabaseFormat.Jet3Mdb => decimal.FromOACurrency(decimal.ToOACurrency(number)),
                decimal number => NumericEncoder.NormalizeStoredValue(
                    new ColumnInfo { NumericPrecision = column.NumericPrecision, NumericScale = column.NumericScale }, number),
                DateTime date when !column.IsDateTimeExtended => DateTime.FromOADate(date.ToOADate()),
                _ => providerValue,
            };
        }
        catch (Exception exception) when (exception is ArgumentException or OverflowException or JetLimitationException)
        {
            throw new InvalidOperationException(
                $"The literal default for property '{property.DeclaringType.DisplayName()}.{property.Name}' cannot be represented by its physical column.",
                exception);
        }
    }
}
