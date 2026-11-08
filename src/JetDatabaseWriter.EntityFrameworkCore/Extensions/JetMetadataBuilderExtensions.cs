namespace JetDatabaseWriter.EntityFrameworkCore.Extensions;

using System;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// <see cref="JetDatabaseWriter"/> specific extension methods for <see cref="EntityTypeBuilder"/> and <see cref="PropertyBuilder"/>.
/// </summary>
public static class JetMetadataBuilderExtensions
{
    private const string TableNameAnnotation = "Relational:TableName";
    private const string ColumnNameAnnotation = "Relational:ColumnName";

    /// <summary>
    /// Configures the Access table that the entity type maps to.
    /// </summary>
    /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
    /// <param name="name">The name of the table.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static EntityTypeBuilder ToTable(this EntityTypeBuilder entityTypeBuilder, string name)
    {
        ArgumentNullException.ThrowIfNull(entityTypeBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        entityTypeBuilder.Metadata.SetAnnotation(TableNameAnnotation, name);
        return entityTypeBuilder;
    }

    /// <summary>
    /// Configures the Access table that the entity type maps to.
    /// </summary>
    /// <typeparam name="TEntity">The entity type being configured.</typeparam>
    /// <param name="entityTypeBuilder">The builder for the entity type being configured.</param>
    /// <param name="name">The name of the table.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static EntityTypeBuilder<TEntity> ToTable<TEntity>(this EntityTypeBuilder<TEntity> entityTypeBuilder, string name)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(entityTypeBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        entityTypeBuilder.Metadata.SetAnnotation(TableNameAnnotation, name);
        return entityTypeBuilder;
    }

    /// <summary>
    /// Configures the Access column that the property maps to.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="name">The name of the column.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasColumnName(this PropertyBuilder propertyBuilder, string name)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        propertyBuilder.Metadata.SetAnnotation(ColumnNameAnnotation, name);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the Access column that the property maps to.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="name">The name of the column.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasColumnName<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, string name)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        propertyBuilder.Metadata.SetAnnotation(ColumnNameAnnotation, name);
        return propertyBuilder;
    }
}
