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
    private const string DefaultValueAnnotation = "Jet:DefaultValue";
    private const string DefaultValueExpressionAnnotation = "Jet:DefaultValueExpression";
    private const string CalculatedExpressionAnnotation = "Jet:CalculatedExpression";
    private const string ValidationRuleAnnotation = "Jet:ValidationRule";
    private const string ValidationTextAnnotation = "Jet:ValidationText";
    private const string DescriptionAnnotation = "Jet:Description";
    private const string AllowZeroLengthAnnotation = "Jet:AllowZeroLength";
    private const string IsCurrencyAnnotation = "Jet:IsCurrency";
    private const string IsDateTimeExtendedAnnotation = "Jet:IsDateTimeExtended";

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

    /// <summary>
    /// Configures the default value for the Access column that the property maps to.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="value">The default value.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetDefaultValue(this PropertyBuilder propertyBuilder, object? value)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(DefaultValueAnnotation, value);
        _ = propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the default value for the Access column that the property maps to.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="value">The default value.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetDefaultValue<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, object? value)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(DefaultValueAnnotation, value);
        _ = propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the persisted Access default-value expression (e.g., <c>"=Now()"</c> or <c>"10 + 5"</c>) for the column.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="expression">The Access default-value expression.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetDefaultValueExpression(this PropertyBuilder propertyBuilder, string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.Metadata.SetAnnotation(DefaultValueExpressionAnnotation, expression);
        _ = propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the persisted Access default-value expression (e.g., <c>"=Now()"</c> or <c>"10 + 5"</c>) for the column.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="expression">The Access default-value expression.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetDefaultValueExpression<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.Metadata.SetAnnotation(DefaultValueExpressionAnnotation, expression);
        _ = propertyBuilder.ValueGeneratedOnAdd();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property as an Access 2010+ calculated column evaluated from <paramref name="expression"/>.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="expression">The Access calculated-column expression (e.g., <c>"[Quantity] * [UnitPrice]"</c>).</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetCalculatedExpression(this PropertyBuilder propertyBuilder, string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.Metadata.SetAnnotation(CalculatedExpressionAnnotation, expression);
        _ = propertyBuilder.ValueGeneratedOnAddOrUpdate();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the property as an Access 2010+ calculated column evaluated from <paramref name="expression"/>.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="expression">The Access calculated-column expression (e.g., <c>"[Quantity] * [UnitPrice]"</c>).</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetCalculatedExpression<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, string expression)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);

        propertyBuilder.Metadata.SetAnnotation(CalculatedExpressionAnnotation, expression);
        _ = propertyBuilder.ValueGeneratedOnAddOrUpdate();
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a persisted Access validation rule and optional validation message for the column.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="ruleExpression">The Access validation rule expression (e.g., <c>"&gt;= 0"</c>).</param>
    /// <param name="validationText">Optional validation message when the rule rejects a value.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetValidationRule(
        this PropertyBuilder propertyBuilder,
        string ruleExpression,
        string? validationText = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleExpression);

        propertyBuilder.Metadata.SetAnnotation(ValidationRuleAnnotation, ruleExpression);
        if (!string.IsNullOrWhiteSpace(validationText))
        {
            propertyBuilder.Metadata.SetAnnotation(ValidationTextAnnotation, validationText);
        }

        return propertyBuilder;
    }

    /// <summary>
    /// Configures a persisted Access validation rule and optional validation message for the column.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="ruleExpression">The Access validation rule expression (e.g., <c>"&gt;= 0"</c>).</param>
    /// <param name="validationText">Optional validation message when the rule rejects a value.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetValidationRule<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string ruleExpression,
        string? validationText = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(ruleExpression);

        propertyBuilder.Metadata.SetAnnotation(ValidationRuleAnnotation, ruleExpression);
        if (!string.IsNullOrWhiteSpace(validationText))
        {
            propertyBuilder.Metadata.SetAnnotation(ValidationTextAnnotation, validationText);
        }

        return propertyBuilder;
    }

    /// <summary>
    /// Configures the Access Design View description for the column.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="description">The column description.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetDescription(this PropertyBuilder propertyBuilder, string description)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);

        propertyBuilder.Metadata.SetAnnotation(DescriptionAnnotation, description);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures the Access Design View description for the column.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="description">The column description.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetDescription<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, string description)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(description);

        propertyBuilder.Metadata.SetAnnotation(DescriptionAnnotation, description);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures whether a Text, Memo, or Hyperlink column accepts zero-length (<c>""</c>) strings.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="allowZeroLength"><see langword="true"/> to allow empty strings; <see langword="false"/> to reject them.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasJetAllowZeroLength(this PropertyBuilder propertyBuilder, bool allowZeroLength = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(AllowZeroLengthAnnotation, allowZeroLength);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures whether a Text, Memo, or Hyperlink column accepts zero-length (<c>""</c>) strings.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="allowZeroLength"><see langword="true"/> to allow empty strings; <see langword="false"/> to reject them.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasJetAllowZeroLength<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, bool allowZeroLength = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(AllowZeroLengthAnnotation, allowZeroLength);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a <see cref="decimal"/> property to be stored as an Access <c>Currency</c> column.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="isCurrency"><see langword="true"/> to use Access <c>Currency</c>; <see langword="false"/> to use <c>Decimal</c>.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder UseJetCurrency(this PropertyBuilder propertyBuilder, bool isCurrency = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(IsCurrencyAnnotation, isCurrency);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a <see cref="decimal"/> property to be stored as an Access <c>Currency</c> column.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="isCurrency"><see langword="true"/> to use Access <c>Currency</c>; <see langword="false"/> to use <c>Decimal</c>.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> UseJetCurrency<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, bool isCurrency = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(IsCurrencyAnnotation, isCurrency);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a <see cref="DateTime"/> property to be stored as an Access 2019+ <c>Date/Time Extended</c> column on ACCDB databases.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="isDateTimeExtended"><see langword="true"/> to use <c>Date/Time Extended</c>; <see langword="false"/> to use classic <c>Date/Time</c>.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder UseJetDateTimeExtended(this PropertyBuilder propertyBuilder, bool isDateTimeExtended = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(IsDateTimeExtendedAnnotation, isDateTimeExtended);
        return propertyBuilder;
    }

    /// <summary>
    /// Configures a <see cref="DateTime"/> property to be stored as an Access 2019+ <c>Date/Time Extended</c> column on ACCDB databases.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="isDateTimeExtended"><see langword="true"/> to use <c>Date/Time Extended</c>; <see langword="false"/> to use classic <c>Date/Time</c>.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> UseJetDateTimeExtended<TProperty>(this PropertyBuilder<TProperty> propertyBuilder, bool isDateTimeExtended = true)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation(IsDateTimeExtendedAnnotation, isDateTimeExtended);
        return propertyBuilder;
    }
}
