namespace JetDatabaseWriter.EntityFrameworkCore.Extensions;

using System;
using System.IO;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

/// <summary>
/// Extension methods on <see cref="DbContextOptionsBuilder"/> and <see cref="PropertyBuilder"/> for configuring a <see cref="DbContext"/>
/// to use the <see cref="JetDatabaseWriter"/> pure-managed Microsoft Access provider.
/// </summary>
public static class JetDbContextOptionsBuilderExtensions
{
    /// <summary>
    /// Configures the context to connect to a Microsoft Access database file (<c>.mdb</c> or <c>.accdb</c>)
    /// at <paramref name="databasePath"/> using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="databasePath">The path to the Access database file.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder UseJetDatabaseWriter(
        this DbContextOptionsBuilder optionsBuilder,
        string databasePath,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        JetOptionsExtension extension = (optionsBuilder.Options.FindExtension<JetOptionsExtension>() ?? new JetOptionsExtension())
            .WithDatabasePath(databasePath)
            .WithFormat(format);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        jetOptionsAction?.Invoke(new JetDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to connect to a Microsoft Access database file (<c>.mdb</c> or <c>.accdb</c>)
    /// at <paramref name="databasePath"/> using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <typeparam name="TContext">The type of context to be configured.</typeparam>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="databasePath">The path to the Access database file.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder<TContext> UseJetDatabaseWriter<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string databasePath,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseJetDatabaseWriter(
            (DbContextOptionsBuilder)optionsBuilder,
            databasePath,
            format,
            jetOptionsAction);

    /// <summary>
    /// Configures the context to connect to a Microsoft Access database backed by <paramref name="databaseStream"/>
    /// using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="databaseStream">A readable, writable, and seekable stream backing the Access database.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="leaveOpen">Whether to leave <paramref name="databaseStream"/> open when the context is disposed.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder UseJetDatabaseWriter(
        this DbContextOptionsBuilder optionsBuilder,
        Stream databaseStream,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        bool leaveOpen = true,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(databaseStream);

        JetOptionsExtension extension = (optionsBuilder.Options.FindExtension<JetOptionsExtension>() ?? new JetOptionsExtension())
            .WithDatabaseStream(databaseStream, leaveOpen)
            .WithFormat(format);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        jetOptionsAction?.Invoke(new JetDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to connect to a Microsoft Access database backed by <paramref name="databaseStream"/>
    /// using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <typeparam name="TContext">The type of context to be configured.</typeparam>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="databaseStream">A readable, writable, and seekable stream backing the Access database.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="leaveOpen">Whether to leave <paramref name="databaseStream"/> open when the context is disposed.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder<TContext> UseJetDatabaseWriter<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Stream databaseStream,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        bool leaveOpen = true,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseJetDatabaseWriter(
            (DbContextOptionsBuilder)optionsBuilder,
            databaseStream,
            format,
            leaveOpen,
            jetOptionsAction);

    /// <summary>
    /// Configures the context to connect to a Microsoft Access database backed by a stream created via <paramref name="streamFactory"/>
    /// using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="streamFactory">A delegate that returns the seekable stream backing the Access database.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="leaveOpen">Whether to leave the stream open when the context is disposed.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder UseJetDatabaseWriter(
        this DbContextOptionsBuilder optionsBuilder,
        Func<Stream> streamFactory,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        bool leaveOpen = false,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(streamFactory);

        JetOptionsExtension extension = (optionsBuilder.Options.FindExtension<JetOptionsExtension>() ?? new JetOptionsExtension())
            .WithStreamFactory(streamFactory, leaveOpen)
            .WithFormat(format);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);
        jetOptionsAction?.Invoke(new JetDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    /// <summary>
    /// Configures the context to connect to a Microsoft Access database backed by a stream created via <paramref name="streamFactory"/>
    /// using <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <typeparam name="TContext">The type of context to be configured.</typeparam>
    /// <param name="optionsBuilder">The builder being used to configure the context.</param>
    /// <param name="streamFactory">A delegate that returns the seekable stream backing the Access database.</param>
    /// <param name="format">The target database format used if the database is created via <c>EnsureCreatedAsync</c>.</param>
    /// <param name="leaveOpen">Whether to leave the stream open when the context is disposed.</param>
    /// <param name="jetOptionsAction">An optional action to allow additional provider-specific configuration.</param>
    /// <returns>The options builder so that further configuration can be chained.</returns>
    public static DbContextOptionsBuilder<TContext> UseJetDatabaseWriter<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Func<Stream> streamFactory,
        DatabaseFormat format = DatabaseFormat.AceAccdb,
        bool leaveOpen = false,
        Action<JetDbContextOptionsBuilder>? jetOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseJetDatabaseWriter(
            (DbContextOptionsBuilder)optionsBuilder,
            streamFactory,
            format,
            leaveOpen,
            jetOptionsAction);

    /// <summary>
    /// Configures the default value for the column that the property maps to when targeting <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="value">The default value of the column.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder HasDefaultValue(
        this PropertyBuilder propertyBuilder,
        object? value = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation("Relational:DefaultValue", value);
        return propertyBuilder.ValueGeneratedOnAdd();
    }

    /// <summary>
    /// Configures the default value for the column that the property maps to when targeting <see cref="JetDatabaseWriter"/>.
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being configured.</typeparam>
    /// <param name="propertyBuilder">The builder for the property being configured.</param>
    /// <param name="value">The default value of the column.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public static PropertyBuilder<TProperty> HasDefaultValue<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        object? value = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetAnnotation("Relational:DefaultValue", value);
        return propertyBuilder.ValueGeneratedOnAdd();
    }
}
