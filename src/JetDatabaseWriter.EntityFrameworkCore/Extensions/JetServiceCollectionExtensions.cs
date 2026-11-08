namespace JetDatabaseWriter.EntityFrameworkCore.Extensions;

using System;
using JetDatabaseWriter.EntityFrameworkCore.Diagnostics;
using JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Query.Internal;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.EntityFrameworkCore.ValueGeneration.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for configuring <see cref="IServiceCollection"/> to use the <see cref="JetDatabaseWriter"/> Entity Framework Core provider.
/// </summary>
public static class JetServiceCollectionExtensions
{
    /// <summary>
    /// Adds the services required by the <see cref="JetDatabaseWriter"/> database provider for Entity Framework Core
    /// to an <see cref="IServiceCollection"/>.
    /// </summary>
    /// <param name="serviceCollection">The <see cref="IServiceCollection"/> to add services to.</param>
    /// <returns>The same service collection so that multiple calls can be chained.</returns>
    public static IServiceCollection AddEntityFrameworkJetDatabaseWriter(this IServiceCollection serviceCollection)
    {
        ArgumentNullException.ThrowIfNull(serviceCollection);

        EntityFrameworkServicesBuilder builder = new(serviceCollection);

        _ = builder
            .TryAdd<LoggingDefinitions, JetLoggingDefinitions>()
            .TryAdd<IDatabaseProvider, DatabaseProvider<JetOptionsExtension>>()
            .TryAdd<ITypeMappingSource, JetTypeMappingSource>()
            .TryAdd<IValueGeneratorSelector, JetValueGeneratorSelector>()
            .TryAdd<IDatabase, JetDatabase>()
            .TryAdd<IDbContextTransactionManager, JetTransactionManager>()
            .TryAdd<IDatabaseCreator, JetDatabaseCreator>()
            .TryAdd<IQueryContextFactory, JetQueryContextFactory>()
            .TryAdd<IQueryableMethodTranslatingExpressionVisitorFactory, JetQueryableMethodTranslatingExpressionVisitorFactory>()
            .TryAdd<IShapedQueryCompilingExpressionVisitorFactory, JetShapedQueryCompilingExpressionVisitorFactory>()
            .TryAdd<IModelValidator, JetModelValidator>()
            .TryAddProviderSpecificServices(static services => services.TryAddScoped<IJetClientConnection, JetClientConnection>());

        _ = builder.TryAddCoreServices();

        return serviceCollection;
    }
}
