namespace JetDatabaseWriter.EntityFrameworkCore.Infrastructure.Internal;

using Microsoft.EntityFrameworkCore.Infrastructure;

/// <summary>
/// Validates EF Core models configured for the <see cref="JetDatabaseWriter"/> provider.
/// </summary>
/// <param name="dependencies">Parameter object containing dependencies for this service.</param>
public sealed class JetModelValidator(ModelValidatorDependencies dependencies) : ModelValidator(dependencies);
