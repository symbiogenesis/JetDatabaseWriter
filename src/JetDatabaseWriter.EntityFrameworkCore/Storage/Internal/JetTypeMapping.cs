namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

/// <summary>
/// Represents a scalar or binary type mapping for the <see cref="JetDatabaseWriter"/> Entity Framework Core provider.
/// </summary>
public sealed class JetTypeMapping : CoreTypeMapping
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JetTypeMapping"/> class.
    /// </summary>
    /// <param name="clrType">The CLR type used in the EF Core model.</param>
    /// <param name="comparer">Optional custom value comparer.</param>
    /// <param name="keyComparer">Optional custom key comparer.</param>
    public JetTypeMapping(
        Type clrType,
        ValueComparer? comparer = null,
        ValueComparer? keyComparer = null)
        : base(new CoreTypeMappingParameters(clrType, converter: null, comparer, keyComparer))
    {
    }

    private JetTypeMapping(CoreTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    /// <inheritdoc/>
    public override CoreTypeMapping WithComposedConverter(
        ValueConverter? converter,
        ValueComparer? comparer = null,
        ValueComparer? keyComparer = null,
        CoreTypeMapping? elementMapping = null,
        JsonValueReaderWriter? jsonValueReaderWriter = null)
        => new JetTypeMapping(this.Parameters.WithComposedConverter(converter, comparer, keyComparer, elementMapping, jsonValueReaderWriter));

    /// <inheritdoc/>
    protected override CoreTypeMapping Clone(CoreTypeMappingParameters parameters)
        => new JetTypeMapping(parameters);
}
