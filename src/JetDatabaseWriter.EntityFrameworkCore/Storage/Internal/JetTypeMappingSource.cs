namespace JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

/// <summary>
/// Resolves <see cref="CoreTypeMapping"/> instances for CLR types supported by <see cref="JetDatabaseWriter"/>.
/// </summary>
/// <param name="dependencies">Parameter object containing dependencies for this service.</param>
public sealed class JetTypeMappingSource(TypeMappingSourceDependencies dependencies) : TypeMappingSource(dependencies)
{
    private static readonly HashSet<Type> SupportedScalarTypes =
    [
        typeof(bool),
        typeof(byte),
        typeof(sbyte),
        typeof(short),
        typeof(ushort),
        typeof(int),
        typeof(uint),
        typeof(long),
        typeof(ulong),
        typeof(float),
        typeof(double),
        typeof(decimal),
        typeof(char),
        typeof(string),
        typeof(DateTime),
        typeof(DateTimeOffset),
        typeof(DateOnly),
        typeof(TimeOnly),
        typeof(TimeSpan),
        typeof(Guid),
        typeof(Hyperlink),
    ];

    private readonly ConcurrentDictionary<Type, CoreTypeMapping> mappings = new();

    /// <inheritdoc/>
    protected override CoreTypeMapping? FindMapping(in TypeMappingInfo mappingInfo)
    {
        Type? clrType = mappingInfo.ClrType;
        if (clrType is null)
        {
            return base.FindMapping(mappingInfo);
        }

        Type unwrappedType = Nullable.GetUnderlyingType(clrType) ?? clrType;
        if (unwrappedType == typeof(byte[]))
        {
            return this.mappings.GetOrAdd(
                clrType,
                static type => new JetTypeMapping(
                    type,
                    comparer: new ByteArrayStructuralComparer(),
                    keyComparer: new ByteArrayStructuralComparer()));
        }

        if (SupportedScalarTypes.Contains(unwrappedType) || unwrappedType.IsEnum)
        {
            return this.mappings.GetOrAdd(clrType, static type => new JetTypeMapping(type));
        }

        return base.FindMapping(mappingInfo);
    }

    private sealed class ByteArrayStructuralComparer : ValueComparer<byte[]>
    {
        public ByteArrayStructuralComparer()
            : base(
                static (left, right) => AreEqual(left, right),
                static bytes => ComputeHashCode(bytes),
                static bytes => (byte[])bytes.Clone())
        {
        }

        private static bool AreEqual(byte[]? left, byte[]? right)
            => ReferenceEquals(left, right) || (left is not null && right is not null && left.AsSpan().SequenceEqual(right));

        private static int ComputeHashCode(byte[]? bytes)
        {
            if (bytes is null)
            {
                return 0;
            }

            HashCode hash = default;
            hash.AddBytes(bytes);
            return hash.ToHashCode();
        }
    }
}
