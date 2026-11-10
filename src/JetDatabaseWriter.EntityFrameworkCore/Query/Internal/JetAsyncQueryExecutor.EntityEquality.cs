namespace JetDatabaseWriter.EntityFrameworkCore.Query.Internal;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

internal static partial class JetAsyncQueryExecutor
{
    private static bool EvaluateEntityEquality(
        QueryContext context,
        JetExecutionMaterializedState state,
        IKey key,
        object? left,
        object? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        IEntityType rootType = key.DeclaringEntityType.GetRootType();
        if (!BelongsToHierarchy(left) || !BelongsToHierarchy(right))
        {
            return false;
        }

        foreach (IProperty property in key.Properties)
        {
            object? leftValue = ReadKeyValue(left, property);
            object? rightValue = ReadKeyValue(right, property);
            if (!property.GetKeyValueComparer().Equals(leftValue, rightValue))
            {
                return false;
            }
        }

        return true;

        bool BelongsToHierarchy(object entity)
            => (state.EntityTypesByInstance.TryGetValue(entity, out IEntityType? entityType)
                ? entityType
                : context.Context.Model.FindRuntimeEntityType(entity.GetType()))?.GetRootType() == rootType;

        object? ReadKeyValue(object entity, IProperty property)
        {
            if (state.EntityBuffers.TryGetValue(entity, out ValueBuffer buffer))
            {
                return buffer[property.GetIndex()];
            }

            return property.IsShadowProperty()
                ? context.Context.Entry(entity).Property(property.Name).CurrentValue
                : property.GetGetter().GetClrValueUsingContainingEntity(entity);
        }
    }
}
