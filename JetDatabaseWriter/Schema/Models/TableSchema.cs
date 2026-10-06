namespace JetDatabaseWriter.Schema.Models;

using System.Collections.Generic;
using JetDatabaseWriter.Catalog;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Enums;

/// <summary>One table's structural image and its persisted properties.</summary>
/// <param name="image">The immutable structural image.</param>
/// <param name="properties">The loaded persisted properties, or null.</param>
/// <param name="propertiesLoaded">Whether a property read has completed.</param>
internal sealed class TableSchema(TDefImage image, ColumnPropertyBlock? properties, bool propertiesLoaded)
{
    private TableDef? definition;

    /// <summary>Gets the structural image used to detect schema invalidation.</summary>
    internal TDefImage Image { get; } = image;

    /// <summary>Gets the persisted properties.</summary>
    internal ColumnPropertyBlock? Properties { get; } = properties;

    /// <summary>Gets a value indicating whether property absence has been verified.</summary>
    internal bool PropertiesLoaded { get; } = propertiesLoaded;

    /// <summary>Projects columns and calculated result types without altering the source image.</summary>
    /// <param name="source">The structural image.</param>
    /// <param name="properties">The persisted properties.</param>
    /// <returns>The owned layout.</returns>
    internal static TableDef CreateDefinition(TDefImage source, ColumnPropertyBlock? properties)
    {
        var columns = new List<ColumnInfo>(source.Columns.Count);
        foreach (ColumnInfo column in source.Columns)
        {
            ColumnType resultType = column.IsCalculated
                ? ColumnPropertyReader.ResolveCalculatedResultType(properties?.FindTarget(column.Name))
                : default;
            columns.Add(resultType == default ? column : column.WithCalculatedResultType(resultType));
        }

        var definition = new TableDef
        {
            Columns = columns,
            HasDeletedColumns = source.HasDeletedColumns,
        };
        return definition;
    }

    /// <summary>Returns the immutable layout shared by resolutions of this schema.</summary>
    /// <returns>The layout.</returns>
    internal TableDef CreateDefinition() => this.definition ??= this.Properties is null ? this.Image.Definition : CreateDefinition(this.Image, this.Properties);
}
