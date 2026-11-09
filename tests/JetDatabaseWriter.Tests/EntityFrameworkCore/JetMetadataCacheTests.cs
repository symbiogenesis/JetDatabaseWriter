namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System.ComponentModel;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Metadata.Internal;
using JetDatabaseWriter.Enums;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Xunit;

#pragma warning disable EF1001 // These tests inspect provider metadata cache behavior directly.
public sealed class JetMetadataCacheTests
{
    [Fact]
    public void FinalizedModel_ReusesHierarchyAndOwnedMetadata()
    {
        using MemoryStream stream = new();
        DbContextOptions<MetadataContext> options = new DbContextOptionsBuilder<MetadataContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true).Options;
        using MetadataContext context = new(options);
        IEntityType type = context.Model.FindEntityType(typeof(MetadataRow))!;
        Assert.Same(JetEntityTypeMetadata.GetHierarchyProperties(type), JetEntityTypeMetadata.GetHierarchyProperties(type));
        Assert.Same(JetEntityTypeMetadata.GetTableSplitOwnedEntityTypes(type), JetEntityTypeMetadata.GetTableSplitOwnedEntityTypes(type));
        Assert.Single(JetEntityTypeMetadata.GetTableSplitOwnedEntityTypes(type));
        Assert.Equal("AttributeTable", JetEntityTypeMetadata.GetTableName(type));
        IProperty property = type.FindProperty(nameof(MetadataRow.Name))!;
        Assert.Equal("AttributeColumn", JetEntityTypeMetadata.GetColumnName(property));
        Assert.True(JetEntityTypeMetadata.TryGetDefaultValue(property, out object? value));
        Assert.Equal("attribute-default", value);
    }

    [Fact]
    public void MutableModel_ObservesPropertyAndAnnotationChanges()
    {
        ModelBuilder builder = new();
        EntityTypeBuilder<MetadataRow> entity = builder.Entity<MetadataRow>();
        _ = entity.HasKey(x => x.Id);
        IMutableEntityType type = entity.Metadata;
        Assert.Equal("AttributeTable", JetEntityTypeMetadata.GetTableName(type));
        _ = entity.ToTable("OverrideTable");
        Assert.Equal("OverrideTable", JetEntityTypeMetadata.GetTableName(type));
        PropertyBuilder<string> property = entity.Property(x => x.Name);
        Assert.Equal("AttributeColumn", JetEntityTypeMetadata.GetColumnName(property.Metadata));
        _ = property.HasColumnName("OverrideColumn");
        Assert.Equal("OverrideColumn", JetEntityTypeMetadata.GetColumnName(property.Metadata));
        Assert.True(JetEntityTypeMetadata.TryGetDefaultValue(property.Metadata, out object? initial));
        Assert.Equal("attribute-default", initial);
        _ = property.HasDefaultValue("override-default");
        Assert.True(JetEntityTypeMetadata.TryGetDefaultValue(property.Metadata, out object? updated));
        Assert.Equal("override-default", updated);
    }

    private sealed class MetadataContext(DbContextOptions<MetadataContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            _ = modelBuilder.Entity<MetadataRow>().OwnsOne(x => x.Details);
    }

    [Table("AttributeTable")]
#pragma warning disable CA1812 // EF constructs instances through its materializer.
    private sealed class MetadataRow
    {
        public int Id { get; set; }

        [Column("AttributeColumn")]
        [DefaultValue("attribute-default")]
        public string Name { get; set; } = string.Empty;

        public Details Details { get; set; } = new();
    }
#pragma warning restore CA1812

    private sealed class Details
    {
        public int Number { get; set; }
    }
}
#pragma warning restore EF1001
