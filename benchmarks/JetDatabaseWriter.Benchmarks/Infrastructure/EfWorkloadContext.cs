namespace JetDatabaseWriter.Benchmarks.Infrastructure;

using System.IO;
using System.Linq;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.TestSupport;
using Microsoft.EntityFrameworkCore;

/// <summary>Maps the shared EF workload data to scalar, owned and related entities.</summary>
/// <param name="options">The provider options for the workload database.</param>
internal sealed class EfWorkloadContext(DbContextOptions<EfWorkloadContext> options) : DbContext(options)
{
    public DbSet<EfWorkloadParent> Parents => this.Set<EfWorkloadParent>();

    public DbSet<EfWorkloadChild> Children => this.Set<EfWorkloadChild>();

    internal static async Task<byte[]> CreateImageAsync(int rowCount)
    {
        await using MemoryStream stream = new();
        DbContextOptions<EfWorkloadContext> options = new DbContextOptionsBuilder<EfWorkloadContext>()
            .UseJetDatabaseWriter(stream, leaveOpen: true).Options;
        await using (EfWorkloadContext schema = new(options))
        {
            _ = await schema.Database.EnsureCreatedAsync().ConfigureAwait(false);
        }

        await using (AccessWriter writer = await AccessWriter.OpenAsync(stream, leaveOpen: true).ConfigureAwait(false))
        {
            _ = await writer.InsertRowsAsync(EfWorkloadData.ParentTable, EfWorkloadData.ParentRows(rowCount).Select(values => new RowValues(values))).ConfigureAwait(false);
            _ = await writer.InsertRowsAsync(EfWorkloadData.ChildTable, EfWorkloadData.ChildRows(rowCount).Select(values => new RowValues(values))).ConfigureAwait(false);
        }

        return stream.ToArray();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        _ = modelBuilder.Entity<EfWorkloadParent>(entity =>
        {
            _ = entity.ToTable(EfWorkloadData.ParentTable);
            _ = entity.Property(parent => parent.Id).ValueGeneratedNever();
            _ = entity.Property(parent => parent.Name).HasMaxLength(40);
            _ = entity.OwnsOne(parent => parent.Detail, owned =>
                _ = owned.Property(detail => detail.Note).HasColumnName("Note").HasMaxLength(40));
            _ = entity.HasMany(parent => parent.Children).WithOne().HasForeignKey(child => child.ParentId)
                .OnDelete(DeleteBehavior.Cascade);
        });
        _ = modelBuilder.Entity<EfWorkloadChild>(entity =>
        {
            _ = entity.ToTable(EfWorkloadData.ChildTable);
            _ = entity.Property(child => child.Id).ValueGeneratedNever();
        });
    }
}
