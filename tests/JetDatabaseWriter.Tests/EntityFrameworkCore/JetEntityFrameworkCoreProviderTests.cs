namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Xunit;

public sealed class JetEntityFrameworkCoreProviderTests
{
    private enum OrderStatus
    {
        /// <summary>Order is pending.</summary>
        Pending = 0,

        /// <summary>Order is processing.</summary>
        Processing = 1,

        /// <summary>Order has shipped.</summary>
        Shipped = 2,
    }

    public static TheoryData<DatabaseFormat> SupportedFormats =>
    [
        DatabaseFormat.AceAccdb,
        DatabaseFormat.Jet4Mdb,
        DatabaseFormat.Jet3Mdb,
    ];

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task EnsureCreated_CanConnect_And_EnsureDeleted_Work_For_File_And_Stream(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string tempDir = Path.Combine(Path.GetTempPath(), $"jdw-efcore-{Guid.NewGuid():N}");
        string dbPath = Path.Combine(tempDir, format == DatabaseFormat.AceAccdb ? "store.accdb" : "store.mdb");

        try
        {
            DbContextOptions<StoreDbContext> fileOptions = new DbContextOptionsBuilder<StoreDbContext>()
                .UseJetDatabaseWriter(dbPath, format)
                .Options;

            await using (StoreDbContext context = new(fileOptions))
            {
                Assert.False(await context.Database.CanConnectAsync(ct));
                Assert.True(await context.Database.EnsureCreatedAsync(ct));
                Assert.True(await context.Database.CanConnectAsync(ct));
                Assert.False(await context.Database.EnsureCreatedAsync(ct));
                Assert.True(await context.Database.EnsureDeletedAsync(ct));
                Assert.False(await context.Database.CanConnectAsync(ct));
                Assert.False(await context.Database.EnsureDeletedAsync(ct));
            }

            await using MemoryStream memoryStream = new();
            DbContextOptions<StoreDbContext> streamOptions = new DbContextOptionsBuilder<StoreDbContext>()
                .UseJetDatabaseWriter(memoryStream, format, leaveOpen: true)
                .Options;

            await using (StoreDbContext streamContext = new(streamOptions))
            {
                Assert.False(await streamContext.Database.CanConnectAsync(ct));
                Assert.True(await streamContext.Database.EnsureCreatedAsync(ct));
                Assert.True(await streamContext.Database.CanConnectAsync(ct));
                Assert.True(memoryStream.Length >= 4096);
                Assert.True(await streamContext.Database.EnsureDeletedAsync(ct));
                Assert.Equal(0, memoryStream.Length);
            }
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task Crud_AutoNumber_ForeignKeys_And_Concurrency_Work_Across_Formats(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using (StoreDbContext setupContext = new(options))
        {
            Assert.True(await setupContext.Database.EnsureCreatedAsync(ct));

            Customer alice = new()
            {
                Code = "CUST-001",
                Name = "Alice Smith",
                IsVip = true,
                CreditLimit = 2500.50m,
                RowVersion = 1,
                Notes = new string('A', 400),
            };
            Customer bob = new()
            {
                Code = "CUST-002",
                Name = "Bob Jones",
                IsVip = false,
                CreditLimit = 750.25m,
                RowVersion = 1,
            };

            Order order1 = new()
            {
                Customer = alice,
                OrderDate = new DateTime(2026, 3, 15, 10, 30, 0, DateTimeKind.Utc),
                TotalAmount = 120.75m,
                Status = OrderStatus.Processing,
                TrackingId = Guid.NewGuid(),
                Payload = [1, 2, 3, 4],
            };
            Order order2 = new()
            {
                Customer = alice,
                OrderDate = new DateTime(2026, 3, 16, 14, 0, 0, DateTimeKind.Utc),
                TotalAmount = 89.25m,
                Status = OrderStatus.Shipped,
                TrackingId = Guid.NewGuid(),
                Payload = [5, 6, 7],
            };

            setupContext.Customers.AddRange(alice, bob);
            setupContext.Orders.AddRange(order1, order2);
            int inserted = await setupContext.SaveChangesAsync(ct);

            Assert.Equal(4, inserted);
            Assert.True(alice.Id > 0);
            Assert.True(bob.Id > alice.Id);
            Assert.Equal(alice.Id, order1.CustomerId);
            Assert.Equal(alice.Id, order2.CustomerId);
            Assert.True(order1.Id > 0);
            Assert.True(order2.Id > order1.Id);
        }

        await using (StoreDbContext updateContext = new(options))
        {
            Customer? bob = await updateContext.Customers.SingleAsync(c => c.Code == "CUST-002", ct);
            bob.CreditLimit = 1500.00m;
            bob.RowVersion = 2;
            int updated = await updateContext.SaveChangesAsync(ct);
            Assert.Equal(1, updated);
        }

        await using (StoreDbContext verifyContext = new(options))
        {
            Customer bob = await verifyContext.Customers.AsNoTracking().SingleAsync(c => c.Code == "CUST-002", ct);
            Assert.Equal(1500.00m, bob.CreditLimit);
            Assert.Equal("Bob Jones", bob.Name);
            Assert.Equal(2, bob.RowVersion);
        }

        await using (StoreDbContext staleContext1 = new(options))
        await using (StoreDbContext staleContext2 = new(options))
        {
            Customer c1 = await staleContext1.Customers.SingleAsync(c => c.Code == "CUST-002", ct);
            Customer c2 = await staleContext2.Customers.SingleAsync(c => c.Code == "CUST-002", ct);

            c1.Name = "Bob Updated First";
            c1.RowVersion = 3;
            _ = await staleContext1.SaveChangesAsync(ct);

            c2.Name = "Bob Stale Update";
            c2.RowVersion = 4;
            _ = await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => staleContext2.SaveChangesAsync(ct));
        }

        await using (StoreDbContext deleteContext = new(options))
        {
            Customer bob = await deleteContext.Customers.SingleAsync(c => c.Code == "CUST-002", ct);
            deleteContext.Customers.Remove(bob);
            int deleted = await deleteContext.SaveChangesAsync(ct);
            Assert.Equal(1, deleted);
            Assert.Equal(1, await deleteContext.Customers.CountAsync(ct));
        }
    }

    [Fact]
    public async Task Linq_Queries_IndexSeeks_Aggregates_Projections_Includes_And_Filters_Work()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (StoreDbContext seed = new(options))
        {
            _ = await seed.Database.EnsureCreatedAsync(ct);

            Customer c1 = new()
            {
                Code = "C-100",
                Name = "Delta Corp",
                IsVip = true,
                CreditLimit = 5000m,
                RowVersion = 1,
                IsDeleted = false,
            };
            Customer c2 = new()
            {
                Code = "C-200",
                Name = "Alpha LLC",
                IsVip = false,
                CreditLimit = 1000m,
                RowVersion = 1,
                IsDeleted = false,
            };
            Customer c3 = new()
            {
                Code = "C-300",
                Name = "Archived Co",
                IsVip = false,
                CreditLimit = 200m,
                RowVersion = 1,
                IsDeleted = true,
            };

            Order o1 = new()
            {
                Customer = c1,
                OrderDate = new DateTime(2026, 1, 10, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 300m,
                Status = OrderStatus.Shipped,
                TrackingId = Guid.NewGuid(),
            };
            Order o2 = new()
            {
                Customer = c1,
                OrderDate = new DateTime(2026, 2, 10, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 700m,
                Status = OrderStatus.Pending,
                TrackingId = Guid.NewGuid(),
            };
            Order o3 = new()
            {
                Customer = c2,
                OrderDate = new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 150m,
                Status = OrderStatus.Processing,
                TrackingId = Guid.NewGuid(),
            };

            seed.Customers.AddRange(c1, c2, c3);
            seed.Orders.AddRange(o1, o2, o3);
            _ = await seed.SaveChangesAsync(ct);
        }

        await using (StoreDbContext queryContext = new(options))
        {
            List<Customer> visibleCustomers = await queryContext.Customers
                .OrderBy(c => c.Name)
                .ToListAsync(ct);
            Assert.Equal(2, visibleCustomers.Count);
            Assert.Equal("Alpha LLC", visibleCustomers[0].Name);
            Assert.Equal("Delta Corp", visibleCustomers[1].Name);

            List<Customer> allCustomers = await queryContext.Customers
                .IgnoreQueryFilters()
                .OrderBy(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(3, allCustomers.Count);

            string[] lookupCodes = ["C-100", "C-200"];
            string targetCode = lookupCodes[0];
            Customer? byIndexedCode = await queryContext.Customers
                .FirstOrDefaultAsync(c => c.Code == targetCode, ct);
            Assert.NotNull(byIndexedCode);
            Assert.Equal("Delta Corp", byIndexedCode.Name);

            Customer? foundById = await queryContext.Customers.FindAsync([byIndexedCode.Id], ct);
            Assert.Same(byIndexedCode, foundById);

            string efPropCode = lookupCodes[1];
            Customer byEfProperty = await queryContext.Customers
                .SingleAsync(c => EF.Property<string>(c, nameof(Customer.Code)) == efPropCode, ct);
            Assert.Equal("Alpha LLC", byEfProperty.Name);

            Assert.Equal(3, await queryContext.Orders.CountAsync(ct));
            Assert.Equal(3L, await queryContext.Orders.LongCountAsync(ct));
            Assert.True(await queryContext.Orders.AnyAsync(o => o.Status == OrderStatus.Shipped, ct));
            Assert.True(await queryContext.Orders.AllAsync(o => o.TotalAmount > 0m, ct));
            Assert.Equal(150m, await queryContext.Orders.MinAsync(o => o.TotalAmount, ct));
            Assert.Equal(700m, await queryContext.Orders.MaxAsync(o => o.TotalAmount, ct));
            Assert.Equal(1150m, await queryContext.Orders.SumAsync(o => o.TotalAmount, ct));

            List<string> pagedCodes = await queryContext.Customers
                .IgnoreQueryFilters()
                .OrderBy(c => c.Code)
                .Skip(1)
                .Take(2)
                .Select(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["C-200", "C-300"], pagedCodes);
        }

        await using (StoreDbContext includeContext = new(options))
        {
            Customer deltaWithOrders = await includeContext.Customers
                .Include(c => c.Orders)
                .SingleAsync(c => c.Code == "C-100", ct);
            Assert.Equal(2, deltaWithOrders.Orders.Count);
            Assert.All(deltaWithOrders.Orders, o => Assert.Same(deltaWithOrders, o.Customer));

            List<Order> untrackedOrders = await includeContext.Orders
                .AsNoTrackingWithIdentityResolution()
                .Include(o => o.Customer)
                .OrderBy(o => o.Id)
                .ToListAsync(ct);
            Assert.Equal(3, untrackedOrders.Count);
            Assert.NotNull(untrackedOrders[0].Customer);
            Assert.Same(untrackedOrders[0].Customer, untrackedOrders[1].Customer);
        }
    }

    [Fact]
    public async Task Explicit_Transactions_Support_Uncommitted_Reads_Rollback_And_Commit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (StoreDbContext context = new(options))
        {
            _ = await context.Database.EnsureCreatedAsync(ct);

            await using (IDbContextTransaction tx = await context.Database.BeginTransactionAsync(ct))
            {
                context.Customers.Add(new Customer
                {
                    Code = "TX-ROLLBACK",
                    Name = "Transient Customer",
                    CreditLimit = 100m,
                    RowVersion = 1,
                });
                _ = await context.SaveChangesAsync(ct);

                Customer? visibleInsideTx = await context.Customers
                    .AsNoTracking()
                    .SingleOrDefaultAsync(c => c.Code == "TX-ROLLBACK", ct);
                Assert.NotNull(visibleInsideTx);

                await tx.RollbackAsync(ct);
            }

            context.ChangeTracker.Clear();
            Assert.Equal(0, await context.Customers.CountAsync(ct));

            await using (IDbContextTransaction tx2 = await context.Database.BeginTransactionAsync(ct))
            {
                context.Customers.Add(new Customer
                {
                    Code = "TX-COMMIT",
                    Name = "Committed Customer",
                    CreditLimit = 900m,
                    RowVersion = 1,
                });
                _ = await context.SaveChangesAsync(ct);
                await tx2.CommitAsync(ct);
            }

            context.ChangeTracker.Clear();
            Customer committed = await context.Customers.SingleAsync(ct);
            Assert.Equal("TX-COMMIT", committed.Code);
        }
    }

    [Fact]
    public async Task Encrypted_Database_With_Password_And_Temporal_Types_RoundTrip()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string tempDir = Path.Combine(Path.GetTempPath(), $"jdw-efcore-enc-{Guid.NewGuid():N}");
        string dbPath = Path.Combine(tempDir, "encrypted.mdb");

        try
        {
            DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
                .UseJetDatabaseWriter(
                    dbPath,
                    DatabaseFormat.Jet4Mdb,
                    jet => jet.WithPassword("SecretPass123"))
                .Options;

            await using (StoreDbContext context = new(options))
            {
                Assert.True(await context.Database.EnsureCreatedAsync(ct));

                ScheduleItem item = new()
                {
                    Title = "Morning Shift",
                    ScheduledDate = new DateOnly(2026, 7, 4),
                    StartTime = new TimeOnly(8, 15, 30),
                    Duration = TimeSpan.FromMinutes(90),
                    Website = new Hyperlink("Example", "https://example.com"),
                };
                context.ScheduleItems.Add(item);
                _ = await context.SaveChangesAsync(ct);
            }

            await using (StoreDbContext readContext = new(options))
            {
                ScheduleItem loaded = await readContext.ScheduleItems.AsNoTracking().SingleAsync(ct);
                Assert.Equal("Morning Shift", loaded.Title);
                Assert.Equal(new DateOnly(2026, 7, 4), loaded.ScheduledDate);
                Assert.Equal(new TimeOnly(8, 15, 30), loaded.StartTime);
                Assert.Equal(TimeSpan.FromMinutes(90), loaded.Duration);
                Assert.NotNull(loaded.Website);
                Assert.Equal("https://example.com", loaded.Website.Address);
            }
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task Synchronous_Methods_Throw_NotSupportedException()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using StoreDbContext context = new(options);
        _ = await context.Database.EnsureCreatedAsync(ct);

        _ = Assert.Throws<NotSupportedException>(() => _ = context.Database.EnsureCreated());
        _ = Assert.Throws<NotSupportedException>(() => _ = context.Database.EnsureDeleted());
        _ = Assert.Throws<NotSupportedException>(() => _ = context.Database.CanConnect());
        _ = Assert.Throws<NotSupportedException>(() => _ = context.Database.BeginTransaction());

        context.Customers.Add(new Customer
        {
            Code = "SYNC-1",
            Name = "Sync Test",
            CreditLimit = 10m,
            RowVersion = 1,
        });
        _ = Assert.Throws<NotSupportedException>(() => _ = context.SaveChanges());
        _ = Assert.Throws<NotSupportedException>(() => _ = context.Customers.ToList());
    }

    private sealed class StoreDbContext(DbContextOptions<StoreDbContext> options) : DbContext(options)
    {
        public DbSet<Customer> Customers => this.Set<Customer>();

        public DbSet<Order> Orders => this.Set<Order>();

        public DbSet<ScheduleItem> ScheduleItems => this.Set<ScheduleItem>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<Customer>(entity =>
            {
                _ = entity.ToTable("Customers");
                _ = entity.HasKey(c => c.Id);
                _ = entity.HasIndex(c => c.Code).IsUnique();
                _ = entity.Property(c => c.Code).HasColumnName("CustomerCode").HasMaxLength(32).IsRequired();
                _ = entity.Property(c => c.Name).HasMaxLength(100).IsRequired();
                _ = entity.Property(c => c.Notes).HasMaxLength(1000);
                _ = entity.Property(c => c.CreditLimit).HasPrecision(18, 4);
                _ = entity.Property(c => c.RowVersion).IsConcurrencyToken();
                _ = entity.HasQueryFilter(c => !c.IsDeleted);
            });

            _ = modelBuilder.Entity<Order>(entity =>
            {
                _ = entity.ToTable("Orders");
                _ = entity.HasKey(o => o.Id);
                _ = entity.HasIndex(o => o.CustomerId);
                _ = entity.Property(o => o.TotalAmount).HasPrecision(18, 4);
                _ = entity.HasOne(o => o.Customer)
                    .WithMany(c => c.Orders)
                    .HasForeignKey(o => o.CustomerId)
                    .OnDelete(DeleteBehavior.Cascade);
            });

            _ = modelBuilder.Entity<ScheduleItem>(entity =>
            {
                _ = entity.ToTable("ScheduleItems");
                _ = entity.HasKey(s => s.Id);
                _ = entity.Property(s => s.Title).HasMaxLength(120).IsRequired();
            });
        }
    }

    [Table("Customers")]
    private sealed class Customer
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [MaxLength(32)]
        public string Code { get; set; } = string.Empty;

        [Required]
        [MaxLength(100)]
        public string Name { get; set; } = string.Empty;

        public bool IsVip { get; set; }

        public decimal CreditLimit { get; set; }

        [ConcurrencyCheck]
        public int RowVersion { get; set; }

        public string? Notes { get; set; }

        public bool IsDeleted { get; set; }

        public List<Order> Orders { get; set; } = [];
    }

    [Table("Orders")]
    private sealed class Order
    {
        [Key]
        public int Id { get; set; }

        public int CustomerId { get; set; }

        public DateTime OrderDate { get; set; }

        public decimal TotalAmount { get; set; }

        public OrderStatus Status { get; set; }

        public Guid TrackingId { get; set; }

        public byte[]? Payload { get; set; }

        public Customer? Customer { get; set; }
    }

    [Table("ScheduleItems")]
    private sealed class ScheduleItem
    {
        [Key]
        public int Id { get; set; }

        public string Title { get; set; } = string.Empty;

        public DateOnly ScheduledDate { get; set; }

        public TimeOnly StartTime { get; set; }

        public TimeSpan Duration { get; set; }

        public Hyperlink? Website { get; set; }
    }
}
