namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.EntityFrameworkCore.ValueGeneration.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
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
            Assert.Equal(2, queryContext.ChangeTracker.Entries<Customer>().Count());

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

            List<Customer> likeMatches = await queryContext.Customers
                .Where(c => EF.Functions.Like(c.Name, "alpha%") && EF.Functions.Like(c.Code, "C-_00"))
                .ToListAsync(ct);
            Customer likeMatch = Assert.Single(likeMatches);
            Assert.Equal("C-200", likeMatch.Code);

            Assert.Equal(3, await queryContext.Orders.CountAsync(ct));
            Assert.Equal(3L, await queryContext.Orders.LongCountAsync(ct));
            Assert.True(await queryContext.Orders.AnyAsync(o => o.Status == OrderStatus.Shipped, ct));
            Assert.True(await queryContext.Orders.AllAsync(o => o.TotalAmount > 0m, ct));
            Assert.Equal(150m, await queryContext.Orders.MinAsync(o => o.TotalAmount, ct));
            Assert.Equal(700m, await queryContext.Orders.MaxAsync(o => o.TotalAmount, ct));
            Assert.Equal(1150m, await queryContext.Orders.SumAsync(o => o.TotalAmount, ct));
            Assert.Empty(queryContext.ChangeTracker.Entries<Order>());

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
            Assert.Single(includeContext.ChangeTracker.Entries<Customer>());
            Assert.Equal(2, includeContext.ChangeTracker.Entries<Order>().Count());

            List<Order> untrackedOrders = await includeContext.Orders
                .AsNoTrackingWithIdentityResolution()
                .Include(o => o.Customer)
                .OrderBy(o => o.Id)
                .ToListAsync(ct);
            Assert.Equal(3, untrackedOrders.Count);
            Assert.NotNull(untrackedOrders[0].Customer);
            Assert.Same(untrackedOrders[0].Customer, untrackedOrders[1].Customer);
        }

        await using (StoreDbContext filteredIncludeContext = new(options))
        {
            Customer deltaFiltered = await filteredIncludeContext.Customers
                .Include(c => c.Orders.Where(o => o.TotalAmount >= 500m))
                .SingleAsync(c => c.Code == "C-100", ct);
            Order filteredOrder = Assert.Single(deltaFiltered.Orders);
            Assert.Equal(700m, filteredOrder.TotalAmount);
            Assert.Single(filteredIncludeContext.ChangeTracker.Entries<Order>());
        }

        await using (StoreDbContext multiRootContext = new(options))
        {
            List<Customer> aboveAlpha = await multiRootContext.Customers
                .Where(c => c.CreditLimit > multiRootContext.Customers
                    .Where(x => x.Code == "C-200")
                    .Select(x => x.CreditLimit)
                    .FirstOrDefault())
                .ToListAsync(ct);
            Customer aboveMatch = Assert.Single(aboveAlpha);
            Assert.Equal("C-100", aboveMatch.Code);

            List<Customer> unionResult = await multiRootContext.Customers
                .Where(c => c.Code == "C-100")
                .Union(multiRootContext.Customers.Where(c => c.Code == "C-200"))
                .OrderBy(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(2, unionResult.Count);
            Assert.Equal(["C-100", "C-200"], unionResult.Select(static c => c.Code).ToArray());
        }
    }

    [Fact]
    public async Task Explicit_Transactions_Support_Uncommitted_Reads_Hyperlinks_Rollback_And_Commit()
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
                context.ScheduleItems.Add(new ScheduleItem
                {
                    Title = "Tx Hyperlink Check",
                    ScheduledDate = new DateOnly(2026, 5, 1),
                    StartTime = new TimeOnly(9, 0, 0),
                    Duration = TimeSpan.FromHours(1),
                    Website = new Hyperlink("Docs", "https://docs.example.com"),
                });
                _ = await context.SaveChangesAsync(ct);

                Customer? visibleInsideTx = await context.Customers
                    .AsNoTracking()
                    .SingleOrDefaultAsync(c => c.Code == "TX-ROLLBACK", ct);
                Assert.NotNull(visibleInsideTx);

                ScheduleItem? scheduleInsideTx = await context.ScheduleItems
                    .AsNoTracking()
                    .SingleOrDefaultAsync(s => s.Title == "Tx Hyperlink Check", ct);
                Assert.NotNull(scheduleInsideTx);
                Assert.NotNull(scheduleInsideTx.Website);
                Assert.Equal("https://docs.example.com", scheduleInsideTx.Website.Address);

                await tx.RollbackAsync(ct);
            }

            context.ChangeTracker.Clear();
            Assert.Equal(0, await context.Customers.CountAsync(ct));
            Assert.Equal(0, await context.ScheduleItems.CountAsync(ct));

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

            TimeSpan preciseDuration = TimeSpan.FromDays(400) + TimeSpan.FromTicks(1234567);

            await using (StoreDbContext context = new(options))
            {
                Assert.True(await context.Database.EnsureCreatedAsync(ct));

                ScheduleItem item = new()
                {
                    Title = "Morning Shift",
                    ScheduledDate = new DateOnly(2026, 7, 4),
                    StartTime = new TimeOnly(8, 15, 30),
                    Duration = preciseDuration,
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
                Assert.Equal(preciseDuration, loaded.Duration);
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

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task Advanced_Schema_LongAutoNumber_Defaults_Seeds_AlternateKeys_Tph_And_SaveOrdering_Work(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<AdvancedCatalogDbContext> options = new DbContextOptionsBuilder<AdvancedCatalogDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using (AdvancedCatalogDbContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));

            List<CategoryNode> seededCategories = await context.Categories.AsNoTracking().OrderBy(c => c.Id).ToListAsync(ct);
            CategoryNode seededCategory = Assert.Single(seededCategories);
            Assert.Equal("ROOT", seededCategory.Code);
            Assert.Equal("General", seededCategory.Tier);

            CategoryNode parentCat = new()
            {
                Code = "ELECTRONICS",
                Name = "Electronics",
                UnannotatedDescription = new string('D', 600),
            };
            CategoryNode childCat = new()
            {
                Code = "LAPTOPS",
                Name = "Laptops",
                Parent = parentCat,
            };

            AuditEntry audit = new()
            {
                CategoryCode = "ELECTRONICS",
                Message = "Created Electronics category",
            };

            CardPaymentMethod card = new()
            {
                AccountNumber = "ACC-01",
                CardNetwork = "Visa",
                LastFourDigits = "4242",
            };
            BankPaymentMethod bank = new()
            {
                AccountNumber = "ACC-02",
                RoutingNumber = "111000025",
                BankName = "First National",
            };

            context.Audits.Add(audit);
            context.PaymentMethods.AddRange(card, bank);
            context.Categories.AddRange(childCat, parentCat);
            _ = await context.SaveChangesAsync(ct);

            Assert.True(parentCat.Id > 1L);
            Assert.True(childCat.Id > parentCat.Id);
            Assert.Equal(parentCat.Id, childCat.ParentId);
            Assert.Equal("General", parentCat.Tier);
            Assert.True(audit.Id > 0L);
        }

        await using (AdvancedCatalogDbContext verifyContext = new(options))
        {
            CategoryNode electronics = await verifyContext.Categories
                .Include(c => c.Children)
                .SingleAsync(c => c.Code == "ELECTRONICS", ct);
            Assert.Equal(600, electronics.UnannotatedDescription?.Length);
            Assert.Equal("General", electronics.Tier);
            CategoryNode laptops = Assert.Single(electronics.Children);
            Assert.Equal("LAPTOPS", laptops.Code);

            List<PaymentMethod> methods = await verifyContext.PaymentMethods
                .OrderBy(p => p.AccountNumber)
                .ToListAsync(ct);
            Assert.Equal(2, methods.Count);
            CardPaymentMethod loadedCard = Assert.IsType<CardPaymentMethod>(methods[0]);
            Assert.Equal("Visa", loadedCard.CardNetwork);
            Assert.Equal("4242", loadedCard.LastFourDigits);
            BankPaymentMethod loadedBank = Assert.IsType<BankPaymentMethod>(methods[1]);
            Assert.Equal("111000025", loadedBank.RoutingNumber);
            Assert.Equal("First National", loadedBank.BankName);

            verifyContext.Categories.Remove(laptops);
            verifyContext.Categories.Add(new CategoryNode
            {
                Code = "LAPTOPS",
                Name = "Laptops Replaced",
                ParentId = electronics.Id,
            });
            _ = await verifyContext.SaveChangesAsync(ct);

            CategoryNode replaced = await verifyContext.Categories.AsNoTracking().SingleAsync(c => c.Code == "LAPTOPS", ct);
            Assert.Equal("Laptops Replaced", replaced.Name);
        }

        await using (AdvancedCatalogDbContext dupContext = new(options))
        {
            dupContext.Categories.Add(new CategoryNode
            {
                Code = "ELECTRONICS",
                Name = "Duplicate Code",
            });
            DbUpdateException dupEx = await Assert.ThrowsAsync<DbUpdateException>(() => dupContext.SaveChangesAsync(ct));
            _ = Assert.IsType<IJetException>(dupEx.InnerException, exactMatch: false);
        }
    }

    [Fact]
    public async Task Synchronous_Dispose_And_Concurrent_File_DbContexts_Release_Reader_Locks()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string tempDir = Path.Combine(Path.GetTempPath(), $"jdw-efcore-sync-{Guid.NewGuid():N}");
        string dbPath = Path.Combine(tempDir, "concurrent.accdb");

        try
        {
            DbContextOptions<StoreDbContext> fileOptions = new DbContextOptionsBuilder<StoreDbContext>()
                .UseJetDatabaseWriter(dbPath, DatabaseFormat.AceAccdb)
                .Options;

#pragma warning disable RCS1261 // Intentionally testing synchronous IDisposable.Dispose.
            using (StoreDbContext syncDisposedContext = new(fileOptions))
            {
                _ = await syncDisposedContext.Database.EnsureCreatedAsync(ct);
                syncDisposedContext.Customers.Add(new Customer
                {
                    Code = "C-SYNC",
                    Name = "Sync Dispose Customer",
                    CreditLimit = 100m,
                    RowVersion = 1,
                });
                _ = await syncDisposedContext.SaveChangesAsync(ct);
                _ = await syncDisposedContext.Customers.ToListAsync(ct);
            }
#pragma warning restore RCS1261

            await using (StoreDbContext readerContext = new(fileOptions))
            await using (StoreDbContext writerContext = new(fileOptions))
            {
                List<Customer> initial = await readerContext.Customers.AsNoTracking().ToListAsync(ct);
                Assert.Single(initial);

                writerContext.Customers.Add(new Customer
                {
                    Code = "C-CONCURRENT",
                    Name = "Added By Second Context",
                    CreditLimit = 250m,
                    RowVersion = 1,
                });
                _ = await writerContext.SaveChangesAsync(ct);

                List<Customer> refreshed = await readerContext.Customers.AsNoTracking().OrderBy(c => c.Code).ToListAsync(ct);
                Assert.Equal(2, refreshed.Count);
                Assert.Equal(["C-CONCURRENT", "C-SYNC"], refreshed.Select(static c => c.Code).ToArray());
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

    [Fact]
    public async Task Options_Templates_Are_Preserved_And_CanConnectAsync_Closes_Temporary_Reader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string tempDir = Path.Combine(Path.GetTempPath(), $"jdw-efcore-opts-{Guid.NewGuid():N}");
        string dbPath = Path.Combine(tempDir, "jet3probe.mdb");
        string lockPath = Path.Combine(tempDir, "jet3probe.ldb");

        try
        {
            AccessReaderOptions customReaderOptions = new()
            {
                MaxEncryptionSpinCount = 250_000,
                MaxEncryptionInfoBytes = 512 * 1024,
                MaxTableDefinitionBytes = 8 * 1024 * 1024,
                MaxSchemaTraversalBytes = 32 * 1024 * 1024,
                LockFileUserName = "EfReaderUser",
                LockFileMachineName = "EfReaderBox",
                LockTimeoutMilliseconds = 1234,
                MaxLongValueBytes = 123_456,
                MaxAttachmentContentBytes = 2 * 1024 * 1024,
                MaxAttachmentContentBytesPerOperation = 8L * 1024 * 1024,
                MaxAttachmentDecodeWorkBytesPerOperation = 16L * 1024 * 1024,
                MaxComplexDiscoveryEntries = 4096,
                MaxComplexItemIdentityEntriesPerOperation = 8192,
                MaxTotalEncryptionSpinCount = 500_000,
                PageCacheSize = 64,
                MaxActivePageLeases = 512,
                MaxIncludeMaterializedRowsPerOperation = 2048,
                MaxIncludeMaterializedPayloadBytesPerOperation = 4L * 1024 * 1024,
                DiagnosticsEnabled = true,
                PageReadOptimizationMode = PageReadOptimizationMode.Disabled,
                ValidateOnOpen = false,
                StrictParsing = false,
                LinkedSourceMaxDepth = 5,
                LinkedTextMaxRecordLength = 10_000,
                LinkedTextMaxFieldLength = 5_000,
                LinkedTextMaxColumnCount = 50,
                LinkedTextMaxSourceFileBytes = 1_000_000L,
                LinkedTextMaxMaterializedRows = 500u,
            };

            var frenchCulture = CultureInfo.GetCultureInfo("fr-FR");
            AccessWriterOptions customWriterOptions = new()
            {
                MaxEncryptionSpinCount = 300_000,
                MaxEncryptionInfoBytes = 256 * 1024,
                MaxTableDefinitionBytes = 4 * 1024 * 1024,
                MaxSchemaTraversalBytes = 16 * 1024 * 1024,
                LockFileUserName = "EfWriterUser",
                LockFileMachineName = "EfWriterBox",
                UseByteRangeLocks = false,
                LockTimeoutMilliseconds = 2345,
                RespectExistingLockFile = false,
                MaxTransactionPageBudget = 1024,
                PageCacheSize = 128,
                CalculatedExpressionCulture = frenchCulture,
                SecureEraseMode = SecureEraseMode.DeletedRowsAndFreedPages,
                UseTransactionalWrites = false,
            };

            DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
                .UseJetDatabaseWriter(
                    dbPath,
                    DatabaseFormat.Jet3Mdb,
                    jet => jet
                        .WithReaderOptions(customReaderOptions)
                        .WithWriterOptions(customWriterOptions))
                .Options;

            await using StoreDbContext context = new(options);
#pragma warning disable EF1001 // Testing internal JetClientConnection option construction directly.
            JetClientConnection clientConnection = Assert.IsType<JetClientConnection>(context.GetService<IJetClientConnection>());

            AccessReaderOptions builtReader = clientConnection.BuildReaderOptions();
            AccessWriterOptions builtWriter = clientConnection.BuildWriterOptions();
#pragma warning restore EF1001

            Assert.Equal(250_000, builtReader.MaxEncryptionSpinCount);
            Assert.Equal(512 * 1024, builtReader.MaxEncryptionInfoBytes);
            Assert.Equal(8 * 1024 * 1024, builtReader.MaxTableDefinitionBytes);
            Assert.Equal(32 * 1024 * 1024, builtReader.MaxSchemaTraversalBytes);
            Assert.Equal("EfReaderUser", builtReader.LockFileUserName);
            Assert.Equal("EfReaderBox", builtReader.LockFileMachineName);
            Assert.Equal(1234, builtReader.LockTimeoutMilliseconds);
            Assert.Equal(123_456, builtReader.MaxLongValueBytes);
            Assert.Equal(2 * 1024 * 1024, builtReader.MaxAttachmentContentBytes);
            Assert.Equal(8L * 1024 * 1024, builtReader.MaxAttachmentContentBytesPerOperation);
            Assert.Equal(16L * 1024 * 1024, builtReader.MaxAttachmentDecodeWorkBytesPerOperation);
            Assert.Equal(4096, builtReader.MaxComplexDiscoveryEntries);
            Assert.Equal(8192, builtReader.MaxComplexItemIdentityEntriesPerOperation);
            Assert.Equal(500_000, builtReader.MaxTotalEncryptionSpinCount);
            Assert.Equal(64, builtReader.PageCacheSize);
            Assert.Equal(512, builtReader.MaxActivePageLeases);
            Assert.Equal(2048, builtReader.MaxIncludeMaterializedRowsPerOperation);
            Assert.Equal(4L * 1024 * 1024, builtReader.MaxIncludeMaterializedPayloadBytesPerOperation);
            Assert.True(builtReader.DiagnosticsEnabled);
            Assert.Equal(PageReadOptimizationMode.Disabled, builtReader.PageReadOptimizationMode);
            Assert.False(builtReader.ValidateOnOpen);
            Assert.False(builtReader.StrictParsing);
            Assert.Equal(5, builtReader.LinkedSourceMaxDepth);
            Assert.Equal(10_000, builtReader.LinkedTextMaxRecordLength);
            Assert.Equal(5_000, builtReader.LinkedTextMaxFieldLength);
            Assert.Equal(50, builtReader.LinkedTextMaxColumnCount);
            Assert.Equal(1_000_000L, builtReader.LinkedTextMaxSourceFileBytes);
            Assert.Equal(500u, builtReader.LinkedTextMaxMaterializedRows);

            Assert.Equal(300_000, builtWriter.MaxEncryptionSpinCount);
            Assert.Equal(256 * 1024, builtWriter.MaxEncryptionInfoBytes);
            Assert.Equal(4 * 1024 * 1024, builtWriter.MaxTableDefinitionBytes);
            Assert.Equal(16 * 1024 * 1024, builtWriter.MaxSchemaTraversalBytes);
            Assert.Equal("EfWriterUser", builtWriter.LockFileUserName);
            Assert.Equal("EfWriterBox", builtWriter.LockFileMachineName);
            Assert.False(builtWriter.UseByteRangeLocks);
            Assert.Equal(2345, builtWriter.LockTimeoutMilliseconds);
            Assert.False(builtWriter.RespectExistingLockFile);
            Assert.Equal(1024, builtWriter.MaxTransactionPageBudget);
            Assert.Equal(128, builtWriter.PageCacheSize);
            Assert.Same(frenchCulture, builtWriter.CalculatedExpressionCulture);
            Assert.Equal(SecureEraseMode.DeletedRowsAndFreedPages, builtWriter.SecureEraseMode);
            Assert.False(builtWriter.UseTransactionalWrites);

            Assert.True(await context.Database.EnsureCreatedAsync(ct));
            Assert.False(File.Exists(lockPath));

            Assert.True(await context.Database.CanConnectAsync(ct));
            Assert.False(File.Exists(lockPath));
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
    public async Task SaveChangesAsync_NestedRollback_TphSiblingDefault_SentinelZero_And_SelfFkModifyBeforeDelete()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<AdvancedCatalogDbContext> options = new DbContextOptionsBuilder<AdvancedCatalogDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (AdvancedCatalogDbContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));

            CategoryNode parent = new()
            {
                Code = "PARENT-DEL",
                Name = "Parent To Delete",
                Priority = 0,
            };
            CategoryNode defaultPriorityNode = new()
            {
                Code = "SENTINEL-DEF",
                Name = "Uses Default Priority",
            };
            context.Categories.AddRange(parent, defaultPriorityNode);
            context.PaymentMethods.Add(new BankPaymentMethod
            {
                AccountNumber = "BANK-001",
                RoutingNumber = "111000025",
                BankName = "First National",
            });
            _ = await context.SaveChangesAsync(ct);

            Assert.Equal(0, parent.Priority);
            Assert.Equal(10, defaultPriorityNode.Priority);

            CategoryNode child = new()
            {
                Code = "CHILD-SEVER",
                Name = "Child Severing FK",
                ParentId = parent.Id,
                Priority = 5,
            };
            context.Categories.Add(child);
            _ = await context.SaveChangesAsync(ct);

            // Mark parent Deleted and modify child to sever FK in the same SaveChangesAsync batch.
            _ = context.Categories.Remove(parent);
            child.ParentId = null;
            _ = await context.SaveChangesAsync(ct);

            // Verify nested SaveChangesAsync rollback inside an explicit user transaction.
            await using IDbContextTransaction tx = await context.Database.BeginTransactionAsync(ct);
            context.Categories.Add(new CategoryNode
            {
                Code = "TX-KEEP",
                Name = "Kept In Outer Transaction",
                Priority = 1,
            });
            _ = await context.SaveChangesAsync(ct);

            CategoryNode partialFirst = new()
            {
                Code = "TX-PARTIAL-1",
                Name = "Should Roll Back With Second Entry Failure",
                Priority = 2,
            };
            CategoryNode duplicateSecond = new()
            {
                Code = "ROOT",
                Name = "Duplicate Alternate Key",
                Priority = 3,
            };
            context.Categories.AddRange(partialFirst, duplicateSecond);
            _ = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));

            await tx.CommitAsync(ct);
        }

        await using (AdvancedCatalogDbContext verifyContext = new(options))
        {
            List<CategoryNode> categories = await verifyContext.Categories
                .AsNoTracking()
                .OrderBy(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["CHILD-SEVER", "ROOT", "SENTINEL-DEF", "TX-KEEP"], categories.Select(static c => c.Code).ToArray());

            CategoryNode severedChild = categories.Single(static c => c.Code == "CHILD-SEVER");
            Assert.Null(severedChild.ParentId);
            Assert.Equal(5, severedChild.Priority);

            CategoryNode sentinelDefault = categories.Single(static c => c.Code == "SENTINEL-DEF");
            Assert.Equal(10, sentinelDefault.Priority);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IReadOnlyList<ColumnMetadata> paymentCols = await reader.GetColumnMetadataAsync("PaymentMethods", ct);
        int cardNetworkOrdinal = paymentCols.Single(static c => c.Name == "CardNetwork").Ordinal;
        List<object[]> paymentRows = [];
        await foreach (object[] row in reader.Rows("PaymentMethods", cancellationToken: ct))
        {
            paymentRows.Add(row);
        }

        object[] bankRow = Assert.Single(paymentRows);
        Assert.True(bankRow[cardNetworkOrdinal] is null or DBNull);
    }

    [Fact]
    public async Task Transaction_Savepoints_Create_Rollback_And_Release_Correctly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (StoreDbContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));

            await using IDbContextTransaction tx = await context.Database.BeginTransactionAsync(ct);
            Assert.True(tx.SupportsSavepoints);
            _ = Assert.Throws<NotSupportedException>(() => tx.CreateSavepoint("sync-sp"));
            _ = Assert.Throws<NotSupportedException>(() => tx.RollbackToSavepoint("sync-sp"));
            _ = Assert.Throws<NotSupportedException>(() => tx.ReleaseSavepoint("sync-sp"));

            context.Customers.Add(new Customer
            {
                Code = "SP-BASE",
                Name = "Before Savepoint",
                CreditLimit = 100m,
                RowVersion = 1,
            });
            _ = await context.SaveChangesAsync(ct);

            await tx.CreateSavepointAsync("sp1", ct);

            context.Customers.Add(new Customer
            {
                Code = "SP-DISCARD",
                Name = "Discarded By RollbackToSavepoint",
                CreditLimit = 200m,
                RowVersion = 1,
            });
            _ = await context.SaveChangesAsync(ct);

            await tx.RollbackToSavepointAsync("sp1", ct);
            context.ChangeTracker.Clear();

            // Savepoint sp1 remains active after RollbackToSavepointAsync, so subsequent work can be released.
            context.Customers.Add(new Customer
            {
                Code = "SP-RETAIN",
                Name = "Added After RollbackToSavepoint",
                CreditLimit = 300m,
                RowVersion = 1,
            });
            _ = await context.SaveChangesAsync(ct);

            await tx.ReleaseSavepointAsync("sp1", ct);
            await tx.CommitAsync(ct);
        }

        await using (StoreDbContext verifyContext = new(options))
        {
            List<string> codes = await verifyContext.Customers
                .AsNoTracking()
                .OrderBy(c => c.Code)
                .Select(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["SP-BASE", "SP-RETAIN"], codes);
        }
    }

    [Fact]
    public async Task Query_IdentityResolution_IncludeTrees_NullSafeNavigation_And_InheritedQueryFilter_Work_Correctly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream storeStream = new();
        DbContextOptions<StoreDbContext> storeOptions = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(storeStream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (StoreDbContext seedStore = new(storeOptions))
        {
            Assert.True(await seedStore.Database.EnsureCreatedAsync(ct));
            Customer alice = new()
            {
                Code = "Q-ALICE",
                Name = "Alice",
                IsVip = false,
                CreditLimit = 100m,
                RowVersion = 1,
                Orders =
                [
                    new Order
                    {
                        OrderDate = new DateTime(2026, 5, 1, 0, 0, 0, DateTimeKind.Utc),
                        TotalAmount = 25m,
                        Status = OrderStatus.Pending,
                        TrackingId = Guid.NewGuid(),
                    },
                    new Order
                    {
                        OrderDate = new DateTime(2026, 5, 2, 0, 0, 0, DateTimeKind.Utc),
                        TotalAmount = 75m,
                        Status = OrderStatus.Shipped,
                        TrackingId = Guid.NewGuid(),
                    },
                ],
            };
            seedStore.Customers.Add(alice);
            _ = await seedStore.SaveChangesAsync(ct);
        }

        // 1. Pre-tracked entity with unsaved in-memory edit:
        //    Database Where predicate evaluates against on-disk value (IsVip == false),
        //    while surviving query result resolves to the tracked instance (preserving unsaved IsVip == true).
        await using (StoreDbContext trackedStore = new(storeOptions))
        {
            Customer trackedAlice = await trackedStore.Customers.SingleAsync(c => c.Code == "Q-ALICE", ct);
            trackedAlice.IsVip = true; // unsaved in-memory edit

            Assert.Empty(await trackedStore.Customers.Where(c => c.IsVip).ToListAsync(ct));
            Customer fromDbFalseQuery = await trackedStore.Customers.SingleAsync(c => !c.IsVip, ct);
            Assert.Same(trackedAlice, fromDbFalseQuery);
            Assert.True(fromDbFalseQuery.IsVip);
        }

        // 2. Projected entity inside anonymous type and ValueTuple is tracked in ChangeTracker.
        await using (StoreDbContext projectionStore = new(storeOptions))
        {
            var projected = await projectionStore.Orders
                .Include(o => o.Customer)
                .OrderBy(o => o.TotalAmount)
                .Select(o => new { Order = o, o.Customer, Pair = new ValueTuple<Order, Customer?>(o, o.Customer) })
                .FirstAsync(ct);

            Assert.NotNull(projected.Customer);
            Assert.Equal(EntityState.Unchanged, projectionStore.Entry(projected.Order).State);
            Assert.Equal(EntityState.Unchanged, projectionStore.Entry(projected.Customer!).State);
            Assert.Same(projected.Order, projected.Pair.Item1);
            Assert.Same(projected.Customer, projected.Pair.Item2);
        }

        // 3. Combining Where(c => c.Orders.Any(...)) with filtered .Include(c => c.Orders.Where(...EF.Property...))
        //    does not leak unfiltered orders into the included collection or fail on EF.Property(o, ...).
        await using (StoreDbContext filteredIncludeStore = new(storeOptions))
        {
            Customer loaded = await filteredIncludeStore.Customers
                .Where(c => c.Orders.Any(o => o.Status == OrderStatus.Pending))
                .Include(c => c.Orders.Where(o => EF.Property<OrderStatus>(o, nameof(Order.Status)) == OrderStatus.Shipped))
                .SingleAsync(ct);

            Order singleOrder = Assert.Single(loaded.Orders);
            Assert.Equal(OrderStatus.Shipped, singleOrder.Status);
        }

        // 4. Self-referencing multi-level Include/ThenInclude, multi-hop navigation predicate,
        //    null-safe value-type navigation comparison, and inherited TPH base query filter.
        await using MemoryStream advStream = new();
        DbContextOptions<AdvancedCatalogDbContext> advOptions = new DbContextOptionsBuilder<AdvancedCatalogDbContext>()
            .UseJetDatabaseWriter(advStream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (AdvancedCatalogDbContext adv = new(advOptions))
        {
            Assert.True(await adv.Database.EnsureCreatedAsync(ct));

            adv.Categories.AddRange(
                new CategoryNode
                {
                    Id = 2L,
                    Code = "CHILD",
                    Name = "Child Category",
                    ParentId = 1L,
                    Priority = 20,
                },
                new CategoryNode
                {
                    Id = 3L,
                    Code = "GRANDCHILD",
                    Name = "Grandchild Category",
                    ParentId = 2L,
                    Priority = 30,
                });

            adv.Audits.Add(new AuditEntry
            {
                CategoryCode = "GRANDCHILD",
                Message = "Leaf audit",
            });

            adv.PaymentMethods.AddRange(
                new CardPaymentMethod
                {
                    AccountNumber = "ACC-ACTIVE",
                    CardNetwork = "VISA",
                    LastFourDigits = "1111",
                    IsArchived = false,
                },
                new CardPaymentMethod
                {
                    AccountNumber = "ACC-ARCHIVED",
                    CardNetwork = "MC",
                    LastFourDigits = "2222",
                    IsArchived = true,
                });

            _ = await adv.SaveChangesAsync(ct);
        }

        await using (AdvancedCatalogDbContext verifyAdv = new(advOptions))
        {
            CategoryNode root = await verifyAdv.Categories
                .AsNoTracking()
                .Include(c => c.Children)
                .ThenInclude(c => c.Children)
                .SingleAsync(c => c.Id == 1L, ct);

            CategoryNode child = Assert.Single(root.Children);
            Assert.Equal("CHILD", child.Code);
            CategoryNode grandchild = Assert.Single(child.Children);
            Assert.Equal("GRANDCHILD", grandchild.Code);

            // Multi-hop navigation in Where without explicit Include
            AuditEntry audit = await verifyAdv.Audits
                .AsNoTracking()
                .SingleAsync(a => a.Category!.Parent!.Parent!.Code == "ROOT", ct);
            Assert.Equal("GRANDCHILD", audit.CategoryCode);

            // Null-safe value-type navigation comparison: root has Parent == null, so Parent.Priority == 0 must not match root!
            Assert.Empty(await verifyAdv.Categories
                .AsNoTracking()
                .Where(c => c.Parent!.Priority == 0)
                .ToListAsync(ct));
            List<string> childrenOfPriority10 = await verifyAdv.Categories
                .AsNoTracking()
                .Where(c => c.Parent!.Priority == 10)
                .Select(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["CHILD"], childrenOfPriority10);

            // Inherited TPH base query filter on derived DbSet<CardPaymentMethod>()
            List<CardPaymentMethod> activeCards = await verifyAdv.Set<CardPaymentMethod>()
                .AsNoTracking()
                .ToListAsync(ct);
            CardPaymentMethod activeCard = Assert.Single(activeCards);
            Assert.Equal("ACC-ACTIVE", activeCard.AccountNumber);

            List<CardPaymentMethod> allCards = await verifyAdv.Set<CardPaymentMethod>()
                .AsNoTracking()
                .IgnoreQueryFilters()
                .ToListAsync(ct);
            Assert.Equal(2, allCards.Count);
        }
    }

    [Fact]
    public async Task Query_ContainsIndexSeek_ColumnProjection_RowLimits_And_BatchedAutoNumberInserts_Work_Correctly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using (StoreDbContext seed = new(options))
        {
            Assert.True(await seed.Database.EnsureCreatedAsync(ct));

            Customer c1 = new() { Code = "C-01", Name = "One", CreditLimit = 100m, RowVersion = 1, Notes = new string('A', 2000) };
            Customer c2 = new() { Code = "C-02", Name = "Two", CreditLimit = 200m, RowVersion = 1, Notes = new string('B', 2000) };
            Customer c3 = new() { Code = "C-03", Name = "Three", CreditLimit = 300m, RowVersion = 1, IsVip = true };
            Customer c4 = new() { Code = "C-04", Name = "Four", CreditLimit = 400m, RowVersion = 1, IsVip = true };

            Order o1 = new()
            {
                Customer = c2,
                OrderDate = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 50m,
                Status = OrderStatus.Shipped,
                TrackingId = Guid.NewGuid(),
                Payload = new byte[4096],
            };
            Order o2 = new()
            {
                Customer = c4,
                OrderDate = new DateTime(2026, 6, 2, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 150m,
                Status = OrderStatus.Shipped,
                TrackingId = Guid.NewGuid(),
                Payload = new byte[4096],
            };

            seed.Customers.AddRange(c1, c2, c3, c4);
            seed.Orders.AddRange(o1, o2);
            seed.ScheduleItems.AddRange(
                new ScheduleItem { Title = "S1", ScheduledDate = new DateOnly(2026, 6, 1), StartTime = new TimeOnly(8, 0), Duration = TimeSpan.FromHours(1) },
                new ScheduleItem { Title = "S2", ScheduledDate = new DateOnly(2026, 6, 2), StartTime = new TimeOnly(9, 0), Duration = TimeSpan.FromHours(1) },
                new ScheduleItem { Title = "S3", ScheduledDate = new DateOnly(2026, 6, 3), StartTime = new TimeOnly(10, 0), Duration = TimeSpan.FromHours(1) });

            int saved = await seed.SaveChangesAsync(ct);
            Assert.Equal(9, saved);
            Assert.Equal([1, 2, 3, 4], [c1.Id, c2.Id, c3.Id, c4.Id]);
            Assert.Equal(c2.Id, o1.CustomerId);
            Assert.Equal(c4.Id, o2.CustomerId);
        }

        await using (StoreDbContext queryContext = new(options))
        {
            // Array & List .Contains index seek pushdown (with duplicate element deduplication)
            string[] targetCodes = ["C-02", "C-04", "C-02"];
            List<string> matchedNames = await queryContext.Customers
                .AsNoTracking()
                .Where(c => targetCodes.Contains(c.Code))
                .OrderBy(c => c.Code)
                .Select(c => c.Name)
                .ToListAsync(ct);
            Assert.Equal(["Two", "Four"], matchedNames);

            List<int> targetIds = [1, 3];
            List<string> matchedByIds = await queryContext.Customers
                .AsNoTracking()
                .Where(c => targetIds.Contains(c.Id))
                .OrderBy(c => c.Id)
                .Select(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["C-01", "C-03"], matchedByIds);

            string[] emptyCodes = [];
            Assert.Empty(await queryContext.Customers
                .AsNoTracking()
                .Where(c => emptyCodes.Contains(c.Code))
                .ToListAsync(ct));

            // Terminal predicate seek pushdown (Count, LongCount, Any, Last) and scalar aggregate projection
            Assert.Equal(1, await queryContext.Customers.CountAsync(c => c.Code == "C-03", ct));
            Assert.Equal(1L, await queryContext.Customers.LongCountAsync(c => c.Code == "C-03", ct));
            Assert.True(await queryContext.Customers.AnyAsync(c => c.Code == "C-03", ct));
            Assert.False(await queryContext.Customers.AnyAsync(c => c.Code == "C-99", ct));
            Customer lastC3 = await queryContext.Customers.AsNoTracking().OrderBy(c => c.Id).LastAsync(c => c.Code == "C-03", ct);
            Assert.Equal("Three", lastC3.Name);

            decimal shippedTotal = await queryContext.Orders
                .Where(o => o.Status == OrderStatus.Shipped)
                .SumAsync(o => o.TotalAmount, ct);
            Assert.Equal(200m, shippedTotal);

            // Unfiltered row limit pushdown (Any, First, Take)
            Assert.True(await queryContext.ScheduleItems.AnyAsync(ct));
            ScheduleItem firstItem = await queryContext.ScheduleItems.AsNoTracking().FirstAsync(ct);
            Assert.Equal("S1", firstItem.Title);
            List<string> takenTitles = await queryContext.ScheduleItems
                .AsNoTracking()
                .Take(2)
                .Select(s => s.Title)
                .ToListAsync(ct);
            Assert.Equal(2, takenTitles.Count);

            // Active transaction .Contains seek pushdown
            await using IDbContextTransaction tx = await queryContext.Database.BeginTransactionAsync(ct);
            List<string> txMatchedCodes = await queryContext.Customers
                .AsNoTracking()
                .Where(c => targetCodes.Contains(c.Code))
                .OrderBy(c => c.Code)
                .Select(c => c.Code)
                .ToListAsync(ct);
            Assert.Equal(["C-02", "C-04"], txMatchedCodes);
        }
    }

    [Fact]
    public async Task ModelValidation_And_JetMetadataExtensions_Calculated_DefaultExpressions_ValidationRules_Currency_And_DateTimeExtended_Work()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        // 1. JetModelValidator rejects TPT (derived entity mapped to a separate table)
        await using MemoryStream invalidStream = new();
        DbContextOptions<InvalidTptDbContext> invalidOptions = new DbContextOptionsBuilder<InvalidTptDbContext>()
            .UseJetDatabaseWriter(invalidStream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;
        await using (InvalidTptDbContext invalidCtx = new(invalidOptions))
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => _ = invalidCtx.Model);
            Assert.Contains("TPH", ex.Message, StringComparison.OrdinalIgnoreCase);
        }

        // 2. Access column metadata extensions: calculated column, default value expression,
        //    validation rule + text, description, allow zero length, currency, and Date/Time Extended.
        await using MemoryStream stream = new();
        DbContextOptions<InvoiceDbContext> options = new DbContextOptionsBuilder<InvoiceDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        DateTime preciseTimestamp = new DateTime(2026, 7, 15, 14, 30, 45, DateTimeKind.Unspecified).AddTicks(1234567);

        await using (InvoiceDbContext context = new(options))
        {
            Assert.True(await context.Database.EnsureCreatedAsync(ct));

            InvoiceLine line = new()
            {
                Sku = "SKU-100",
                Quantity = 3,
                UnitPrice = 12.50m,
                RecordedAt = preciseTimestamp,
            };
            context.Lines.Add(line);
            _ = await context.SaveChangesAsync(ct);

            // Calculated column ([Quantity] * [UnitPrice]) and DefaultValueExpression (=10 + 5) read back into tracked entity
            Assert.Equal(1, line.Id);
            Assert.Equal(15, line.DiscountPoints);
            Assert.Equal(37.50m, line.LineTotal);

            // Updating Quantity recomputes calculated LineTotal and updates tracked entity
            line.Quantity = 4;
            _ = await context.SaveChangesAsync(ct);
            Assert.Equal(50.00m, line.LineTotal);

            // Validation rule (> 0) rejects Quantity == 0 with DbUpdateException
            InvoiceLine invalidQty = new()
            {
                Sku = "SKU-BAD",
                Quantity = 0,
                UnitPrice = 10m,
                RecordedAt = preciseTimestamp,
            };
            context.Lines.Add(invalidQty);
            DbUpdateException ruleEx = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
            Assert.Contains("Quantity must be positive", ruleEx.Message, StringComparison.Ordinal);
            context.Entry(invalidQty).State = EntityState.Detached;

            // AllowZeroLength(false) rejects empty Sku with DbUpdateException
            InvoiceLine emptySku = new()
            {
                Sku = string.Empty,
                Quantity = 1,
                UnitPrice = 10m,
                RecordedAt = preciseTimestamp,
            };
            context.Lines.Add(emptySku);
            _ = await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(ct));
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IReadOnlyList<ColumnMetadata> cols = await reader.GetColumnMetadataAsync("InvoiceLines", ct);
        ColumnMetadata skuCol = cols.Single(static c => c.Name == "Sku");
        Assert.False(skuCol.AllowZeroLength);

        ColumnMetadata qtyCol = cols.Single(static c => c.Name == "Quantity");
        Assert.Equal("> 0", qtyCol.ValidationRuleExpression);
        Assert.Equal("Quantity must be positive", qtyCol.ValidationText);
        Assert.Equal("Line item quantity", qtyCol.Description);

        ColumnMetadata priceCol = cols.Single(static c => c.Name == "UnitPrice");
        Assert.True(priceCol.IsCurrency);

        ColumnMetadata totalCol = cols.Single(static c => c.Name == "LineTotal");
        Assert.True(totalCol.IsCalculated);

        ColumnMetadata recordedCol = cols.Single(static c => c.Name == "RecordedAt");
        Assert.Equal("Date/Time Extended", recordedCol.TypeName);
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task TrackedNavigationWithoutInclude_AndNestedProjection_RetainFixupAndTrackEntities(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using (StoreDbContext seed = new(options))
        {
            _ = await seed.Database.EnsureCreatedAsync(ct);
            Customer c1 = new()
            {
                Code = "C-NAV-1",
                Name = "Nav Customer One",
                IsVip = true,
                CreditLimit = 1000m,
                RowVersion = 1,
                Orders =
                [
                    new Order
                    {
                        OrderDate = new DateTime(2026, 3, 1, 0, 0, 0, DateTimeKind.Utc),
                        TotalAmount = 250m,
                        Status = OrderStatus.Shipped,
                        TrackingId = Guid.NewGuid(),
                    },
                    new Order
                    {
                        OrderDate = new DateTime(2026, 3, 2, 0, 0, 0, DateTimeKind.Utc),
                        TotalAmount = 50m,
                        Status = OrderStatus.Pending,
                        TrackingId = Guid.NewGuid(),
                    },
                ],
            };
            seed.Customers.Add(c1);
            _ = await seed.SaveChangesAsync(ct);
        }

        // 1. Tracked query referencing a navigation in Where without .Include() retains populated navigation and tracks related entities.
        await using (StoreDbContext context = new(options))
        {
            Customer matched = await context.Customers
                .Where(c => c.Orders.Any(o => o.TotalAmount >= 200m))
                .SingleAsync(ct);

            Assert.Equal("C-NAV-1", matched.Code);
            Assert.Equal(EntityState.Unchanged, context.Entry(matched).State);
            Assert.Equal(2, matched.Orders.Count);
            foreach (Order order in matched.Orders)
            {
                Assert.Same(matched, order.Customer);
                Assert.Equal(EntityState.Unchanged, context.Entry(order).State);
            }
        }

        // 2. Tracked query projecting a multi-level nested anonymous/DTO graph promotes inner entities to tracked instances.
        await using (StoreDbContext context = new(options))
        {
            var projected = await context.Customers
                .Where(c => c.Code == "C-NAV-1")
                .Select(c => new
                {
                    Outer = new
                    {
                        Entity = c,
                        TopOrder = c.Orders.OrderByDescending(o => o.TotalAmount).FirstOrDefault(),
                    },
                })
                .SingleAsync(ct);

            Assert.NotNull(projected.Outer.Entity);
            Assert.NotNull(projected.Outer.TopOrder);
            Assert.Equal(EntityState.Unchanged, context.Entry(projected.Outer.Entity).State);
            Assert.Equal(EntityState.Unchanged, context.Entry(projected.Outer.TopOrder!).State);

            projected.Outer.Entity.Name = "Updated Via Nested Projection";
            _ = await context.SaveChangesAsync(ct);
        }

        await using (StoreDbContext verify = new(options))
        {
            Customer reloaded = await verify.Customers.AsNoTracking().SingleAsync(c => c.Code == "C-NAV-1", ct);
            Assert.Equal("Updated Via Nested Projection", reloaded.Name);
        }
    }

    [Fact]
    public async Task TemporaryValueGenerators_ShortAndByte_StayBoundedAcrossWraparound()
    {
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, DatabaseFormat.AceAccdb, leaveOpen: true)
            .Options;

        await using StoreDbContext context = new(options);
        Customer customer = new() { Code = "TMP", Name = "Temp" };
        Microsoft.EntityFrameworkCore.ChangeTracking.EntityEntry<Customer> entry = context.Entry(customer);

        CancellationToken ct = TestContext.Current.CancellationToken;
#pragma warning disable EF1001 // Testing internal temporary integer value generator wraparound bounds.
        JetTemporaryIntegerValueGenerator<short> shortGen = new();
        for (int i = 0; i < 40_000; i++)
        {
            short val = await shortGen.NextAsync(entry, ct);
            Assert.True(val < 0, $"Expected negative short temporary value, got {val} at iteration {i}.");
        }

        JetTemporaryIntegerValueGenerator<byte> byteGen = new();
        for (int i = 0; i < 600; i++)
        {
            byte val = await byteGen.NextAsync(entry, ct);
            Assert.True(val is >= 1 and <= 250, $"Expected byte temporary value in [1, 250], got {val} at iteration {i}.");
        }
#pragma warning restore EF1001
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task ActiveTransaction_RangeDisjunctionAndBooleanIndexSeek_ReadsUncommittedMutations(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using StoreDbContext context = new(options);
        _ = await context.Database.EnsureCreatedAsync(ct);

        Customer c1 = new() { Code = "C1", Name = "One", IsVip = true, CreditLimit = 100m, RowVersion = 1 };
        Customer c2Init = new() { Code = "C2", Name = "Two", IsVip = false, CreditLimit = 200m, RowVersion = 1 };
        Customer c3 = new() { Code = "C3", Name = "Three", IsVip = true, CreditLimit = 300m, RowVersion = 1 };
        context.Customers.AddRange(c1, c2Init, c3);
        _ = await context.SaveChangesAsync(ct);

        await using IDbContextTransaction tx = await context.Database.BeginTransactionAsync(ct);

        // Mutate and insert inside the uncommitted transaction
        Customer c2 = await context.Customers.SingleAsync(c => c.Code == "C2", ct);
        c2.CreditLimit = 250m;
        c2.IsVip = true;
        Customer c4 = new() { Code = "C4", Name = "Four", IsVip = false, CreditLimit = 400m, RowVersion = 1 };
        context.Customers.Add(c4);
        _ = await context.SaveChangesAsync(ct);

        // Range seek on indexed Code + boolean member access + OrElse equality disjunction inside active transaction
        List<string> disjunctionCodes = await context.Customers
            .AsNoTracking()
            .Where(c => c.IsVip && (c.Code == "C1" || c.Code == "C2" || c.Code == "C4"))
            .OrderBy(c => c.Code)
            .Select(c => c.Code)
            .ToListAsync(ct);
        Assert.Equal(["C1", "C2"], disjunctionCodes);

        // Range seek on indexed primary key Id (>= c2.Id && <= c3.Id) inside active transaction
        int minId = c2.Id;
        int maxId = c3.Id;
        List<string> rangeCodes = await context.Customers
            .AsNoTracking()
            .Where(c => c.Id >= minId && c.Id <= maxId)
            .OrderBy(c => c.Code)
            .Select(c => c.Code)
            .ToListAsync(ct);
        Assert.Equal(["C2", "C3"], rangeCodes);

        // Negated boolean member filter
        List<string> nonVipCodes = await context.Customers
            .AsNoTracking()
            .Where(c => !c.IsVip)
            .Select(c => c.Code)
            .ToListAsync(ct);
        Assert.Equal(["C4"], nonVipCodes);

        await tx.CommitAsync(ct);
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task OwnedEntities_ComplexProperties_AndManyToManySkipNavigations_RoundTrip(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<RichModelingDbContext> options = new DbContextOptionsBuilder<RichModelingDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using (RichModelingDbContext context = new(options))
        {
            _ = await context.Database.EnsureCreatedAsync(ct);

            SupplyTag hardware = new() { Name = "Hardware" };
            SupplyTag cloud = new() { Name = "Cloud" };
            context.Tags.AddRange(hardware, cloud);

            Supplier s1 = new()
            {
                Name = "Acme Industrial",
                TaxProfile = new SupplierTaxProfile
                {
                    RegistrationCode = "TX-100",
                    TaxRate = 0.0825m,
                },
                Headquarters = new StreetAddress
                {
                    Street = "100 Main St",
                    City = "Austin",
                },
                SecondaryOffice = null,
                Contacts =
                [
                    new SupplierContact { ContactName = "Alice", Email = "alice@acme.test" },
                    new SupplierContact { ContactName = "Bob", Email = "bob@acme.test" },
                ],
                Tags = [hardware, cloud],
            };

            Supplier s2 = new()
            {
                Name = "Globex Corp",
                TaxProfile = new SupplierTaxProfile
                {
                    RegistrationCode = "TX-200",
                    TaxRate = 0.065m,
                },
                Headquarters = new StreetAddress
                {
                    Street = "200 Oak Ave",
                    City = "Dallas",
                },
                SecondaryOffice = new StreetAddress
                {
                    Street = "500 Annex Blvd",
                    City = "Houston",
                },
                Contacts =
                [
                    new SupplierContact { ContactName = "Charlie", Email = "charlie@globex.test" },
                ],
                Tags = [hardware],
            };

            context.Suppliers.AddRange(s1, s2);
            _ = await context.SaveChangesAsync(ct);
        }

        // Verify query auto-includes owned navigations, materializes ComplexProperty, leaves null optional OwnsOne as null,
        // and loads many-to-many skip navigations via .Include().
        await using (RichModelingDbContext context = new(options))
        {
            List<Supplier> suppliers = await context.Suppliers
                .Include(s => s.Tags)
                .OrderBy(s => s.Name)
                .ToListAsync(ct);

            Assert.Equal(2, suppliers.Count);

            Supplier acme = suppliers[0];
            Assert.Equal("Acme Industrial", acme.Name);
            Assert.Equal("TX-100", acme.TaxProfile.RegistrationCode);
            Assert.Equal(0.0825m, acme.TaxProfile.TaxRate);
            Assert.Equal("100 Main St", acme.Headquarters.Street);
            Assert.Equal("Austin", acme.Headquarters.City);
            Assert.Null(acme.SecondaryOffice);
            Assert.Equal(2, acme.Contacts.Count);
            Assert.Equal(["Cloud", "Hardware"], acme.Tags.OrderBy(t => t.Name).Select(t => t.Name).ToArray());

            Supplier globex = suppliers[1];
            Assert.NotNull(globex.SecondaryOffice);
            Assert.Equal("Houston", globex.SecondaryOffice!.City);
            SupplyTag globexTag = Assert.Single(globex.Tags);
            Assert.Equal("Hardware", globexTag.Name);

            // Standalone update of table-split OwnsOne while owner entry remains Unchanged
            acme.Headquarters.City = "Round Rock";
            acme.SecondaryOffice = new StreetAddress
            {
                Street = "900 Branch Rd",
                City = "San Antonio",
            };
            _ = await context.SaveChangesAsync(ct);
        }

        // Verify ComplexProperty predicate pushdown and updated table-split OwnsOne values
        await using (RichModelingDbContext verify = new(options))
        {
            Supplier acme = await verify.Suppliers
                .AsNoTracking()
                .Include(s => s.Tags)
                .SingleAsync(s => s.TaxProfile.RegistrationCode == "TX-100", ct);

            Assert.Equal("Round Rock", acme.Headquarters.City);
            Assert.NotNull(acme.SecondaryOffice);
            Assert.Equal("San Antonio", acme.SecondaryOffice!.City);
            Assert.Equal(2, acme.Tags.Count);
        }
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task SelfReferencingForeignKey_AndCascadeUpdates_PersistInDatabaseRelationships(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<CascadeRelationshipDbContext> options = new DbContextOptionsBuilder<CascadeRelationshipDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using (CascadeRelationshipDbContext context = new(options))
        {
            _ = await context.Database.EnsureCreatedAsync(ct);
            EmployeeNode manager = new()
            {
                EmployeeCode = "EMP-01",
                FullName = "Jane Manager",
            };
            EmployeeNode report = new()
            {
                EmployeeCode = "EMP-02",
                FullName = "John Report",
                Manager = manager,
            };
            EmployeeAssignment assignment = new()
            {
                EmployeeCode = "EMP-02",
                ProjectName = "Apollo",
                Employee = report,
            };
            context.Employees.AddRange(manager, report);
            context.Assignments.Add(assignment);
            _ = await context.SaveChangesAsync(ct);
        }

        stream.Position = 0;
        await using AccessReader reader = await AccessReader.OpenAsync(stream, leaveOpen: true, cancellationToken: ct);
        IReadOnlyList<RelationshipMetadata> relationships = await reader.ListRelationshipsAsync(ct);

        // 1. Self-referencing relationship on Employees (ManagerId -> Id)
        RelationshipMetadata? selfRel = relationships.FirstOrDefault(
            static r => string.Equals(r.PrimaryTable, "Employees", StringComparison.OrdinalIgnoreCase)
                && string.Equals(r.ForeignTable, "Employees", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(selfRel);
        Assert.True(selfRel!.EnforcesReferentialIntegrity);

        // 2. Cascade Updates + Cascade Deletes on Assignments -> Employees
        RelationshipMetadata? cascadeRel = relationships.FirstOrDefault(
            static r => string.Equals(r.PrimaryTable, "Employees", StringComparison.OrdinalIgnoreCase)
                && string.Equals(r.ForeignTable, "Assignments", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(cascadeRel);
        Assert.True(cascadeRel!.CascadeUpdates);
        Assert.True(cascadeRel.CascadeDeletes);
    }

    [Theory]
    [MemberData(nameof(SupportedFormats))]
    public async Task QueryPushdown_ParameterizedTake_FilteredLimit_OrderedScan_Aggregates_AndJoinKeySeek(DatabaseFormat format)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using MemoryStream stream = new();
        DbContextOptions<StoreDbContext> options = new DbContextOptionsBuilder<StoreDbContext>()
            .UseJetDatabaseWriter(stream, format, leaveOpen: true)
            .Options;

        await using StoreDbContext context = new(options);
        _ = await context.Database.EnsureCreatedAsync(ct);

        Customer c1 = new() { Code = "C-01", Name = "Alpha", IsVip = false, CreditLimit = 100m, RowVersion = 1 };
        Customer c2 = new() { Code = "C-02", Name = "Beta", IsVip = true, CreditLimit = 200m, RowVersion = 1 };
        Customer c3 = new() { Code = "C-03", Name = "Gamma", IsVip = true, CreditLimit = 300m, RowVersion = 1 };
        Customer c4 = new() { Code = "C-04", Name = "Delta", IsVip = false, CreditLimit = 400m, RowVersion = 1 };
        context.Customers.AddRange(c1, c2, c3, c4);
        _ = await context.SaveChangesAsync(ct);

        context.Orders.AddRange(
            new Order
            {
                CustomerId = c2.Id,
                OrderDate = new DateTime(2026, 4, 1, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 50m,
                Status = OrderStatus.Pending,
                TrackingId = Guid.NewGuid(),
            },
            new Order
            {
                CustomerId = c3.Id,
                OrderDate = new DateTime(2026, 4, 2, 0, 0, 0, DateTimeKind.Utc),
                TotalAmount = 150m,
                Status = OrderStatus.Shipped,
                TrackingId = Guid.NewGuid(),
            });
        _ = await context.SaveChangesAsync(ct);

        // 1. Parameterized Take(p) with different parameter values reuses plan accurately
        int[] rowLimits = [1, 3];
        int takeOne = rowLimits[0];
        int takeThree = rowLimits[1];
        List<Customer> firstBatch = await context.Customers.AsNoTracking().Take(takeOne).ToListAsync(ct);
        List<Customer> secondBatch = await context.Customers.AsNoTracking().Take(takeThree).ToListAsync(ct);
        _ = Assert.Single(firstBatch);
        Assert.Equal(3, secondBatch.Count);

        // 2. Filtered Where(predicate).Take(p) short-circuits after matching rows
        List<Customer> vipSingle = await context.Customers
            .AsNoTracking()
            .Where(c => c.IsVip)
            .Take(takeOne)
            .ToListAsync(ct);
        Customer singleVip = Assert.Single(vipSingle);
        Assert.True(singleVip.IsVip);

        // 3. Ascending OrderBy(indexed).Take(2) index scan pushdown
        List<string> orderedCodes = await context.Customers
            .AsNoTracking()
            .OrderBy(c => c.Code)
            .Take(2)
            .Select(c => c.Code)
            .ToListAsync(ct);
        Assert.Equal(["C-01", "C-02"], orderedCodes);

        // 4. Entity-free CountAsync / LongCountAsync / AnyAsync fast paths
        Assert.Equal(4, await context.Customers.CountAsync(ct));
        Assert.Equal(4L, await context.Customers.LongCountAsync(ct));
        Assert.True(await context.Customers.AnyAsync(ct));

        // 5. Inner Join & GroupJoin key-seek pushdown
        var joined = await context.Customers
            .AsNoTracking()
            .Where(c => c.IsVip)
            .Join(
                context.Orders.AsNoTracking(),
                c => c.Id,
                o => o.CustomerId,
                (c, o) => new { c.Code, o.TotalAmount })
            .OrderBy(x => x.Code)
            .ToListAsync(ct);
        Assert.Equal(2, joined.Count);
        Assert.Equal("C-02", joined[0].Code);
        Assert.Equal(50m, joined[0].TotalAmount);
        Assert.Equal("C-03", joined[1].Code);
        Assert.Equal(150m, joined[1].TotalAmount);

        var groupJoined = await context.Customers
            .AsNoTracking()
            .Where(c => c.Code == "C-02")
            .GroupJoin(
                context.Orders.AsNoTracking(),
                c => c.Id,
                o => o.CustomerId,
                (c, orders) => new { c.Code, OrderCount = orders.Count() })
            .SingleAsync(ct);
        Assert.Equal("C-02", groupJoined.Code);
        Assert.Equal(1, groupJoined.OrderCount);
    }

    private sealed class RichModelingDbContext(DbContextOptions<RichModelingDbContext> options) : DbContext(options)
    {
        public DbSet<Supplier> Suppliers => this.Set<Supplier>();

        public DbSet<SupplyTag> Tags => this.Set<SupplyTag>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<SupplyTag>(entity =>
            {
                _ = entity.ToTable("SupplyTags");
                _ = entity.HasKey(t => t.Id);
                _ = entity.Property(t => t.Name).HasMaxLength(60).IsRequired();
            });

            _ = modelBuilder.Entity<Supplier>(entity =>
            {
                _ = entity.ToTable("Suppliers");
                _ = entity.HasKey(s => s.Id);
                _ = entity.Property(s => s.Name).HasMaxLength(100).IsRequired();

                _ = entity.ComplexProperty(
                    s => s.TaxProfile,
                    complex =>
                    {
                        _ = complex.Property(p => p.RegistrationCode)
                            .HasColumnName("TaxRegCode")
                            .HasMaxLength(40)
                            .IsRequired();
                        _ = complex.Property(p => p.TaxRate)
                            .HasColumnName("TaxRate")
                            .HasPrecision(18, 4);
                    });

                _ = entity.OwnsOne(
                    s => s.Headquarters,
                    owned =>
                    {
                        _ = owned.Property(a => a.Street).HasMaxLength(120).IsRequired();
                        _ = owned.Property(a => a.City).HasMaxLength(80).IsRequired();
                    });
                _ = entity.Navigation(s => s.Headquarters).IsRequired();

                _ = entity.OwnsOne(
                    s => s.SecondaryOffice,
                    owned =>
                    {
                        _ = owned.Property(a => a.Street).HasMaxLength(120);
                        _ = owned.Property(a => a.City).HasMaxLength(80);
                    });

                _ = entity.OwnsMany(
                    s => s.Contacts,
                    owned =>
                    {
                        _ = owned.ToTable("SupplierContacts");
                        _ = owned.WithOwner().HasForeignKey(c => c.SupplierId);
                        _ = owned.HasKey(c => c.Id);
                        _ = owned.Property(c => c.ContactName).HasMaxLength(80).IsRequired();
                        _ = owned.Property(c => c.Email).HasMaxLength(120).IsRequired();
                    });

                _ = entity.HasMany(s => s.Tags)
                    .WithMany(t => t.Suppliers);
            });
        }
    }

    private sealed class Supplier
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public SupplierTaxProfile TaxProfile { get; set; } = new();

        public StreetAddress Headquarters { get; set; } = new();

        public StreetAddress? SecondaryOffice { get; set; }

        public List<SupplierContact> Contacts { get; set; } = [];

        public List<SupplyTag> Tags { get; set; } = [];
    }

    private sealed class SupplierTaxProfile
    {
        public string RegistrationCode { get; set; } = string.Empty;

        public decimal TaxRate { get; set; }
    }

    private sealed class StreetAddress
    {
        public string Street { get; set; } = string.Empty;

        public string City { get; set; } = string.Empty;
    }

    private sealed class SupplierContact
    {
        public int Id { get; set; }

        public int SupplierId { get; set; }

        public string ContactName { get; set; } = string.Empty;

        public string Email { get; set; } = string.Empty;
    }

    private sealed class SupplyTag
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<Supplier> Suppliers { get; set; } = [];
    }

    private sealed class CascadeRelationshipDbContext(DbContextOptions<CascadeRelationshipDbContext> options) : DbContext(options)
    {
        public DbSet<EmployeeNode> Employees => this.Set<EmployeeNode>();

        public DbSet<EmployeeAssignment> Assignments => this.Set<EmployeeAssignment>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<EmployeeNode>(entity =>
            {
                _ = entity.ToTable("Employees");
                _ = entity.HasKey(e => e.Id);
                _ = entity.HasAlternateKey(e => e.EmployeeCode);
                _ = entity.Property(e => e.EmployeeCode).HasMaxLength(32).IsRequired();
                _ = entity.Property(e => e.FullName).HasMaxLength(100).IsRequired();
                _ = entity.HasOne(e => e.Manager)
                    .WithMany(e => e.DirectReports)
                    .HasForeignKey(e => e.ManagerId)
                    .OnDelete(DeleteBehavior.Restrict);
            });

            _ = modelBuilder.Entity<EmployeeAssignment>(entity =>
            {
                _ = entity.ToTable("Assignments");
                _ = entity.HasKey(a => a.Id);
                _ = entity.Property(a => a.EmployeeCode).HasMaxLength(32).IsRequired();
                _ = entity.Property(a => a.ProjectName).HasMaxLength(100).IsRequired();
                _ = entity.HasOne(a => a.Employee)
                    .WithMany(e => e.Assignments)
                    .HasForeignKey(a => a.EmployeeCode)
                    .HasPrincipalKey(e => e.EmployeeCode)
                    .OnDelete(DeleteBehavior.Cascade)
                    .OnUpdateCascade();
            });
        }
    }

    private sealed class EmployeeNode
    {
        public int Id { get; set; }

        public string EmployeeCode { get; set; } = string.Empty;

        public string FullName { get; set; } = string.Empty;

        public int? ManagerId { get; set; }

        public EmployeeNode? Manager { get; set; }

        public List<EmployeeNode> DirectReports { get; set; } = [];

        public List<EmployeeAssignment> Assignments { get; set; } = [];
    }

    private sealed class EmployeeAssignment
    {
        public int Id { get; set; }

        public string EmployeeCode { get; set; } = string.Empty;

        public string ProjectName { get; set; } = string.Empty;

        public EmployeeNode? Employee { get; set; }
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

    private sealed class AdvancedCatalogDbContext(DbContextOptions<AdvancedCatalogDbContext> options) : DbContext(options)
    {
        public DbSet<CategoryNode> Categories => this.Set<CategoryNode>();

        public DbSet<AuditEntry> Audits => this.Set<AuditEntry>();

        public DbSet<PaymentMethod> PaymentMethods => this.Set<PaymentMethod>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<CategoryNode>(entity =>
            {
                _ = entity.ToTable("Categories");
                _ = entity.HasKey(c => c.Id);
                _ = entity.HasAlternateKey(c => c.Code);
                _ = entity.Property(c => c.Code).HasMaxLength(40).IsRequired();
                _ = entity.Property(c => c.Name).HasMaxLength(100).IsRequired();
                _ = entity.Property(c => c.Tier).HasMaxLength(40).HasDefaultValue("General");
                _ = entity.Property(c => c.Priority).HasDefaultValue(10).HasSentinel(-1);
                _ = entity.HasOne(c => c.Parent)
                    .WithMany(c => c.Children)
                    .HasForeignKey(c => c.ParentId)
                    .OnDelete(DeleteBehavior.NoAction);
                _ = entity.HasData(new CategoryNode
                {
                    Id = 1L,
                    Code = "ROOT",
                    Name = "Root Category",
                    Tier = "General",
                    Priority = 10,
                });
            });

            _ = modelBuilder.Entity<AuditEntry>(entity =>
            {
                _ = entity.ToTable("Audits");
                _ = entity.HasKey(a => a.Id);
                _ = entity.Property(a => a.CategoryCode).HasMaxLength(40).IsRequired();
                _ = entity.Property(a => a.Message).HasMaxLength(200).IsRequired();
                _ = entity.HasOne(a => a.Category)
                    .WithMany()
                    .HasForeignKey(a => a.CategoryCode)
                    .HasPrincipalKey(c => c.Code);
            });

            _ = modelBuilder.Entity<PaymentMethod>(entity =>
            {
                _ = entity.ToTable("PaymentMethods");
                _ = entity.HasKey(p => p.Id);
                _ = entity.Property(p => p.AccountNumber).HasMaxLength(40).IsRequired();
                _ = entity.HasQueryFilter(p => !p.IsArchived);
                _ = entity.HasDiscriminator<string>("MethodKind")
                    .HasValue<CardPaymentMethod>("Card")
                    .HasValue<BankPaymentMethod>("Bank");
            });

            _ = modelBuilder.Entity<CardPaymentMethod>(entity =>
            {
                _ = entity.Property(c => c.CardNetwork).HasMaxLength(30).HasDefaultValue("VISA");
                _ = entity.Property(c => c.LastFourDigits).HasMaxLength(4);
            });

            _ = modelBuilder.Entity<BankPaymentMethod>(entity =>
            {
                _ = entity.Property(b => b.RoutingNumber).HasMaxLength(20);
                _ = entity.Property(b => b.BankName).HasMaxLength(80);
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

    private sealed class CategoryNode
    {
        public long Id { get; set; }

        public string Code { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;

        public string? Tier { get; set; }

        public int Priority { get; set; } = -1;

        public string? UnannotatedDescription { get; set; }

        public long? ParentId { get; set; }

        public CategoryNode? Parent { get; set; }

        public List<CategoryNode> Children { get; set; } = [];
    }

    private sealed class AuditEntry
    {
        public long Id { get; set; }

        public string CategoryCode { get; set; } = string.Empty;

        public string Message { get; set; } = string.Empty;

        public CategoryNode? Category { get; set; }
    }

    private abstract class PaymentMethod
    {
        public int Id { get; set; }

        public string AccountNumber { get; set; } = string.Empty;

        public bool IsArchived { get; set; }
    }

    private sealed class CardPaymentMethod : PaymentMethod
    {
        public string CardNetwork { get; set; } = string.Empty;

        public string LastFourDigits { get; set; } = string.Empty;
    }

    private sealed class BankPaymentMethod : PaymentMethod
    {
        public string RoutingNumber { get; set; } = string.Empty;

        public string BankName { get; set; } = string.Empty;
    }

    private sealed class InvalidTptDbContext(DbContextOptions<InvalidTptDbContext> options) : DbContext(options)
    {
        public DbSet<PaymentMethod> PaymentMethods => this.Set<PaymentMethod>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            _ = modelBuilder.Entity<PaymentMethod>(entity =>
            {
                _ = entity.ToTable("PaymentMethods");
                _ = entity.HasKey(p => p.Id);
            });

            _ = modelBuilder.Entity<CardPaymentMethod>(static entity => _ = entity.ToTable("CardPaymentMethods"));
        }
    }

    private sealed class InvoiceDbContext(DbContextOptions<InvoiceDbContext> options) : DbContext(options)
    {
        public DbSet<InvoiceLine> Lines => this.Set<InvoiceLine>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            _ = modelBuilder.Entity<InvoiceLine>(entity =>
            {
                _ = entity.ToTable("InvoiceLines");
                _ = entity.HasKey(l => l.Id);
                _ = entity.Property(l => l.Sku)
                    .HasMaxLength(40)
                    .IsRequired()
                    .HasJetAllowZeroLength(false);
                _ = entity.Property(l => l.Quantity)
                    .HasJetValidationRule("> 0", "Quantity must be positive")
                    .HasJetDescription("Line item quantity");
                _ = entity.Property(l => l.DiscountPoints)
                    .HasJetDefaultValueExpression("=10 + 5");
                _ = entity.Property(l => l.UnitPrice)
                    .UseJetCurrency();
                _ = entity.Property(l => l.LineTotal)
                    .UseJetCurrency()
                    .HasJetCalculatedExpression("[Quantity] * [UnitPrice]");
                _ = entity.Property(l => l.RecordedAt)
                    .UseJetDateTimeExtended();
            });
    }

    private sealed class InvoiceLine
    {
        public int Id { get; set; }

        public string Sku { get; set; } = string.Empty;

        public int Quantity { get; set; }

        public int DiscountPoints { get; set; }

        public decimal UnitPrice { get; set; }

        public decimal LineTotal { get; set; }

        public DateTime RecordedAt { get; set; }
    }
}
