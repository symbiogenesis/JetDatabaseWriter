namespace JetDatabaseWriter.Benchmarks.Queries;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Benchmarks.Infrastructure;
using JetDatabaseWriter.Benchmarks.Models;
using JetDatabaseWriter.Linq;

/// <summary>
/// Eager loading through <c>Query&lt;T&gt;().Include(...)</c> over the synthetic
/// relational database: 1,000 customers and 10,000 orders joined by the
/// <c>FK_Orders_Customers</c> relationship. Covers a collection navigation, a
/// reference navigation and a filtered, ordered and paged collection. Setup runs
/// each benchmark once and throws unless every customer and order is loaded.
/// </summary>
[MemoryDiagnoser]
public class QueryIncludeBenchmarks
{
    private const int FilteredTake = 5;
    private const decimal FilteredMinimumAmount = 100m;

    private AccessReader? reader;

    private AccessReader Reader => this.reader ?? throw new InvalidOperationException("The reader was not initialized.");

    [GlobalSetup]
    public async Task Setup()
    {
        await SyntheticDatabases.EnsureRelationalAsync().ConfigureAwait(false);
        this.reader = await AccessReader.OpenAsync(SyntheticDatabases.RelationalDbPath).ConfigureAwait(false);

        Expect(nameof(this.Include_Collection), SyntheticDatabases.RelationalOrders, await this.Include_Collection().ConfigureAwait(false));
        Expect(nameof(this.Include_Reference), SyntheticDatabases.RelationalOrders, await this.Include_Reference().ConfigureAwait(false));
        Expect(
            nameof(this.Include_FilteredCollection),
            SyntheticDatabases.RelationalCustomers * FilteredTake,
            await this.Include_FilteredCollection().ConfigureAwait(false));
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        if (this.reader is not null)
        {
            await this.reader.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Loads every customer with all of its orders.</summary>
    /// <returns>The orders loaded.</returns>
    /// <exception cref="InvalidOperationException">A customer is missing.</exception>
    [Benchmark(Baseline = true)]
    public async Task<int> Include_Collection()
    {
        List<Customer> customers = await this.Reader.Query<Customer>(SyntheticDatabases.CustomersTable)
            .Include(c => c.Orders)
            .ToListAsync()
            .ConfigureAwait(false);
        return CountOrders(customers);
    }

    /// <summary>Loads every order with its customer.</summary>
    /// <returns>The orders whose customer was loaded.</returns>
    [Benchmark]
    public async Task<int> Include_Reference()
    {
        List<Order> orders = await this.Reader.Query<Order>(SyntheticDatabases.OrdersTable)
            .Include(o => o.Customer)
            .ToListAsync()
            .ConfigureAwait(false);
        return orders.Count(o => o.Customer is not null && o.Customer.CustomerId == o.CustomerId);
    }

    /// <summary>Loads every customer with its five largest orders of at least 100.</summary>
    /// <returns>The orders loaded.</returns>
    /// <exception cref="InvalidOperationException">A customer is missing.</exception>
    [Benchmark]
    public async Task<int> Include_FilteredCollection()
    {
        List<Customer> customers = await this.Reader.Query<Customer>(SyntheticDatabases.CustomersTable)
            .Include(c => c.Orders.Where(o => o.Amount >= FilteredMinimumAmount).OrderByDescending(o => o.Amount).Take(FilteredTake))
            .ToListAsync()
            .ConfigureAwait(false);
        return CountOrders(customers);
    }

    private static int CountOrders(List<Customer> customers)
    {
        if (customers.Count != SyntheticDatabases.RelationalCustomers)
        {
            throw new InvalidOperationException(
                $"The query returned {customers.Count} of the {SyntheticDatabases.RelationalCustomers} customers.");
        }

        return customers.Sum(c => c.Orders.Count);
    }

    private static void Expect(string benchmark, int expected, int actual)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException($"{benchmark} loaded {actual} orders; expected {expected}.");
        }
    }
}
