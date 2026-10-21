namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Linq;
using JetDatabaseWriter.Models;
using Xunit;

/// <summary>Seek inference must preserve when predicate operands are evaluated.</summary>
public sealed class QueryOperandEvaluationTests
{
    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ComputedBoundAfterFalseGuard_DoesNotRun(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);
        Expression<Func<Person, bool>> predicate = p => p.Id == 1 && p.Score == ThrowValue();

        Assert.Empty(await reader.Query<Person>("People").Where(predicate).ToListAsync(TestContext.Current.CancellationToken));
        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Id == 1 && ThrowValue() == p.Score)
            .ToListAsync(TestContext.Current.CancellationToken));
        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Id == 1).Where(p => p.Score == ThrowValue())
            .ToListAsync(TestContext.Current.CancellationToken));
        await foreach (Person unused in reader.Rows("People", predicate, cancellationToken: TestContext.Current.CancellationToken))
        {
            Assert.Fail("The guard must reject the only row.");
        }
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CapturedGetterAfterFalseGuard_DoesNotRun(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);
        var bound = new Bound { ThrowOnRead = true };

        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Id == 1 && p.Score == bound.Value)
            .ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, bound.Reads);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CapturedNullMemberAfterFalseGuard_DoesNotRun(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);
        Bound? bound = null;

        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Id == 1 && p.Score == bound!.Value)
            .ToListAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CapturedGetterBeforeFalseGuard_RunsOncePerRow(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format, rowCount: 2);
        await using AccessReader reader = await OpenReaderAsync(stream);
        var bound = new Bound();

        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Score == bound.Value && p.Id == 1)
            .ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, bound.Reads);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task ComputedBoundOnEmptyTable_DoesNotRun(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format, rowCount: 0);
        await using AccessReader reader = await OpenReaderAsync(stream);

        Assert.Empty(await reader.Query<Person>("People").Where(p => p.Score == ThrowValue())
            .ToListAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CapturedGetterReached_PreservesItsException(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);
        var bound = new Bound { ThrowOnRead = true };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await reader.Query<Person>("People").Where(p => p.Id == 257 && p.Score == bound.Value)
                .ToListAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, bound.Reads);
    }

    [Theory]
    [InlineData(DatabaseFormat.AceAccdb)]
    [InlineData(DatabaseFormat.Jet4Mdb)]
    [InlineData(DatabaseFormat.Jet3Mdb)]
    public async Task CapturedVariableChanges_AreReadOnEveryExecution(DatabaseFormat format)
    {
        await using MemoryStream stream = await BuildAsync(format);
        await using AccessReader reader = await OpenReaderAsync(stream);
        int id = 1;
        IQueryable<Person> query = reader.Query<Person>("People").Where(p => p.Id == id);

        Assert.Empty(await query.ToListAsync(TestContext.Current.CancellationToken));
        id = 257;
        Assert.Equal(257, Assert.Single(await query.ToListAsync(TestContext.Current.CancellationToken)).Id);
        id = 1;
        Assert.Empty(await query.ToListAsync(TestContext.Current.CancellationToken));
    }

    private static int ThrowValue() => throw new InvalidOperationException("Seek operand was evaluated.");

    private static async ValueTask<MemoryStream> BuildAsync(DatabaseFormat format, int rowCount = 1)
    {
        var stream = new MemoryStream();
        await using (AccessWriter writer = await AccessWriter.CreateDatabaseAsync(
            stream, format, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken))
        {
            await writer.CreateTableAsync(
                "People",
                [new ColumnDefinition("Id", typeof(int)) { IsPrimaryKey = true }, new ColumnDefinition("Score", typeof(int))],
                [new IndexDefinition("IX_Score", "Score")],
                TestContext.Current.CancellationToken);
            for (int id = 257; id < 257 + rowCount; id++)
            {
                await writer.InsertRowAsync("People", [id, 10], TestContext.Current.CancellationToken);
            }
        }

        return stream;
    }

    private static ValueTask<AccessReader> OpenReaderAsync(MemoryStream stream)
    {
        stream.Position = 0;
        return AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, TestContext.Current.CancellationToken);
    }

#pragma warning disable CA1812 // The row mapper creates this type through its generic factory.
    private sealed class Person
#pragma warning restore CA1812
    {
        public int Id { get; set; }

        public int Score { get; set; }
    }

    private sealed class Bound
    {
        public bool ThrowOnRead { get; init; }

        public int Reads { get; private set; }

        public int Value
        {
            get
            {
                this.Reads++;
                return this.ThrowOnRead ? ThrowValue() : 10;
            }
        }
    }
}
