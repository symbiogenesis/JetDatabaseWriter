namespace JetDatabaseWriter.Tests.Queries;

using System;
using System.Linq;
using JetDatabaseWriter.Linq;
using Xunit;

/// <summary>
/// The public LINQ surface (<c>Include</c>, <c>ThenInclude</c> and the async terminals, and
/// the interface <c>Include</c> returns) lives in <c>JetDatabaseWriter.Linq</c>, not in the
/// root namespace. EF Core declares extension methods and an <c>IIncludableQueryable</c> of
/// the same names, so a file that uses EF Core can import the root namespace for
/// <see cref="AccessReader"/> without ambiguity errors.
/// </summary>
public sealed class QueryNamespaceTests
{
    private const string LinqNamespace = "JetDatabaseWriter.Linq";

    [Fact]
    public void PublicLinqTypes_LiveInLinqNamespace()
    {
        Assert.Equal(LinqNamespace, typeof(AccessQueryExtensions).Namespace);
        Assert.Equal(LinqNamespace, typeof(IAccessIncludableQueryable<,>).Namespace);

        Type[] exported = typeof(AccessReader).Assembly.GetExportedTypes();
        string[] linqTypes = [.. exported.Where(t => t.Namespace == LinqNamespace).Select(t => t.Name).Order(StringComparer.Ordinal)];
        Assert.Equal(["AccessQueryExtensions", "IAccessIncludableQueryable`2"], linqTypes);

        // No type keeps the old name, and the root namespace no longer holds the extensions.
        Assert.DoesNotContain(exported, t => t.Name.StartsWith("IIncludableQueryable", StringComparison.Ordinal));
        Assert.DoesNotContain(exported, t => t.Namespace == "JetDatabaseWriter" && t.Name == nameof(AccessQueryExtensions));
    }
}
