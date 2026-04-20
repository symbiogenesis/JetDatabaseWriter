namespace JetDatabaseReader.Tests;

using Xunit;

[CollectionDefinition]
[System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1515:Consider making public types internal", Justification = "xUnit CollectionDefinition requires public accessibility")]
public sealed class ReadOnlyDatabaseFixture : ICollectionFixture<DatabaseCache>
{
}
