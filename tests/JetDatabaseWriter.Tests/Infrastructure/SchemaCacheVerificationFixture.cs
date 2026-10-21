[assembly: Xunit.AssemblyFixture(typeof(JetDatabaseWriter.Tests.Infrastructure.SchemaCacheVerificationFixture))]

namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Schema;

#pragma warning disable CA1812 // xUnit constructs this assembly fixture through reflection.
/// <summary>Enables structural cache verification for every reader constructed by the suite.</summary>
internal sealed class SchemaCacheVerificationFixture : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="SchemaCacheVerificationFixture"/> class.</summary>
    public SchemaCacheVerificationFixture() => TableDefReader.VerifyCacheHits = true;

    /// <inheritdoc/>
    public void Dispose() => TableDefReader.VerifyCacheHits = false;
}
#pragma warning restore CA1812
