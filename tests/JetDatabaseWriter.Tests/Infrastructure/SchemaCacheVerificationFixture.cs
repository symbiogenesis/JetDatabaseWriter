[assembly: Xunit.AssemblyFixture(typeof(JetDatabaseWriter.Tests.Infrastructure.SchemaCacheVerificationFixture))]

namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using JetDatabaseWriter.Schema;

#pragma warning disable CA1812 // xUnit constructs this assembly fixture through reflection.
/// <summary>Verifies each partition's runtime and library asset, then enables structural cache verification.</summary>
internal sealed class SchemaCacheVerificationFixture : IDisposable
{
    /// <summary>Initializes a new instance of the <see cref="SchemaCacheVerificationFixture"/> class.</summary>
    public SchemaCacheVerificationFixture()
    {
        LibraryTarget.AssertExpectedTarget();
        TableDefReader.VerifyCacheHits = true;
    }

    /// <inheritdoc/>
    public void Dispose() => TableDefReader.VerifyCacheHits = false;
}
#pragma warning restore CA1812
