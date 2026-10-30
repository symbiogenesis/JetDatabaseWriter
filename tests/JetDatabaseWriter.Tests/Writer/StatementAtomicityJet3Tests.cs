namespace JetDatabaseWriter.Tests.Writer;

using System.Threading.Tasks;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>Checks every Jet3 statement fault position in an independently runnable partition.</summary>
/// <param name="db">Caches Access-authored fixtures with relationship catalogs.</param>
/// <param name="generated">Caches generated statement fault seeds.</param>
public sealed class StatementAtomicityJet3Tests(DatabaseCache db, GeneratedDatabaseCache generated) : StatementAtomicityTestBase(db, generated)
{
    /// <summary>Every physical write, partial write, flush and read either completes or restores the statement.</summary>
    /// <param name="statement">The mutation.</param>
    /// <param name="shard">The independent subset of fault positions.</param>
    /// <returns>The test completion.</returns>
    [Theory]
    [MemberData(nameof(Cases))]
    public Task OneShotFault_RestoresBytesAndWriterRemainsUsable(string statement, int shard) =>
        this.AssertEveryFaultAsync(DatabaseFormat.Jet3Mdb, statement, shard);
}
