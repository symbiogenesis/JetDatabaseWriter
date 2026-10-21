namespace JetDatabaseWriter.Tests.Fuzz;

using System.Threading.Tasks;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

/// <summary>
/// Runs one process-isolated input selected by the hosted fuzz driver. Targeted ordinary
/// regressions check the exercise contract; intentional fuzz inputs run only on hosted CI.
/// </summary>
/// <param name="output">The operation and failure output.</param>
public sealed class AccessReaderFuzzTests(ITestOutputHelper output)
{
    [Trait("Category", "Fuzz")]
    [Fact(Explicit = true)]
    public async Task FuzzAccessReader()
    {
        byte[] bytes = await FuzzInput.ReadAsync(TestContext.Current.CancellationToken);
        await AccessReaderFuzzExercise.RunAsync(bytes, output.WriteLine, TestContext.Current.CancellationToken);
    }
}
