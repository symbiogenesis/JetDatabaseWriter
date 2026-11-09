namespace JetDatabaseWriter.Tests.Infrastructure;

using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

/// <summary>Required fixture cases remain discoverable even when their input is missing or damaged.</summary>
public sealed class TestDatabaseInventoryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RequiredFixtureData_KeepsMissingAndCorruptInputs(bool corrupt)
    {
        string path = Path.Combine(Path.GetTempPath(), $"jdw-required-fixture-{Guid.NewGuid():N}.mdb");
        try
        {
            if (corrupt)
            {
                await File.WriteAllBytesAsync(path, [1, 2, 3], TestContext.Current.CancellationToken);
            }

            var data = TestDatabases.ToTheoryData([path]);
            Assert.Equal(path, Assert.Single(data).Data);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
