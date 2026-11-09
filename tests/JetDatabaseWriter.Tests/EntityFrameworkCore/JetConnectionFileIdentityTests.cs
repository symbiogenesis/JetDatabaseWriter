namespace JetDatabaseWriter.Tests.EntityFrameworkCore;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.EntityFrameworkCore.Extensions;
using JetDatabaseWriter.EntityFrameworkCore.Storage.Internal;
using JetDatabaseWriter.Enums;
using JetDatabaseWriter.Models;
using Microsoft.EntityFrameworkCore;
using Xunit;

#pragma warning disable EF1001 // These regressions exercise the provider's file identity and peer coordination directly.
public sealed class JetConnectionFileIdentityTests(ITestOutputHelper output)
{
    [Fact(DisableParallelization = true)]
    public async Task Relative_Path_Opens_Updates_And_Deletes_The_Construction_Directory_File()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string originalDirectory = Environment.CurrentDirectory;
        string root = Path.Combine(Path.GetTempPath(), $"jdw-ef-identity-{Guid.NewGuid():N}");
        string boundDirectory = Path.Combine(root, "bound");
        string laterDirectory = Path.Combine(root, "later");
        _ = Directory.CreateDirectory(boundDirectory);
        _ = Directory.CreateDirectory(laterDirectory);
        string boundPath = Path.Combine(boundDirectory, "data.accdb");
        string laterPath = Path.Combine(laterDirectory, "data.accdb");
        try
        {
            await CreateDatabaseAsync(boundPath, 11, ct);
            await CreateDatabaseAsync(laterPath, 22, ct);
            Environment.CurrentDirectory = boundDirectory;
            boundPath = Path.Combine(Environment.CurrentDirectory, "data.accdb");
            output.WriteLine($"Requested directory: {boundDirectory}; bound path: {boundPath}");
            await using JetClientConnection connection = new(CreateOptions("data.accdb"));
            Environment.CurrentDirectory = laterDirectory;

            Assert.Equal(boundPath, connection.DatabasePath);
            Assert.True(await connection.CanConnectAsync(ct));
            Assert.Equal(11, await ReadSingleValueAsync(await connection.GetReaderAsync(ct), ct));
            await connection.CloseReaderAsync();
            (AccessWriter writer, bool owned) = await connection.RentWriterAsync(ct);
            try
            {
                await writer.InsertRowAsync("Items", [33], ct);
            }
            finally
            {
                await connection.ReleaseWriterAsync(writer, owned);
            }

            await using JetClientConnection peer = new(CreateOptions(boundPath));
            AccessReader reader = await peer.GetReaderAsync(ct);
            int count = 0;
            await foreach (object[] row in reader.Rows("Items", cancellationToken: ct))
            {
                Assert.True((int)row[0] is 11 or 33);
                count++;
            }

            Assert.Equal(2, count);
            Assert.True(await connection.DeleteDatabaseAsync(ct));
            Assert.False(File.Exists(boundPath));
            await using AccessReader untouched = await AccessReader.OpenAsync(laterPath, cancellationToken: ct);
            Assert.Equal(22, await ReadSingleValueAsync(untouched, ct));
        }
        finally
        {
            Environment.CurrentDirectory = originalDirectory;
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact(DisableParallelization = true)]
    public async Task Relative_Path_Creation_And_Existence_Check_Keep_The_Construction_Directory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string originalDirectory = Environment.CurrentDirectory;
        string root = Path.Combine(Path.GetTempPath(), $"jdw-ef-create-identity-{Guid.NewGuid():N}");
        string boundDirectory = Path.Combine(root, "bound");
        string laterDirectory = Path.Combine(root, "later");
        _ = Directory.CreateDirectory(boundDirectory);
        _ = Directory.CreateDirectory(laterDirectory);
        string relativePath = Path.Combine("nested", "new.accdb");
        try
        {
            Environment.CurrentDirectory = boundDirectory;
            string boundPath = Path.Combine(Environment.CurrentDirectory, relativePath);
            output.WriteLine($"Requested directory: {boundDirectory}; bound path: {boundPath}");
            await using JetClientConnection connection = new(CreateOptions(relativePath));
            Environment.CurrentDirectory = laterDirectory;

            Assert.Equal(boundPath, connection.DatabasePath);
            Assert.False(await connection.CanConnectAsync(ct));
            AccessWriter writer = await connection.CreateDatabaseWriterAsync(ct);
            await connection.ReleaseWriterAsync(writer, ownedByCaller: false);
            await connection.CloseReaderAsync();
            Assert.True(File.Exists(boundPath));
            Assert.True(await connection.CanConnectAsync(ct));
            Assert.False(Directory.Exists(Path.Combine(laterDirectory, "nested")));
            Assert.True(await connection.DeleteDatabaseAsync(ct));
            Assert.False(await connection.CanConnectAsync(ct));
        }
        finally
        {
            Environment.CurrentDirectory = originalDirectory;
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Case_Variant_Paths_Do_Not_Merge_Distinct_File_Connections()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string root = Path.Combine(Path.GetTempPath(), $"jdw-ef-case-identity-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(root);
        string firstPath = Path.Combine(root, "data.accdb");
        string secondPath = Path.Combine(root, "DATA.accdb");
        try
        {
            await CreateDatabaseAsync(firstPath, 11, ct);
            bool distinctFiles = !File.Exists(secondPath);
            output.WriteLine($"First path: {firstPath}; second path: {secondPath}; distinct files: {distinctFiles}");
            if (distinctFiles)
            {
                await CreateDatabaseAsync(secondPath, 22, ct);
            }

            await using JetClientConnection first = new(CreateOptions(firstPath));
            await using JetClientConnection second = new(CreateOptions(secondPath));
            (AccessWriter writer, bool owned) = await first.RentWriterAsync(ct);
            try
            {
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
                deadline.CancelAfter(TimeSpan.FromSeconds(10));
                Assert.Equal(distinctFiles, await second.CanConnectAsync(deadline.Token));
                if (distinctFiles)
                {
                    Assert.Equal(22, await ReadSingleValueAsync(await second.GetReaderAsync(ct), ct));
                    Assert.True(await second.DeleteDatabaseAsync(ct));
                    Assert.True(File.Exists(firstPath));
                    await writer.InsertRowAsync("Items", [33], ct);
                }
                else
                {
                    await Assert.ThrowsAnyAsync<IOException>(async () => _ = await second.RentWriterAsync(ct));
                }
            }
            finally
            {
                await first.ReleaseWriterAsync(writer, owned);
            }
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    private static DbContextOptions CreateOptions(string path) => new DbContextOptionsBuilder().UseJetDatabaseWriter(path).Options;

    private static async Task CreateDatabaseAsync(string path, int value, CancellationToken cancellationToken)
    {
        await using AccessWriter writer = await AccessWriter.CreateDatabaseAsync(path, DatabaseFormat.AceAccdb, cancellationToken: cancellationToken);
        await writer.CreateTableAsync("Items", [new ColumnDefinition("Id", typeof(int))], cancellationToken);
        await writer.InsertRowAsync("Items", [value], cancellationToken);
    }

    private static async Task<int> ReadSingleValueAsync(AccessReader reader, CancellationToken cancellationToken)
    {
        int count = 0;
        int result = 0;
        await foreach (object[] row in reader.Rows("Items", cancellationToken: cancellationToken))
        {
            result = (int)row[0];
            count++;
        }

        Assert.Equal(1, count);
        return result;
    }
}
#pragma warning restore EF1001
