namespace JetDatabaseWriter.Tests.Schema;

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

/// <summary>Native user regional settings govern calculated values independently of database LCID.</summary>
public sealed class CalculatedRegionalProfileTests
{
    [Theory]
    [InlineData("en-US", "E8D9E175E2A7F3F08B53D1563C884E52197999AEF33836E2FAB720DED99A2C6E", "1ECBB8774E257EE4A517BDD7AF8E220EC20B369902330EC6BD856E0B364F9152")]
    [InlineData("fr-FR", "924EABF3A9DACCA4A9ACE10B604E8516293695C09219C02172F84B037C4E2E13", "EA15C0FCA4A642C5B69E18C4BD8799CB0093416715121490082DE4BA790D7085")]
    [InlineData("de-DE", "AF23266A165B391D3A70542C12C3A8906E47AB00750AABED1EF8A2B8806133A6", "7E3FD5F6AA8084381BC6B2F416F1B454CE36C7E82660E379E417DC17EA1DC453")]
    [InlineData("tr-TR", "6A8F01B89C800AF875FDB876672D955DD14FC5A4617D06B1A37FE31B3E27CAE0", "1F3D8433F7EE0F9B428A67F67E7DB2D93AAF5764904389EC365783C92E8D9DA6")]
    [InlineData("ja-JP", "AB4BEE5C066125D3F02E91B33E1F0EE42D6F80D9F5A441A5E5437CF61984380F", "59DA396196F7BE222D9290065EAD5A7DC96155DE3368894F972BCCE1F435C14E")]
    [InlineData("ar-SA", "B3DAD0B3668643166B549669FAD9066ED23FA7505BBBDF2997B2D28216EC1C5C", "EED3873932112E045356AEF4440D969FD2AC6C6302C211528ADD9419DD6DF5CE")]
    public async Task NativeProfile_InsertAndUpdateMatchEveryRegionalCache(string cultureName, string sourceHash, string compactedHash)
    {
        var differences = new List<string>();
        foreach (string fixture in new[] { "native-profile.accdb", "native-profile-compacted.accdb" })
        {
            string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalProfiles", cultureName, fixture);
            byte[] original = await File.ReadAllBytesAsync(path, TestContext.Current.CancellationToken);
            Assert.Equal(fixture == "native-profile.accdb" ? sourceHash : compactedHash, Convert.ToHexString(SHA256.HashData(original)));
            var expected = new Dictionary<string, object[]>();
            await using (AccessReader reader = await AccessReader.OpenAsync(path, new AccessReaderOptions { UseLockFile = false }, TestContext.Current.CancellationToken))
            {
                foreach (string table in new[] { "T", "MonthNames", "WeekNames", "RegionalNumbers", "RegionalDates" })
                {
                    expected.Add(table, Assert.Single(await reader.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken)));
                }
            }

            await using var stream = new MemoryStream();
            stream.Write(original);
            stream.Position = 0;
            CultureInfo previous = CultureInfo.CurrentCulture;
            try
            {
                CultureInfo.CurrentCulture = await ReadNativeCultureAsync(cultureName);
                await using AccessWriter writer = await AccessWriter.OpenAsync(stream, new AccessWriterOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
                foreach ((string table, object[] row) in expected)
                {
                    object?[] inserted = new object?[row.Length];
                    Array.Copy(row, inserted, 4);
                    await writer.InsertRowAsync(table, inserted, TestContext.Current.CancellationToken);
                    Assert.Equal(2, await writer.UpdateRowsAsync(table, "N", row[1], new Dictionary<string, object?> { ["N"] = row[1] }, TestContext.Current.CancellationToken));
                }
            }
            finally
            {
                CultureInfo.CurrentCulture = previous;
            }

            stream.Position = 0;
            await using AccessReader actual = await AccessReader.OpenAsync(stream, new AccessReaderOptions { UseLockFile = false }, leaveOpen: true, cancellationToken: TestContext.Current.CancellationToken);
            foreach ((string table, object[] row) in expected)
            {
                object[][] rows = await actual.Rows(table, cancellationToken: TestContext.Current.CancellationToken).ToArrayAsync(TestContext.Current.CancellationToken);
                Assert.Equal(2, rows.Length);
                foreach (object[] result in rows)
                {
                    for (int column = 0; column < row.Length; column++)
                    {
                        if (!Equals(row[column], result[column]))
                        {
                            differences.Add($"{cultureName}/{fixture}/{table}[{column}]: native {row[column]}, writer {result[column]}");
                        }
                    }
                }
            }
        }

        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences.Distinct()));
    }

    private static async Task<CultureInfo> ReadNativeCultureAsync(string name)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "Databases", "CalculatedRegionalProfiles", name, "observations.json");
        using var report = JsonDocument.Parse(await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken));
        JsonElement metadata = report.RootElement.GetProperty("RuntimeCultureMetadata");
        var culture = new CultureInfo(name, useUserOverride: false);
        culture.DateTimeFormat.ShortDatePattern = metadata.GetProperty("ShortDatePattern").GetString()!;
        culture.DateTimeFormat.LongTimePattern = metadata.GetProperty("LongTimePattern").GetString()!;
        culture.DateTimeFormat.FirstDayOfWeek = Enum.Parse<DayOfWeek>(metadata.GetProperty("FirstDayOfWeek").GetString()!);
        culture.DateTimeFormat.AMDesignator = metadata.GetProperty("AMDesignator").GetString()!;
        culture.DateTimeFormat.PMDesignator = metadata.GetProperty("PMDesignator").GetString()!;
        culture.NumberFormat.NumberDecimalSeparator = metadata.GetProperty("NumberDecimalSeparator").GetString()!;
        culture.NumberFormat.NumberGroupSeparator = metadata.GetProperty("NumberGroupSeparator").GetString()!;
        return culture;
    }
}
