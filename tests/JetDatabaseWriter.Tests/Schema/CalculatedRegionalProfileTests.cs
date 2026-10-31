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
    [InlineData("en-US", "712F8B3AC150DDBAEE88924AB7CC66488EB3650429681835A9E82AB0425FB9FB", "F5AE2EAA52F64B58DB1F9DAEA222B59E361B7A5413A23E14B1F96568C19A0A9C")]
    [InlineData("fr-FR", "326EC3B48CEB8155098623F150F6267A381FCFAF2451E5517EA698B47A20D38F", "8505EFEE088AE8D992734473FE8F4701B7A3D5085DA11F9F87C280333A11A062")]
    [InlineData("de-DE", "60C7252A2560BE7959CCFC3771FF86FC4486081AE7E2B37BDF212E26EF4FF8E5", "AE7509CD3FD1F5C9BB5776CABBB3AB6F793DA1D366A1E3D897AD898F57EEA241")]
    [InlineData("tr-TR", "0D5493479901BF5071B37F23DF9714F2793406980B1414DE88E9F148A7643993", "C6BB2C88E6E1749D44A8DF400710088B684E842B620265340F1F370F55A7D451")]
    [InlineData("ja-JP", "F67121821452078F12B89075980ABFBBC9CD1072D904119D336D7289DBCAF0B0", "D474C5055B62BDE3BBD9B8E2272F850E46BD1E140CC704A14BD3AEE376B4AC5F")]
    [InlineData("ar-SA", "666C899DDE44C50C829FF107A1B63F8B3A00B9EA27411FEDA0391BF10D37E010", "93A977EECF7D6BB9FBE38B9FCA5CB00D05F6B571B5EE1E357F415A34E328D7A9")]
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
                foreach (string table in new[] { "T", "MonthNames", "WeekNames", "RegionalNumbers", "RegionalDates", "RegionalGrammar", "RegionalBooleanResults", "RegionalMembership", "RegionalBooleanComparisons" })
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
