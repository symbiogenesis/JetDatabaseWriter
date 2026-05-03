namespace JetDatabaseWriter.Tests.Internal;

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Core;
using JetDatabaseWriter.Internal;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Tests.Infrastructure;
using Xunit;

#pragma warning disable CA1707 // Test names use underscores by convention

/// <summary>
/// Diagnostic probe — dumps the raw bytes of every real-idx physical
/// descriptor in <c>testIndexPropertiesV2007.accdb</c>'s TableIgnoreNulls1
/// TDEF so we can see exactly where the on-disk <c>flags</c> byte lives.
/// Lets us verify the Jackcess-derived offset (46 in the 52-byte Jet4
/// descriptor) is correct against an Access-authored fixture.
/// </summary>
public sealed class IndexFlagsRawProbeTests
{
    [Fact]
    public async Task DumpRealIdxBytes_ForTableIgnoreNulls1_V2007()
    {
        string fixturePath = TestDatabases.TestIndexPropertiesV2007;
        if (!File.Exists(fixturePath))
        {
            Assert.Skip($"Fixture not present: {fixturePath}");
        }

        CancellationToken ct = TestContext.Current.CancellationToken;
        await using AccessReader reader = await AccessReader.OpenAsync(
            fixturePath,
            new AccessReaderOptions { UseLockFile = false },
            ct);

        var sb = new StringBuilder();
        foreach (string tableName in new[] { "TableIgnoreNulls1", "TableUnique1_temp" })
        {
            var resolved = await reader.GetRawTDefBytesForTableAsync(tableName, ct);
            if (resolved is null)
            {
                sb.Append(CultureInfo.InvariantCulture, $"{tableName}: <not found>\n");
                continue;
            }

            var (td, descStart, numRealIdx) = resolved.Value;
            string header = FormattableString.Invariant(
                $"{tableName}: realIdxDescStart={descStart} numRealIdx={numRealIdx} td.Length={td.Length}\n");
            sb.Append(header);
            for (int i = 0; i < numRealIdx; i++)
            {
                int phys = descStart + (i * 52);
                if (phys + 52 > td.Length)
                {
                    break;
                }

                string slotHeader = FormattableString.Invariant($"  slot {i} phys={phys}: ");
                sb.Append(slotHeader);
                sb.Append(Convert.ToHexString(td.AsSpan(phys, 52)));
                sb.Append('\n');
                string firstDp = Convert.ToHexString(td.AsSpan(phys + 38, 4));
                string unk42 = Convert.ToHexString(td.AsSpan(phys + 42, 4));
                string unk47 = Convert.ToHexString(td.AsSpan(phys + 47, 5));
                string detail = FormattableString.Invariant(
                    $"    [38..41 first_dp]={firstDp} [42..45 unk]={unk42} [46 flags?]={td[phys + 46]:X2} [47..51 unk]={unk47}\n");
                sb.Append(detail);
            }
        }

        // This is a probe — never fails; the dump is the assertion.
        Assert.Fail(sb.ToString());
    }
}
