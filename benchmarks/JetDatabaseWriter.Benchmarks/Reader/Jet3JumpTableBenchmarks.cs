namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.Pages;

/// <summary>Compares verified Jet3 jump controls with the original span algorithms.</summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
public class Jet3JumpTableBenchmarks
{
    private int[] offsets = [];
    private byte[] jumps = [];
    private byte[] originalJumps = [];
    private int eod;

    /// <summary>Gets or sets the row length before jump entries: a boundary, dummy or long-row control.</summary>
    [Params(256, 512, 2028, 2029)]
    public int LengthWithoutJumps { get; set; }

    /// <summary>Builds and checks native boundary and dummy controls outside measurement.</summary>
    /// <exception cref="InvalidOperationException">A control disagrees with the expected bytes or offsets.</exception>
    [GlobalSetup]
    public void Setup()
    {
        this.offsets = this.LengthWithoutJumps switch
        {
            256 => [5, 128],
            512 => [5, 260],
            2028 => [5, 260, 520, 780, 1040, 1300, 1560, 1820],
            2029 => CreateDenseOffsets(),
            _ => throw new InvalidOperationException("The Jet3 row control is unsupported."),
        };
        this.eod = this.LengthWithoutJumps switch
        {
            256 => 247,
            512 => 507,
            2029 => 1740,
            _ => 2010,
        };
        this.jumps = new byte[this.OriginalSizing()];
        this.originalJumps = new byte[this.jumps.Length];
        this.ProductionWrite();
        this.OriginalWrite();
        if (!this.jumps.AsSpan().SequenceEqual(this.originalJumps)
            || this.ProductionSizing() != this.jumps.Length
            || this.ProductionHighParts() != this.OriginalHighParts())
        {
            throw new InvalidOperationException("The Jet3 row control changed native jump outcomes.");
        }
    }

    /// <summary>Computes the proved smallest self-consistent jump count.</summary>
    /// <returns>The jump count.</returns>
    [Benchmark]
    [BenchmarkCategory("Sizing")]
    public int ProductionSizing() => Jet3JumpTable.CountForLength(this.LengthWithoutJumps);

    /// <summary>Computes the original repeated-increment jump count.</summary>
    /// <returns>The jump count.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Sizing")]
    public int OriginalSizing() => OriginalCountForLength(this.LengthWithoutJumps);

    /// <summary>Resolves each variable offset and the EOD through the generated scan policy.</summary>
    /// <returns>A checksum of all high parts.</returns>
    [Benchmark]
    [BenchmarkCategory("HighParts")]
    public int ProductionHighParts()
    {
        int result = 0;
        int usable = Jet3JumpTable.UsableEntryCount(this.jumps.Length, this.eod);
        for (int index = 0; index <= this.offsets.Length; index++)
        {
            result += Jet3JumpTable.HighPart(this.jumps, 0, this.jumps.Length, usable, index);
        }

        return result;
    }

    /// <summary>Resolves the same offsets with the original bounded prefix scan.</summary>
    /// <returns>A checksum of all high parts.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("HighParts")]
    public int OriginalHighParts()
    {
        int result = 0;
        int usable = OriginalUsableEntryCount(this.jumps.Length, this.eod);
        for (int index = 0; index <= this.offsets.Length; index++)
        {
            result += OriginalHighPart(this.jumps, 0, this.jumps.Length, usable, index);
        }

        return result;
    }

    /// <summary>Writes native jump bytes through the generated entry and ambiguity policy.</summary>
    [Benchmark]
    [BenchmarkCategory("Write")]
    public void ProductionWrite() => Jet3JumpTable.Write(this.jumps, this.offsets, this.eod);

    /// <summary>Writes the same bytes with the original span algorithm.</summary>
    /// <exception cref="JetLimitationException">A fixture needs an unrepresentable dummy entry.</exception>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Write")]
    public void OriginalWrite() => OriginalWrite(this.originalJumps, this.offsets, this.eod);

    private static int[] CreateDenseOffsets()
    {
        int[] result = new int[255];
        Array.Fill(result, 1);
        int[] tail = [225, 478, 731, 984, 1237, 1490, 1700];
        tail.CopyTo(result, 248);
        return result;
    }

    private static int OriginalCountForLength(int lengthWithoutJumps)
    {
        int count = 0;
        while ((lengthWithoutJumps + count - 1) / 256 > count)
        {
            count++;
        }

        return count;
    }

    private static int OriginalUsableEntryCount(int entryCount, int eodPosition)
        => entryCount > 0 && eodPosition / 256 < entryCount ? entryCount - 1 : entryCount;

    private static int OriginalHighPart(ReadOnlySpan<byte> row, int rowStart, int jumpTableEnd, int usableEntries, int index)
    {
        int reached = 0;
        int previous = 0;
        while (reached < usableEntries)
        {
            int entry = row[rowStart + jumpTableEnd - 1 - reached];
            if (entry > index || entry < previous)
            {
                break;
            }

            previous = entry;
            reached++;
        }

        return reached * 256;
    }

    private static void OriginalWrite(Span<byte> destination, ReadOnlySpan<int> variableOffsets, int eod)
    {
        int varLen = variableOffsets.Length;
        int dummies = 0;
        for (int k = destination.Length; k >= 1; k--)
        {
            int boundary = k * 256;
            int entry = 255;
            if (eod >= boundary)
            {
                entry = varLen;
                for (int i = 0; i < varLen; i++)
                {
                    if (variableOffsets[i] >= boundary)
                    {
                        entry = i;
                        break;
                    }
                }
            }
            else
            {
                dummies++;
            }

            destination[^k] = checked((byte)entry);
        }

        if (varLen == 255 && dummies > 1)
        {
            throw new JetLimitationException(JetErrorCode.RowTooLarge, $"A Jet3 row with 255 variable columns cannot end its variable data at offset {eod}: its jump table would need {dummies} unused entries, which Jet cannot tell from the EOD's index.");
        }
    }
}
