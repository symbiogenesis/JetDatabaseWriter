namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Verification;

/// <summary>Compares generated ordered bitmap enumeration with the retained original byte scan.</summary>
[MemoryDiagnoser]
[DisassemblyDiagnoser(maxDepth: 3, printSource: true, exportCombinedDisassemblyReport: true)]
public class UsageMapEnumerationBenchmarks
{
    private byte[] bitmap = [];
    private List<long> pages = [];

    /// <summary>Gets or sets the native inline or reference bitmap length.</summary>
    [Params(64, 2044, 4092)]
    public int BitmapBytes { get; set; }

    /// <summary>Gets or sets the approximate percentage of marked bits.</summary>
    [Params(0, 1, 50, 100)]
    public int Density { get; set; }

    /// <summary>Creates a deterministic bitmap and validates both scans against independent bit expansion.</summary>
    /// <exception cref="InvalidOperationException">A measured scanner disagrees with independent membership.</exception>
    [GlobalSetup]
    public void Setup()
    {
        this.bitmap = new byte[this.BitmapBytes];
        this.pages = new List<long>(this.BitmapBytes * 8);
        var expected = new List<long>();
        for (int bit = 0; bit < this.BitmapBytes * 8; bit++)
        {
            int selection = bit * 37;
            if (selection % 100 < this.Density)
            {
                this.bitmap[bit / 8] |= (byte)(1 << (bit % 8));
                expected.Add(100L + bit);
            }
        }

        _ = this.GeneratedScanner();
        if (!this.pages.SequenceEqual(expected))
        {
            throw new InvalidOperationException("The generated scan disagrees with independent membership.");
        }

        _ = this.OriginalScanner();
        if (!this.pages.SequenceEqual(expected))
        {
            throw new InvalidOperationException("The original scan disagrees with independent membership.");
        }
    }

    /// <summary>Runs the generated scan with its allocation-free continuation retained by the production adapter.</summary>
    /// <returns>The number of emitted members.</returns>
    [Benchmark]
    public int GeneratedScanner()
    {
        this.pages.Clear();
        if (!UsageMapKernel.HasScanRange((uint)this.bitmap.Length, 0, (uint)this.bitmap.Length, 100, 0))
        {
            return 0;
        }

        uint cursor = 0;
        while (true)
        {
            UsageMapKernel.NextBitmapPage(this.bitmap, 0, (uint)this.bitmap.Length, 100, long.MaxValue, 0, true, cursor, out cursor, out byte members, out byte state, out long firstPage);
            if (members != 0)
            {
                if (members == byte.MaxValue)
                {
                    this.pages.Add(firstPage);
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 1));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 2));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 3));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 4));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 5));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 6));
                    this.pages.Add(UsageMapKernel.PageOffset(firstPage, 7));
                }
                else
                {
                    int bits = members;
                    long pageNumber = firstPage;
                    while (true)
                    {
                        if ((bits & 1) != 0)
                        {
                            this.pages.Add(pageNumber);
                        }

                        bits >>= 1;
                        if (bits == 0)
                        {
                            break;
                        }

                        pageNumber++;
                    }
                }
            }

            if (state != 2)
            {
                return this.pages.Count;
            }
        }
    }

    /// <summary>Runs the original byte-cached bitmap scan with identical output storage.</summary>
    /// <returns>The number of emitted members.</returns>
    [Benchmark(Baseline = true)]
    public int OriginalScanner()
    {
        this.pages.Clear();
        for (int index = 0; index < this.bitmap.Length; index++)
        {
            int bits = this.bitmap[index];
            if (bits == 0)
            {
                continue;
            }

            long pageNumber = 100L + (index * 8);
            do
            {
                if ((bits & 1) != 0)
                {
                    if (pageNumber is >= 0 and < long.MaxValue)
                    {
                        this.pages.Add(pageNumber);
                    }
                    else
                    {
                        return this.pages.Count;
                    }
                }

                bits >>= 1;
                pageNumber++;
            }
            while (bits != 0);
        }

        return this.pages.Count;
    }
}
