namespace JetDatabaseWriter.Benchmarks.Writer;

using System;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Verification;

/// <summary>Compares generated recovery page scans with their previous scalar-loop implementation.</summary>
[MemoryDiagnoser]
public class JournalContentBenchmarks
{
    private byte[] original = [];
    private byte[] current = [];
    private byte[] first = [];
    private byte[] second = [];
    private bool[] generated = [];
    private bool[] control = [];
    private bool recovering;

    /// <summary>Gets or sets the native page size.</summary>
    [Params(2048, 4096)]
    public int PageSize { get; set; }

    /// <summary>Gets or sets the supported success or refusal shape.</summary>
    [Params("Original", "Mixed", "Replay", "Conflict")]
    public string Shape { get; set; } = "Original";

    /// <summary>Builds and checks the stable byte arrays outside measurement.</summary>
    /// <exception cref="InvalidOperationException">The compared algorithms disagree on authorization.</exception>
    [GlobalSetup]
    public void Setup()
    {
        this.original = new byte[this.PageSize];
        this.current = new byte[this.PageSize];
        this.first = new byte[this.PageSize + 40];
        this.second = new byte[this.PageSize + 40];
        this.generated = new bool[this.PageSize];
        this.control = new bool[this.PageSize];
        this.recovering = this.Shape == "Replay";
        for (int index = 0; index < this.PageSize; index++)
        {
            this.original[index] = 11;
            this.first[index + 8] = 22;
            this.second[index + 8] = 33;
            if (this.Shape == "Original")
            {
                this.current[index] = 11;
            }
            else if (this.recovering && index >= this.PageSize / 2)
            {
                this.current[index] = 0;
            }
            else
            {
                this.current[index] = (index % 3) switch { 0 => 11, 1 => 22, _ => 33 };
            }
        }

        if (this.Shape == "Conflict")
        {
            this.current[^1] = 44;
        }

        bool expected = this.Shape != "Conflict";
        if (this.Generated() != expected || this.OriginalControl() != expected || !this.generated.AsSpan().SequenceEqual(this.control))
        {
            throw new InvalidOperationException("Recovery page authorization disagrees with its independent fixture or original control.");
        }
    }

    /// <summary>Measures generated page initialization, accumulation and final admission.</summary>
    /// <returns>Whether every current byte is authorized.</returns>
    [Benchmark]
    public bool Generated()
    {
        byte state = RecoveryKernel.InitializeJournalAuthorization(
            this.current,
            this.original,
            this.generated,
            (uint)this.PageSize,
            this.recovering,
            4096,
            (ulong)(4096 + (this.PageSize / 2)));
        if (state == 2)
        {
            return true;
        }

        return state != 0
            && RecoveryKernel.IncludeJournalAfterImage(this.current, this.first, this.generated, (uint)this.PageSize)
            && RecoveryKernel.IncludeJournalAfterImage(this.current, this.second, this.generated, (uint)this.PageSize)
            && RecoveryKernel.CheckJournalAuthorization(this.generated, (uint)this.PageSize);
    }

    /// <summary>Measures the original allocation-free scalar loops over identical buffers and records.</summary>
    /// <returns>Whether every current byte is authorized.</returns>
    [Benchmark(Baseline = true)]
    public bool OriginalControl()
    {
        byte[] current = this.current;
        byte[] original = this.original;
        byte[] first = this.first;
        byte[] second = this.second;
        bool[] valid = this.control;
        int count = this.PageSize;
        bool recovering = this.recovering;
        ulong minimumLength = (ulong)(4096 + (count / 2));
        bool matchesOriginal = true;
        for (int index = 0; index < count; index++)
        {
            valid[index] = RecoveryKernel.OriginalOrZeroByte(recovering, (ulong)(4096 + index), minimumLength, current[index], original[index]);
            matchesOriginal &= valid[index];
        }

        if (matchesOriginal)
        {
            return true;
        }

        for (int index = 0; index < count; index++)
        {
            valid[index] = RecoveryKernel.IncludeAfterImageByte(valid[index], current[index], first[8 + index]);
        }

        for (int index = 0; index < count; index++)
        {
            valid[index] = RecoveryKernel.IncludeAfterImageByte(valid[index], current[index], second[8 + index]);
        }

        for (int index = 0; index < count; index++)
        {
            if (!valid[index])
            {
                return false;
            }
        }

        return true;
    }
}
