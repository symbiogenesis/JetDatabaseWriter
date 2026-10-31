namespace JetDatabaseWriter.Benchmarks.Reader;

using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using JetDatabaseWriter.Exceptions;
using JetDatabaseWriter.LongValues;
using JetDatabaseWriter.Verification;

/// <summary>Compares production visited control with its original count/growth wrapper.</summary>
[MemoryDiagnoser]
public class LongValueVisitedSetBenchmarks
{
    private uint[] pointers = [];

    /// <summary>The insertion sequence measured by both controllers.</summary>
    public enum SequenceShape
    {
        /// <summary>Distinct external-row identities across geometric growth.</summary>
        Ordinary = 0,

        /// <summary>Distinct identities sharing a wrapped probe cluster.</summary>
        Colliding = 1,

        /// <summary>Repeated identities at the initial half-load boundary.</summary>
        DuplicateBoundary = 2,

        /// <summary>A single identity requiring only the initial allocation.</summary>
        SingleRow = 3,
    }

    /// <summary>Gets or sets the independently validated insertion sequence.</summary>
    [Params(SequenceShape.Ordinary, SequenceShape.Colliding, SequenceShape.DuplicateBoundary, SequenceShape.SingleRow)]
    public SequenceShape Shape { get; set; }

    /// <summary>Builds input sequences and checks each outcome against independent membership.</summary>
    /// <exception cref="InvalidOperationException">A controller disagrees with independent membership.</exception>
    [GlobalSetup]
    public void Setup()
    {
        int length = this.Shape switch
        {
            SequenceShape.DuplicateBoundary => 264,
            SequenceShape.SingleRow => 1,
            SequenceShape.Ordinary or SequenceShape.Colliding => 256,
            _ => throw new InvalidOperationException("The visited-set sequence shape is unsupported."),
        };
        this.pointers = new uint[length];
        int index = 0;
        for (uint pointer = 1; index < this.pointers.Length; pointer++)
        {
            if (this.Shape == SequenceShape.Colliding)
            {
                ulong hash = ((ulong)pointer * 2654435761) + (pointer / 256) + (pointer / 65536);
                if (hash % 512 != 511)
                {
                    continue;
                }
            }

            this.pointers[index] = this.Shape switch
            {
                SequenceShape.Ordinary or SequenceShape.SingleRow => pointer * 256,
                SequenceShape.Colliding => pointer,
                SequenceShape.DuplicateBoundary => (checked((uint)index) % 8) + 1,
                _ => throw new InvalidOperationException("The visited-set sequence shape is unsupported."),
            };
            index++;
        }

        var expected = new HashSet<uint>();
        LongValueVisitedSet production = default;
        OriginalVisitedSet control = default;
        foreach (uint pointer in this.pointers)
        {
            bool inserted = expected.Add(pointer);
            if (production.Add(pointer) != inserted || control.Add(pointer) != inserted)
            {
                throw new InvalidOperationException("The visited-set fixture disagrees with independent membership.");
            }
        }

        foreach (uint pointer in expected)
        {
            if (production.Add(pointer) || control.Add(pointer))
            {
                throw new InvalidOperationException("The visited-set fixture lost an inserted identity.");
            }
        }

        if (this.ProductionController() != expected.Count || this.OriginalController() != expected.Count)
        {
            throw new InvalidOperationException("A measured visited-set controller lost insertion outcomes.");
        }
    }

    /// <summary>Runs the production controller with fresh lazy storage.</summary>
    /// <returns>The number of newly inserted identities.</returns>
    [Benchmark]
    public int ProductionController()
    {
        LongValueVisitedSet entries = default;
        int inserted = 0;
        foreach (uint pointer in this.pointers)
        {
            if (entries.Add(pointer))
            {
                inserted++;
            }
        }

        return inserted;
    }

    /// <summary>Runs the original wrapper using the same generated array operations.</summary>
    /// <returns>The number of newly inserted identities.</returns>
    [Benchmark(Baseline = true)]
    public int OriginalController()
    {
        OriginalVisitedSet entries = default;
        int inserted = 0;
        foreach (uint pointer in this.pointers)
        {
            if (entries.Add(pointer))
            {
                inserted++;
            }
        }

        return inserted;
    }

    /// <summary>Retains the original production wrapper as a same-host algorithm control.</summary>
    private struct OriginalVisitedSet
    {
        private uint[]? entries;
        private int count;

        /// <summary>Adds a nonzero identity once, refusing impossible array growth.</summary>
        /// <param name="pointer">The nonzero external-row identity.</param>
        /// <returns>Whether the identity was absent.</returns>
        /// <exception cref="JetLimitationException">The bounded tracking capacity is exhausted.</exception>
        /// <exception cref="JetOperationException">The table violates its insertion capacity invariant.</exception>
        internal bool Add(uint pointer)
        {
            if (pointer == 0)
            {
                return false;
            }

            if (this.entries is null || this.count >= this.entries.Length / 2)
            {
                // A duplicate at the growth boundary must not allocate a new table.
                if (this.entries is not null && LongValueKernel.ContainsVisited(this.entries, pointer))
                {
                    return false;
                }

                uint capacity = LongValueKernel.NextVisitedCapacity(checked((uint)(this.entries?.Length ?? 0)));
                if (capacity == 0)
                {
                    throw new JetLimitationException(JetErrorCode.ValueTooLarge, "The long-value row-identity tracking capacity is exhausted.");
                }

                uint[] next = new uint[checked((int)capacity)];
                if (this.entries is not null)
                {
                    LongValueKernel.RehashVisited(this.entries, next);
                }

                this.entries = next;
            }

            byte added = LongValueKernel.AddVisited(this.entries, pointer);
            if (added == 1)
            {
                return false;
            }

            if (added != 2)
            {
                throw new JetOperationException(JetErrorCode.InvalidOperationState, "The long-value row-identity table is unexpectedly full.");
            }

            this.count++;
            return true;
        }
    }
}
