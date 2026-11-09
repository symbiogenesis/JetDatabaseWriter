namespace JetDatabaseWriter.Tests.Writer;

using System;

/// <summary>Owns the successful fault inventory and images without sharing mutable streams or writers.</summary>
/// <param name="original">The original database image.</param>
/// <param name="completed">The successfully mutated database image.</param>
/// <param name="writes">The successful mutation's physical write count.</param>
/// <param name="reads">The successful mutation's physical read count.</param>
internal sealed class StatementAtomicityBaseline(byte[] original, byte[] completed, int writes, int reads)
{
    private readonly byte[] originalImage = (byte[])original.Clone();
    private readonly byte[] completedImage = (byte[])completed.Clone();

    internal ReadOnlyMemory<byte> Original => this.originalImage;

    internal ReadOnlyMemory<byte> Completed => this.completedImage;

    internal int Writes { get; } = writes;

    internal int Reads { get; } = reads;
}
