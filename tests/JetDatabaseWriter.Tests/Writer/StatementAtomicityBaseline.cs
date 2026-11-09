namespace JetDatabaseWriter.Tests.Writer;

using System;

/// <summary>Owns the successful fault inventory and images without sharing mutable streams or writers.</summary>
internal sealed class StatementAtomicityBaseline(byte[] original, byte[] completed, int writes, int reads)
{
    private readonly byte[] originalImage = (byte[])original.Clone();
    private readonly byte[] completedImage = (byte[])completed.Clone();

    internal ReadOnlyMemory<byte> Original => this.originalImage;

    internal ReadOnlyMemory<byte> Completed => this.completedImage;

    internal int Writes { get; } = writes;

    internal int Reads { get; } = reads;
}
