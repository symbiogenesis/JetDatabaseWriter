using BenchmarkDotNet.Running;
using JetDatabaseWriter.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(RowMapperBenchmarks).Assembly).Run(args);
