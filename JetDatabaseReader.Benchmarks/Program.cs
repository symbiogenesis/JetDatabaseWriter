using BenchmarkDotNet.Running;
using JetDatabaseReader.Benchmarks;

BenchmarkSwitcher.FromAssembly(typeof(RowMapperBenchmarks).Assembly).Run(args);
