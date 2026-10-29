# Contributing

Issues and pull requests are welcome. Open an issue before starting a larger change.

## Getting started

Install the .NET SDK pinned in [global.json](global.json). Run these commands from the repository root:

```shell
dotnet build JetDatabaseWriter.slnx -c Release
dotnet test --project tests/JetDatabaseWriter.Tests -c Release -f net10.0 --no-build --filter-class "JetDatabaseWriter.Tests.Indexes.IndexWriterTests"
```

Replace the example test class with one relevant to your change. See [the project layout](AGENTS.md#the-project) to find the code you need.

## Making changes

- Work on your own local branch and worktree.
- Write a focused test first when changing behavior; for bugs, make it fail before applying the fix.
- Follow the [coding rules](AGENTS.md#code) and [test conventions](AGENTS.md#tests). Release builds must have zero warnings and errors.
- Update affected documentation. Keep [docs/todo.md](docs/todo.md) limited to open work.

## Validation

Local builds and focused tests are allowed. Full test suites and **all benchmarks run only on CI**, including single-case and dry benchmark runs.

Changes that affect builds or tests require successful CI for the exact integrated `main` revision. Local checks and hosted candidate builds do not replace this gate. Documentation-only changes do not require it. Follow the [integration and CI instructions](AGENTS.md#gating-a-commit-on-ci); publish only `main`.

## Task-specific guidance

| Task | Guide |
|---|---|
| Get hosted build feedback before integration | [Candidate workflow](AGENTS.md#local-execution-and-ci) |
| Update the SDK or dependencies | [Build and lockfile rules](AGENTS.md#build) |
| Measure performance | [Hosted benchmarks](AGENTS.md#benchmarks) |
| Change proofs or recovery models | [Proof tooling and trust policy](verification/README.md) |
| Publish a release | [Release process](PUBLISH.md) |
