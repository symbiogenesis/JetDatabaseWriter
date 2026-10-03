# Publishing JetDatabaseWriter

Only the `JetDatabaseWriter` library is published to nuget.org. The scaffolding CLI, the tests, the benchmarks and the format probe are not packable.

## How the version is set

[MinVer](https://github.com/adamralph/minver) sets the package, assembly file and informational versions from git tags when the library builds. The settings are in `JetDatabaseWriter/JetDatabaseWriter.csproj`:

- `MinVerTagPrefix` is `v`, so the tag `v4.0.0` gives version `4.0.0`.
- `MinVerMinimumMajorMinor` is `4.0`, so until a `v4.*` tag exists, every build is `4.0.0-alpha.0.<height>`, where the height is the number of commits since the last tag (or since the first commit).
- A commit after `vX.Y.Z` builds as `X.Y.(Z+1)-alpha.0.<height>`. Only tagged commits are published, so these prerelease versions never reach nuget.org.
- `AssemblyVersion` stays `<major>.0.0.0` for every 4.x release, so binding redirects are not needed within a major version. `FileVersion` is `X.Y.Z.0` and `InformationalVersion` is the full version plus the commit hash.
- A build outside a git clone, such as from a source archive, warns `MINVER1001` and builds `4.0.0-alpha.0`.

CI checks out the full history (`fetch-depth: 0`) so MinVer sees every tag.

## Releasing a version

1. Make sure the commit you want to release is on `main` and its CI run passed.
2. Tag it and push only that tag:

   ```bash
   git tag v4.0.0 <commit>
   git push origin v4.0.0
   ```

   A prerelease uses a SemVer prerelease tag, such as `v4.1.0-beta.1`.
3. The tag runs `.github/workflows/publish.yml`. It calls `ci.yml`, which restores in locked mode, builds the solution in Release, runs both test legs, packs the library and uploads the package as the `nupkg` artifact. The publish job then downloads that artifact, checks that it holds `JetDatabaseWriter.<tag without the v>.nupkg`, and pushes it to nuget.org with the `NUGET_API_KEY` secret. Nothing is rebuilt between the tests and the push.
4. Check that the version appears at <https://www.nuget.org/packages/JetDatabaseWriter/> and at <https://api.nuget.org/v3-flatcontainer/jetdatabasewriter/index.json>.

To start the prereleases of a new major or minor version before its first tag, raise `MinVerMinimumMajorMinor` (for example to `5.0`).

### When something goes wrong

- **The version check fails.** MinVer did not use the tag, usually because it is not a `vX.Y.Z` SemVer version, so CI packed a prerelease. Delete the tag on the remote, fix it, and push it again.
- **The push fails with 409 Conflict.** nuget.org already has that version. The workflow does not pass `--skip-duplicate`, so a duplicate fails the run instead of looking like a publish. A version number can't be reused on nuget.org; tag the next patch version.
- **The push fails for another reason** (for example an expired API key). Fix the cause, then re-run the failed jobs of the same workflow run from the Actions page. The `nupkg` artifact is kept for 7 days; after that, re-run all jobs so CI packs it again.
- **A published version is broken.** Unlist it on nuget.org (it can't be deleted) and release the next patch version.

### Tags that must not be pushed

Some local clones have the release tags `v1.0.0` through `v2.2.0` of the upstream `JetDatabaseReader` package this project grew out of. They are not on `origin`. Never push them: a pushed `v*` tag runs that commit's own `publish.yml`, which packs `JetDatabaseReader` and would push it with this repository's API key. Push single tags (`git push origin <tag>`), never `git push --tags`.

## Symbols

`DotNet.ReproducibleBuilds` embeds the PDB in the DLL, so the package carries its own symbols and Source Link information and there is no separate `.snupkg`.

## Package validation

Every pack runs .NET package validation (`EnablePackageValidation`). Today it checks that each public API of the `netstandard2.1` build also exists in the `net10.0` build, so a public member compiled only into the `netstandard2.1` build fails the pack (`CP0001` or `CP0002`).

Once a version is on nuget.org, also validate against it:

1. Add `<PackageValidationBaselineVersion>4.0.0</PackageValidationBaselineVersion>` to the library project. The baseline package must exist on nuget.org, or the restore fails with `NU1101`; before the first release, <https://api.nuget.org/v3-flatcontainer/jetdatabasewriter/index.json> returns 404.
2. After each release, move the baseline to the version just published.
3. A pack that breaks the baseline's public API then fails (for example `CP0002` for a removed member). For an intentional break, either release a new major version or generate a suppression file with `dotnet pack JetDatabaseWriter/JetDatabaseWriter.csproj -c Release -p:ApiCompatGenerateSuppressionFile=true`, review the generated `CompatibilitySuppressions.xml`, and commit it.

## Packing locally

```bash
dotnet pack JetDatabaseWriter/JetDatabaseWriter.csproj -c Release -o artifacts/nupkg
```

This produces the same package CI would, with the version MinVer computes for your checkout.
