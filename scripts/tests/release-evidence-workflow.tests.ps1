# Publication must consume exact-revision campaigns and preserve their success-only receipts.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-Job([string] $Workflow, [string] $Name) {
    return [regex]::Match($Workflow, '(?ms)^  ' + [regex]::Escape($Name) + ':\n(?<body>.*?)(?=^  [\w-]+:\n|\z)').Groups['body'].Value
}

function Assert-Publication([string] $Workflow) {
    $publish = Get-Job $Workflow 'publish'
    $guard = $publish.IndexOf('run: ./scripts/assert-release-readiness.ps1 -Sha $env:GITHUB_SHA', [StringComparison]::Ordinal)
    Assert-Contract ($guard -ge 0 -and $guard -lt $publish.IndexOf('name: Push to NuGet.org', [StringComparison]::Ordinal)) 'Publication must verify exact-revision release evidence before NuGet push.'
    Assert-Contract ($publish.Contains('GITHUB_TOKEN: ${{ github.token }}') -and $publish.Contains('persist-credentials: false')) 'Publication preflight needs read-only API access and a credential-free checkout.'
    Assert-Contract ($publish -notmatch 'continue-on-error:|if: always\(') 'Publication must not bypass failed evidence.'
    Assert-Contract ($Workflow.Contains('uses: ./.github/workflows/ci.yml') -and $publish.Contains('needs: ci') -and $publish -notmatch 'dotnet (build|pack) ') 'Publication must retain full CI and publish its package without rebuilding.'
    Assert-Contract ($publish.Contains("    permissions:`n      contents: read`n      actions: read`n      id-token: write`n") -and $Workflow -notmatch '(?m)^  id-token:' -and (Get-Job $Workflow 'ci') -notmatch 'id-token:') 'Only the publish job may request an OIDC token, retaining read-only repository and Actions access.'
    $login = [regex]::Match($publish, '(?ms)^      - name: NuGet login\n(?<body>.*?)(?=^      - name:|\z)').Groups['body'].Value
    Assert-Contract ($login -match '(?m)^        uses: NuGet/login@[0-9a-f]{40}(?: # [^\n]+)?$' -and $login.Contains("        id: nuget_login`n") -and $login.Contains("          user: symbiogenesis`n")) 'Trusted publishing must use the pinned official NuGet login action and the policy creator symbiogenesis.'
    $steps = @([regex]::Matches($publish, '(?m)^      - name: ([^\n]+)$') | ForEach-Object { $_.Groups[1].Value })
    Assert-Contract ($steps.Count -ge 2 -and $steps[-2] -ceq 'NuGet login' -and $steps[-1] -ceq 'Push to NuGet.org') 'NuGet login must occur immediately before package push.'
    $loginPosition = $publish.IndexOf('name: NuGet login', [StringComparison]::Ordinal)
    $versionPosition = $publish.IndexOf('name: Check package version', [StringComparison]::Ordinal)
    Assert-Contract ($guard -lt $loginPosition -and $versionPosition -ge 0 -and $versionPosition -lt $loginPosition) 'Trusted publishing may request credentials only after release readiness and artifact version checks.'
    Assert-Contract ($publish.Contains('NUGET_API_KEY: ${{ steps.nuget_login.outputs.NUGET_API_KEY }}') -and $publish.Contains('run: dotnet nuget push "nupkg/*.nupkg" --api-key $env:NUGET_API_KEY --source https://api.nuget.org/v3/index.json') -and $Workflow -notmatch '(?m)secrets\.NUGET_API_KEY|^\s*run:.*--skip-duplicate') 'NuGet push must consume only the temporary login output and fail on duplicate publication.'
    Assert-Contract ($publish.Contains("          global-json-file: global.json`n") -and $publish -notmatch 'dotnet-version:') 'Publication must install the SDK selected by root global.json.'
}

function Assert-Campaign([string] $Workflow, [string] $Kind) {
    $matrixJob = if ($Kind -ceq 'fuzz') { 'fuzz' } else { 'test' }
    Assert-Contract ($Workflow.Contains('run-name: "Validation: ' + $Kind + ' ${{ github.event.workflow_run.head_sha || github.sha }}"')) 'Campaign titles must identify the tested revision before results exist.'
    $record = Get-Job $Workflow 'release-evidence'
    Assert-Contract ($record.Contains('name: Record release evidence') -and $record.Contains("needs: [prerequisite, $matrixJob]") -and $record.Contains('if: ${{ success() && needs.prerequisite.outputs.run == ''true'' }}')) 'Only complete successful matrix execution may record release evidence.'
    Assert-Contract ($record.Contains('persist-credentials: false') -and $record.Contains('ref: ${{ env.JDW_VALIDATION_SHA }}') -and $record.Contains("run: ./scripts/write-release-evidence.ps1 -Kind $Kind")) 'Release evidence must inspect the immutable checked-out revision.'
    Assert-Contract ($record.Contains('name: release-evidence-' + $Kind + '-${{ env.JDW_VALIDATION_SHA }}') -and $record.Contains('path: artifacts/release-evidence/receipt.json') -and $record.Contains('if-no-files-found: error') -and $record.Contains('overwrite: true') -and $record.Contains('retention-days: 30')) 'Release receipts must retain exact identity, replace stale attempts and refuse missing output.'
    if ($Kind -ceq 'portability') {
        $test = Get-Job $Workflow 'test'
        Assert-Contract ($test.Contains('./scripts/assert-test-results.ps1 -ResultsDirectory artifacts/test-results/portability-${{ matrix.os }}-${{ matrix.tf }}')) 'Portability must reject unknown skip and explicit-test outcomes before recording success.'
    }
}

function Assert-Refusal([scriptblock] $Action, [string] $Expected) {
    $caught = $null
    try { & $Action } catch { $caught = $_.Exception.Message }
    Assert-Contract ($caught -and $caught.Contains($Expected)) "Expected refusal '$Expected', got '$caught'."
}

$publish = [IO.File]::ReadAllText((Join-Path $repository '.github/workflows/publish.yml')).Replace("`r`n", "`n")
Assert-Publication $publish
Assert-Refusal { Assert-Publication ($publish.Replace('run: ./scripts/assert-release-readiness.ps1 -Sha $env:GITHUB_SHA', 'run: echo unchecked')) } 'verify exact-revision'
Assert-Refusal { Assert-Publication ($publish.Replace('      id-token: write', '      id-token: read')) } 'Only the publish job'
Assert-Refusal { Assert-Publication ($publish.Replace("permissions:`n  contents: read", "permissions:`n  id-token: write`n  contents: read")) } 'Only the publish job'
Assert-Refusal { Assert-Publication ([regex]::Replace($publish, 'NuGet/login@[0-9a-f]{40}', 'NuGet/login@v1')) } 'pinned official NuGet login'
Assert-Refusal { Assert-Publication ($publish.Replace('user: symbiogenesis', 'user: another-user')) } 'policy creator symbiogenesis'
Assert-Refusal { Assert-Publication ($publish.Replace('      - name: Push to NuGet.org', "      - name: Unrelated work`n        run: echo delayed`n      - name: Push to NuGet.org")) } 'immediately before'
Assert-Refusal { Assert-Publication ($publish.Replace('name: Check package version', 'name: Unchecked artifact')) } 'artifact version checks'
Assert-Refusal { Assert-Publication ($publish.Replace('steps.nuget_login.outputs.NUGET_API_KEY', 'secrets.NUGET_API_KEY')) } 'temporary login output'
Assert-Refusal { Assert-Publication ($publish.Replace('dotnet nuget push ', 'dotnet nuget push --skip-duplicate ')) } 'fail on duplicate'
Assert-Refusal { Assert-Publication ($publish.Replace('global-json-file: global.json', 'dotnet-version: 10.0.x')) } 'SDK selected by root'
foreach ($kind in 'fuzz', 'portability') {
    $workflow = [IO.File]::ReadAllText((Join-Path $repository ".github/workflows/$kind.yml")).Replace("`r`n", "`n")
    Assert-Campaign $workflow $kind
    Assert-Refusal { Assert-Campaign ($workflow.Replace('success() && ', 'always() && ')) $kind } 'complete successful matrix'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('ref: ${{ env.JDW_VALIDATION_SHA }}', 'ref: main')) $kind } 'immutable checked-out'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('overwrite: true', 'overwrite: false')) $kind } 'replace stale attempts'
    Assert-Refusal { Assert-Campaign ($workflow.Replace('retention-days: 30', 'retention-days: 1')) $kind } 'retain exact identity'
}
'Release evidence workflow contracts passed.'
