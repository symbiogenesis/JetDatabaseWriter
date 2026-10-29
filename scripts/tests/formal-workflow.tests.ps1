# Guard the mandatory proof/trust lane and its retained failure evidence.
# This checks the repository's fixed YAML shape, not arbitrary YAML syntax.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$workflow = [IO.File]::ReadAllText((Join-Path $repoRoot '.github/workflows/ci.yml')).Replace("`r`n", "`n")

function Assert-ProofJob([string] $Workflow) {
    $match = [regex]::Match($Workflow, '(?ms)^  formal:\n(?<body>.*?)(?=^  [\w-]+:\n|\z)')
    if (-not $match.Success) { throw 'The mandatory formal job is missing.' }
    $job = $match.Groups['body'].Value
    if ($job -notmatch '(?m)^    runs-on: ubuntu-22\.04$' -or
        $job -notmatch '(?m)^    timeout-minutes: 15$' -or
        $job -notmatch '(?m)^    needs: changes$' -or
        $job -notmatch "(?m)^    if: needs\.changes\.outputs\.formal == 'true'$" -or
        [regex]::Matches($job, '(?m)^    (?:if|needs):').Count -ne 2 -or
        $job -match 'continue-on-error:|--(?:no-verify|allow-warnings)|-Skip|-UpdateGeneratedSource') {
        throw 'The proof lane must depend only on reviewed path selection and fail closed within its time bound.'
    }
    $checkout = 'actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1'
    if (-not $job.Contains($checkout)) { throw 'The proof lane must pin checkout.' }
    $install = [regex]::Match($job, '(?ms)^      - name: Install pinned verifier\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    $verify = [regex]::Match($job, '(?ms)^      - name: Verify proofs and trust policy\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    foreach ($step in @($install, $verify)) {
        if (-not $step -or $step -match '(?m)^        if:' -or $step -notmatch '(?m)^        shell: pwsh$') {
            throw 'Install and verification must be unconditional PowerShell steps.'
        }
    }
    if (-not $install.Contains('./scripts/install-dafny.ps1 -Destination "${{ runner.temp }}/jdw-dafny-${{ github.run_id }}-${{ github.run_attempt }}"') -or
        -not $verify.Contains('./scripts/verify-proofs.ps1') -or
        -not $verify.Contains('-DafnyPath "${{ runner.temp }}/jdw-dafny-${{ github.run_id }}-${{ github.run_attempt }}/dafny/dafny"') -or
        -not $verify.Contains('-OutputDirectory "${{ runner.temp }}/jdw-proof-results"')) {
        throw 'The proof lane must use the integrity-pinned installer and verifier with external absolute output paths.'
    }
    $upload = [regex]::Match($job, '(?ms)^      - name: Upload proof diagnostics\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    if (-not $upload.Contains('if: ${{ !cancelled() }}') -or
        -not $upload.Contains('uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a') -or
        -not $upload.Contains('path: ${{ runner.temp }}/jdw-proof-results/')) {
        throw 'Proof diagnostics must survive failed verification.'
    }
    $checks = [regex]::Match($Workflow, '(?ms)^  checks:\n(?<body>.*?)(?=^  [\w-]+:\n|\z)').Groups['body'].Value
    if (-not $checks -or $checks -match '(?m)^    (?:if|needs|continue-on-error):') { throw 'Proof review must run unconditionally, including when expensive lanes are skipped.' }
    $review = [regex]::Match($checks, '(?ms)^      - name: Check proof/trust review records\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    if (-not $review -or $review -match '(?m)^        if:' -or $review -notmatch '(?m)^        shell: pwsh$' -or
        -not $review.Contains('./scripts/check-proof-review.ps1 -OutputDirectory "${{ runner.temp }}/jdw-proof-review"')) {
        throw 'Mapped implementation and pin changes must have unconditional proof/trust review freshness checks.'
    }
    $reviewUpload = [regex]::Match($checks, '(?ms)^      - name: Upload proof review\n(?<step>.*?)(?=^      - |\z)').Groups['step'].Value
    if (-not $reviewUpload.Contains('if: ${{ !cancelled() }}') -or
        -not $reviewUpload.Contains('uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a') -or
        -not $reviewUpload.Contains('path: ${{ runner.temp }}/jdw-proof-review/') -or
        -not $reviewUpload.Contains('if-no-files-found: error') -or
        -not $reviewUpload.Contains('retention-days: 30') -or -not $upload.Contains('retention-days: 30')) {
        throw 'Review outcomes and proof counterexample diagnostics must be retained, including failed gates.'
    }
}

function Assert-Case([string] $Name, [string] $Workflow, [bool] $ShouldFail) {
    $failed = $false
    try { Assert-ProofJob $Workflow } catch { $failed = $true }
    if ($failed -ne $ShouldFail) { throw "Unexpected workflow verdict: $Name" }
    "PASS: $Name"
}

Assert-Case 'required independent proof lane' $workflow $false
Assert-Case 'missing proof lane' ($workflow.Replace('  formal:', '  removed:')) $true
Assert-Case 'optional proof lane' ($workflow.Replace('    name: Verify recovery models (Dafny)', "    if: false`n    name: Verify recovery models (Dafny)")) $true
Assert-Case 'ignored verification failure' ($workflow.Replace('    name: Verify recovery models (Dafny)', "    continue-on-error: true`n    name: Verify recovery models (Dafny)")) $true
Assert-Case 'conditional verification' ($workflow.Replace('      - name: Verify proofs and trust policy', "      - name: Verify proofs and trust policy`n        if: false")) $true
Assert-Case 'unbounded proof process' ($workflow.Replace('    timeout-minutes: 15', '    timeout-minutes: 360')) $true
Assert-Case 'unpinned installation' ($workflow.Replace('./scripts/install-dafny.ps1', 'curl latest')) $true
Assert-Case 'verification omitted' ($workflow.Replace('./scripts/verify-proofs.ps1', 'echo verified')) $true
Assert-Case 'audit bypass option' ($workflow.Replace('./scripts/verify-proofs.ps1', './scripts/verify-proofs.ps1 -SkipAudit')) $true
Assert-Case 'generated freshness cannot repair itself in CI' ($workflow.Replace('./scripts/verify-proofs.ps1', './scripts/verify-proofs.ps1 -UpdateGeneratedSource')) $true
Assert-Case 'failure diagnostics lost' ($workflow.Replace('if: ${{ !cancelled() }}', 'if: success()')) $true
Assert-Case 'review gate omitted' ($workflow.Replace('./scripts/check-proof-review.ps1', 'echo reviewed')) $true
Assert-Case 'review gate conditional' ($workflow.Replace('      - name: Check proof/trust review records', "      - name: Check proof/trust review records`n        if: false")) $true
Assert-Case 'review report lost on failure' ($workflow.Replace('      - name: Upload proof review' + "`n" + '        if: ${{ !cancelled() }}', '      - name: Upload proof review' + "`n" + '        if: success()')) $true
Assert-Case 'counterexamples retention shortened' ($workflow.Replace('retention-days: 30', 'retention-days: 1')) $true
