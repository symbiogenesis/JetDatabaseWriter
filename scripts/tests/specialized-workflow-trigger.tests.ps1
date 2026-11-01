# Evaluate the supported GitHub condition syntax against complete synthetic event payloads.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
$repository = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

function Test-PrerequisiteCondition([string] $Workflow, [hashtable] $Payload) {
    $condition = [regex]::Match($Workflow, '(?ms)^  prerequisite:\n    if: >-\n\s+\$\{\{ (?<expression>.*?) \}\}').Groups['expression'].Value
    if (-not $condition) { throw 'The specialized prerequisite condition was not found.' }
    # This translator covers only the operators and startsWith function used by these guards.
    $condition = $condition.Replace('github.event.workflow_run.', '$Payload.workflow_run.').Replace('github.event_name', '$Payload.event_name').Replace('github.ref', '$Payload.ref').Replace('github.repository', '$Payload.repository')
    $condition = $condition.Replace(' == ', ' -eq ').Replace(' != ', ' -ne ').Replace(' &&', ' -and').Replace(' ||', ' -or')
    $condition = [regex]::Replace($condition, 'startsWith\((\$Payload\.workflow_run\.path), (''[^'']*'')\)', '$1.StartsWith($2, [StringComparison]::OrdinalIgnoreCase)')
    return [bool](& ([scriptblock]::Create($condition)))
}

foreach ($workflowName in 'fuzz', 'portability') {
    $workflow = [IO.File]::ReadAllText((Join-Path $repository ".github/workflows/$workflowName.yml")).Replace("`r`n", "`n")
    foreach ($case in @(
        @{ Name = 'successful main push'; Expected = $true }
        @{ Name = 'successful suffixed manual main CI'; Event = 'workflow_dispatch'; Path = '.github/workflows/ci.yml@main'; Expected = $true }
        @{ Name = 'successful full-ref manual main CI'; Event = 'workflow_dispatch'; Path = '.github/workflows/ci.yml@refs/heads/main'; Expected = $true }
        @{ Name = 'different workflow'; Path = '.github/workflows/other.yml'; Expected = $false }
        @{ Name = 'lookalike workflow prefix'; Path = '.github/workflows/ci.yml-other@main'; Expected = $false }
        @{ Name = 'pull request event'; Event = 'pull_request'; Expected = $false }
        @{ Name = 'foreign repository'; Repository = 'foreign/repo'; Expected = $false }
        @{ Name = 'non-main branch'; Branch = 'work'; Expected = $false }
        @{ Name = 'unsuccessful CI'; Conclusion = 'failure'; Expected = $false }
        @{ Name = 'direct main manual campaign'; Trigger = 'workflow_dispatch'; Expected = $true }
        @{ Name = 'direct non-main manual campaign'; Trigger = 'workflow_dispatch'; Ref = 'refs/heads/work'; Expected = $false }
    )) {
        $payload = @{
            ref = 'refs/heads/main'; event_name = 'workflow_run'; repository = 'example/repo'
            workflow_run = @{ conclusion = 'success'; head_branch = 'main'; head_repository = @{ full_name = 'example/repo' }; path = '.github/workflows/ci.yml'; event = 'push' }
        }
        if ($case.ContainsKey('Event')) { $payload.workflow_run.event = $case.Event }
        if ($case.ContainsKey('Path')) { $payload.workflow_run.path = $case.Path }
        if ($case.ContainsKey('Repository')) { $payload.workflow_run.head_repository.full_name = $case.Repository }
        if ($case.ContainsKey('Branch')) { $payload.workflow_run.head_branch = $case.Branch }
        if ($case.ContainsKey('Conclusion')) { $payload.workflow_run.conclusion = $case.Conclusion }
        if ($case.ContainsKey('Trigger')) { $payload.event_name = $case.Trigger; $payload.workflow_run = $null }
        if ($case.ContainsKey('Ref')) { $payload.ref = $case.Ref }
        if ((Test-PrerequisiteCondition $workflow $payload) -ne $case.Expected) { throw "$workflowName rejected or accepted '$($case.Name)' incorrectly." }
        "PASS: $workflowName $($case.Name)"
    }
}
