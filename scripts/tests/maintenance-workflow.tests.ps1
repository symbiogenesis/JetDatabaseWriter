# Exercise task-specific dispatch validation without compiling or contacting GitHub.
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$workflow = [IO.File]::ReadAllText((Join-Path $root '.github/workflows/maintenance.yml')).Replace("`r`n", "`n")

function Assert-Contract([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

$validation = [regex]::Match($workflow, '(?ms)^      - name: Validate task inputs\n(?<body>.*?)(?=^      - |\z)').Groups['body'].Value
$command = [regex]::Match($validation, '(?ms)^        run: \|\n(?<command>.*)$').Groups['command'].Value
Assert-Contract ([bool]$command) 'Maintenance must validate task-specific inputs before setup.'
Assert-Contract ($command -notmatch '\$\{\{') 'Dispatch inputs must reach validation as environment data.'
$validate = [scriptblock]::Create([regex]::Replace($command, '(?m)^          ', ''))
Assert-Contract ($workflow.IndexOf('name: Validate task inputs', [StringComparison]::Ordinal) -lt $workflow.IndexOf('uses: actions/', [StringComparison]::Ordinal)) 'Invalid maintenance requests must fail before checkout or SDK installation.'

$candidateSteps = @('Prepare candidate with trusted coordinator', 'Setup candidate SDK', 'Check candidate', 'Upload diagnostics and reviewable patches')
$sdkSteps = @('Install requested SDK', 'Refresh and verify locks without building', 'Create SDK and lock patch', 'Upload reviewable patch')
foreach ($task in 'candidate', 'sdk-lock-refresh') {
    $names = if ($task -eq 'candidate') { $candidateSteps } else { $sdkSteps }
    foreach ($name in $names) {
        $step = [regex]::Match($workflow, ('(?ms)^      - name: ' + [regex]::Escape($name) + '\n(?<body>.*?)(?=^      - |\z)')).Groups['body'].Value
        Assert-Contract ($step -and $step.Contains("inputs.task == '$task'")) "The '$name' step must run only for '$task'."
    }
}

$saved = @{}
foreach ($name in 'MAINTENANCE_TASK', 'CANDIDATE_BASE', 'CANDIDATE_PATCH', 'SDK_VERSION') {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name)
}
function Assert-Request([string] $Name, [string] $Task, [string] $Base, [string] $Patch, [string] $Sdk, [string] $ExpectedError = '') {
    $env:MAINTENANCE_TASK = $Task
    $env:CANDIDATE_BASE = $Base
    $env:CANDIDATE_PATCH = $Patch
    $env:SDK_VERSION = $Sdk
    $caught = ''
    try { & $validate } catch { $caught = $_.Exception.Message }
    if ($ExpectedError) {
        Assert-Contract ($caught.Contains($ExpectedError)) "$Name expected '$ExpectedError', got '$caught'."
    } else { Assert-Contract (-not $caught) "$Name failed: $caught" }
    "PASS: $Name"
}
try {
    $sha = 'a' * 40
    Assert-Request 'candidate needs no SDK version' 'candidate' $sha 'cGF0Y2g=' ''
    Assert-Request 'SDK refresh needs no candidate patch' 'sdk-lock-refresh' '' '' '10.0.403'
    Assert-Request 'unknown task is refused' 'unknown' '' '' '' 'Unknown maintenance task'
    Assert-Request 'missing candidate base is refused' 'candidate' '' 'cGF0Y2g=' '' 'Candidate checks require'
    Assert-Request 'invalid candidate base is refused' 'candidate' 'main' 'cGF0Y2g=' '' 'Candidate checks require'
    Assert-Request 'missing candidate patch is refused' 'candidate' $sha '' '' 'Candidate checks require'
    Assert-Request 'blank candidate patch is refused' 'candidate' $sha '   ' '' 'Candidate checks require'
    Assert-Request 'missing SDK is refused' 'sdk-lock-refresh' '' '' '' 'exact .NET 10 SDK version'
    Assert-Request 'floating SDK is refused' 'sdk-lock-refresh' '' '' '10.0.x' 'exact .NET 10 SDK version'
    Assert-Request 'other SDK major is refused' 'sdk-lock-refresh' '' '' '11.0.100' 'exact .NET 10 SDK version'
    Assert-Request 'SDK remains data' 'sdk-lock-refresh' '' '' '10.0.403; throw "executed input"' 'exact .NET 10 SDK version'
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name]) }
}
