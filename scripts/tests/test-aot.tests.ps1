# Exercises the AOT coordinator with command doubles; no builds or generated consumers run.
[CmdletBinding()]
param([string] $OutputDirectory)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (-not $OutputDirectory) {
    $OutputDirectory = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-test-aot-' + [Guid]::NewGuid().ToString('N'))
}
$testRoot = [IO.Path]::GetFullPath($OutputDirectory)
$fixtureScripts = Join-Path $testRoot 'fixture/scripts'
[IO.Directory]::CreateDirectory($fixtureScripts) | Out-Null
$subject = Join-Path $fixtureScripts 'test-aot.ps1'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../test-aot.ps1') -Destination $subject
Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../restore-dependencies.ps1') -Destination $fixtureScripts
$aotTestContext = [pscustomobject]@{
    Calls = [Collections.Generic.List[object]]::new()
    Executions = [Collections.Generic.List[object]]::new()
    ConsumerFunctions = [Collections.Generic.List[string]]::new()
    Failure = ''
}

function Assert-Check([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}

function Get-Option([string[]] $Arguments, [string] $Name) {
    $index = [Array]::IndexOf($Arguments, $Name)
    if ($index -ge 0) { return $Arguments[$index + 1] }
    return ''
}

function dotnet {
    $arguments = [string[]]$args
    $stage = if ($arguments[0] -eq 'restore') {
        'restore-' + (@($arguments | Where-Object { $_ -like '-p:SmokeMode=*' })[0].Split('=')[1])
    } elseif ($arguments -contains '-p:SmokeMode=seed') {
        'seed'
    } elseif ($arguments[0] -eq 'run') {
        $(if ($arguments -contains '--records') { 'records-scaffold' } else { 'classes-scaffold' })
    } elseif ($arguments[0] -eq 'publish') {
        [IO.Path]::GetFileName((Get-Option $arguments '-o')) + '-publish'
    } else {
        throw "Unexpected dotnet command: $($arguments -join ' ')"
    }
    $aotTestContext.Calls.Add([pscustomobject]@{ Stage = $stage; Arguments = $arguments })
    $global:LASTEXITCODE = if ($aotTestContext.Failure -eq $stage) { 7 } else { 0 }
    if ($LASTEXITCODE -ne 0) { return }
    if ($stage.StartsWith('restore-', [StringComparison]::Ordinal)) {
        return
    } elseif ($stage -eq 'seed') {
        [IO.File]::WriteAllText($arguments[-1], 'Seed database')
    } elseif ($stage.EndsWith('-scaffold', [StringComparison]::Ordinal)) {
        $models = Get-Option $arguments '--output'
        [IO.Directory]::CreateDirectory($models) | Out-Null
        [IO.File]::WriteAllText((Join-Path $models 'People.cs'), '// Generated model')
    } else {
        $publish = Get-Option $arguments '-o'
        [IO.Directory]::CreateDirectory($publish) | Out-Null
        $consumerPath = Join-Path $publish 'AotSmoke.exe'
        $context = $aotTestContext
        $consumer = {
            param([string] $Database)
            $consumerStage = [IO.Path]::GetFileNameWithoutExtension($Database)
            $context.Executions.Add([pscustomobject]@{ Stage = $consumerStage; Path = $consumerPath; Database = $Database })
            Assert-Check ([IO.File]::ReadAllText($Database) -eq 'Seed database') "The $consumerStage consumer did not receive a fresh seed database."
            [IO.File]::AppendAllText($Database, ' Modified')
            $global:LASTEXITCODE = if ($context.Failure -eq $consumerStage + '-consumer') { 9 } else { 0 }
        }.GetNewClosure()
        Set-Item -LiteralPath ('Function:\global:' + $consumerPath) -Value $consumer
        $aotTestContext.ConsumerFunctions.Add($consumerPath)
    }
}

function Invoke-Scenario([string] $Name, [string] $Failure = '') {
    $aotTestContext.Calls.Clear()
    $aotTestContext.Executions.Clear()
    $aotTestContext.Failure = $Failure
    $output = Join-Path $testRoot $Name
    $location = Get-Location
    $caught = $null
    try { & $subject -OutputDirectory $output }
    catch { $caught = $_.Exception.Message }
    finally {
        foreach ($consumerPath in $aotTestContext.ConsumerFunctions) {
            Remove-Item -LiteralPath ('Function:\' + $consumerPath)
        }
        $aotTestContext.ConsumerFunctions.Clear()
    }
    Assert-Check ((Get-Location).Path -eq $location.Path) "$Name did not restore the caller's working directory."
    return [pscustomobject]@{ Output = $output; Error = $caught }
}

$pass = Invoke-Scenario 'pass'
Assert-Check (-not $pass.Error) "Passing AOT commands failed: $($pass.Error)"
Assert-Check ($aotTestContext.Calls.Count -eq 10) 'Every lock must be verified before the seed, two scaffolds and four publishes run.'
$expectedStages = 'restore-seed,restore-trimmed,restore-native,seed,classes-scaffold,classes-trimmed-publish,classes-native-publish,records-scaffold,records-trimmed-publish,records-native-publish'
Assert-Check (($aotTestContext.Calls.Stage -join ',') -eq $expectedStages) 'The full classes/records and trimmed/native matrix changed.'
$artifacts = Join-Path $pass.Output 'build'
foreach ($call in $aotTestContext.Calls) {
    if ($call.Stage.StartsWith('restore-', [StringComparison]::Ordinal)) {
        Assert-Check ($call.Arguments -contains '--locked-mode') "$($call.Stage) did not verify the lockfile."
        continue
    }
    Assert-Check ($call.Arguments -contains '-p:AnalyzeOnly=AotSmoke') "$($call.Stage) repeated library/tool analysis or disabled consumer analysis."
    Assert-Check ($call.Arguments -contains '-p:RestoreLockedMode=true') "$($call.Stage) did not preserve locked restoration."
    Assert-Check ((Get-Option $call.Arguments '--artifacts-path') -eq $artifacts) "$($call.Stage) did not reuse the established build artifacts."
    Assert-Check ((Get-Option $call.Arguments '-c') -eq 'Release') "$($call.Stage) did not use Release."
    Assert-Check (-not ($call.Arguments -match '^-p:(RunAnalyzers|EnableNETAnalyzers|TreatWarningsAsErrors|ILLinkTreatWarningsAsErrors|IlcTreatWarningsAsErrors)=false$')) "$($call.Stage) weakened consumer validation."
}
$classes = @($aotTestContext.Calls | Where-Object Stage -eq 'classes-scaffold')[0].Arguments
$records = @($aotTestContext.Calls | Where-Object Stage -eq 'records-scaffold')[0].Arguments
Assert-Check ($classes -notcontains '--no-build' -and $classes -notcontains '--no-restore') 'The first scaffold invocation must build and restore the tool.'
Assert-Check ($records -contains '--no-build' -and $records -contains '--no-restore') 'The second scaffold invocation must reuse the first tool build and restore.'
Assert-Check ($records -contains '--records') 'The record scaffold option was lost.'
foreach ($publish in @($aotTestContext.Calls | Where-Object { $_.Stage.EndsWith('-publish', [StringComparison]::Ordinal) })) {
    $kind, $mode = $publish.Stage.Split('-')[0..1]
    Assert-Check ($publish.Arguments -contains ('-p:GeneratedModels=' + (Join-Path $pass.Output ($kind + '-models')))) "$($publish.Stage) did not compile its generated models."
    Assert-Check ($publish.Arguments -contains ('-p:SmokeMode=' + $mode)) "$($publish.Stage) did not enable the matching consumer mode."
    Assert-Check ($publish.Arguments -notcontains '--no-build') "$($publish.Stage) skipped consumer compilation."
}
Assert-Check (($aotTestContext.Executions.Stage -join ',') -eq 'classes-trimmed,classes-native,records-trimmed,records-native') 'Every generated consumer must execute.'
Assert-Check (@($aotTestContext.Executions.Database | Select-Object -Unique).Count -eq 4) 'Consumers shared their mutable databases.'

$project = [xml][IO.File]::ReadAllText((Join-Path $PSScriptRoot '../AotSmoke/AotSmoke.csproj'))
Assert-Check ($project.SelectSingleNode('/Project/PropertyGroup/PublishTrimmed').InnerText -eq 'true') 'The trimmed consumer project must enable trimming.'
Assert-Check ($project.SelectSingleNode('/Project/PropertyGroup/PublishAot').InnerText -eq 'true') 'The native consumer project must enable NativeAOT.'

foreach ($failure in @('restore-seed', 'restore-native', 'seed', 'classes-scaffold', 'records-scaffold', 'classes-trimmed-publish', 'records-native-publish', 'classes-native-consumer', 'records-native-consumer')) {
    $scenario = Invoke-Scenario $failure $failure
    Assert-Check ([bool]$scenario.Error) "$failure falsely passed."
    $expectedExit = if ($failure.EndsWith('-consumer', [StringComparison]::Ordinal)) { '9' } else { '7' }
    Assert-Check ($scenario.Error.Contains('failed with exit code ' + $expectedExit)) "$failure did not report its failing command: $($scenario.Error)"
    $lastStage = if ($failure.EndsWith('-consumer', [StringComparison]::Ordinal)) {
        $aotTestContext.Executions[-1].Stage + '-consumer'
    } else {
        $aotTestContext.Calls[-1].Stage
    }
    Assert-Check ($lastStage -eq $failure) "$failure continued after a failed command."
}
Write-Host "All mocked AOT checks passed. Artifacts: $testRoot"
