# Tests structural scheduling and bounded shrinking using synthetic bytes only.
[CmdletBinding()]
param()
$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '../fuzz-inputs.ps1')
function Assert-Fuzz([bool] $Condition, [string] $Message) {
    if (-not $Condition) { throw $Message }
}
$fixtureRoot = Join-Path ([IO.Path]::GetTempPath()) ('JetDatabaseWriter-fuzz-metadata-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixtureRoot) | Out-Null
foreach ($version in @(0, 1, 2)) {
    $size = if ($version -eq 0) { 2048 } else { 4096 }
    $count = if ($version -eq 0) { 8 } else { 12 }
    $slots = $count + 2
    $used = if ($version -eq 0) { 35 } else { 55 }
    $bytes = [byte[]]::new($size * 5)
    [Text.Encoding]::ASCII.GetBytes("Standard Jet DB`0").CopyTo($bytes, 4)
    $bytes[0x14] = [byte]$version
    Protect-FuzzHeader $bytes
    $bytes[$size] = 1
    $bytes[$size + $count] = 1
    [BitConverter]::GetBytes([uint16]($size - 69)).CopyTo($bytes, $size + $slots)
    $bytes[$size * 2] = 2
    $bytes[$size * 2 + $used + 1] = 1
    $bytes[$size * 3] = 4
    $bytes[$size * 4] = 5
    $fields = @(Get-FuzzStructuralFields $bytes)
    foreach ($family in @('Header', 'Data.Count', 'Data.Pointer', 'Schema.Count', 'Schema.Pointer', 'Index.Count', 'Index.Pointer', 'Usage.Pointer', 'Encryption.Header')) {
        Assert-Fuzz (@($fields | Where-Object Family -eq $family).Count -gt 0) "Version $version is missing $family."
    }
    $rowPointer = @($fields | Where-Object Name -eq 'Usage.RowPointer')[0]
    Assert-Fuzz ($rowPointer.Offset -eq ($size * 2 - 68)) 'Usage-map mutation did not follow the real TDEF row pointer.'
    $rowCount = @($fields | Where-Object Name -eq 'Data.NumRows')[0]
    Assert-Fuzz ($rowCount.Offset -eq ($size + $count)) 'Data row-count metadata used the wrong format layout.'
    $mutation = New-FuzzStructuralInput $bytes $rowCount 2
    Assert-Fuzz ($mutation.Mutation.Offset -eq $rowCount.Offset) 'Mutation provenance omitted its exact field offset.'
    Assert-Fuzz ($mutation.Bytes[$rowCount.Offset] -eq 255 -and $mutation.Bytes[$rowCount.Offset + 1] -eq 255) 'Count boundary mutation was not encoded little endian.'
    Assert-Fuzz ($bytes[$rowCount.Offset] -eq 1) 'Structural generation changed the corpus bytes.'
    $prefix = @($fields | Where-Object Name -eq 'Index.PrefixLength')[0]
    Assert-Fuzz ($prefix.Width -eq $(if ($version -eq 0) { 1 } else { 2 })) 'An index prefix count used the wrong native field width.'
    $prefixMutation = New-FuzzStructuralInput $bytes $prefix 2
    Assert-Fuzz ($prefixMutation.Bytes[$prefix.Offset + $prefix.Width] -eq $bytes[$prefix.Offset + $prefix.Width]) 'An index prefix mutation changed the neighboring field.'
    $key = @($fields | Where-Object Name -eq 'Encryption.EncodingKey')[0]
    $unmasked = [byte[]]$bytes.Clone()
    Protect-FuzzHeader $unmasked
    $changedKey = New-FuzzStructuralInput $bytes $key 2
    Protect-FuzzHeader $changedKey.Bytes
    Assert-Fuzz ([BitConverter]::ToUInt32($changedKey.Bytes, 0x3E) -eq [uint32]::MaxValue) 'Masked header mutation did not set the logical field boundary.'
    if ($version -eq 2) {
        $bytes[0x299] = 12
        $fields = @(Get-FuzzStructuralFields $bytes)
        Assert-Fuzz (@($fields | Where-Object Family -eq 'Encryption.Descriptor').Count -gt 0) 'Native ACE descriptor lengths were not targeted.'
    }
    [IO.File]::WriteAllBytes((Join-Path $fixtureRoot "synthetic-$version.mdb"), $bytes)
}
$container = [byte[]]::new(512)
[byte[]](0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1) | ForEach-Object -Begin { $i = 0 } -Process { $container[$i++] = $_ }
Assert-Fuzz (@(Get-FuzzStructuralFields $container | Where-Object Family -eq 'Encryption.Container').Count -ge 4) 'Compound-file encryption count/pointer fields were not targeted.'
Assert-Fuzz (@(Get-FuzzStructuralFields ([byte[]](0, 1, 2))).Count -eq 0) 'A short input was treated as a structured database.'
$wrong = [byte[]]::new(4096)
$wrong[0x14] = 1
Assert-Fuzz (@(Get-FuzzStructuralFields $wrong).Count -eq 0) 'Unknown signatures were treated as structured databases.'
$corpus = @(Get-ChildItem -LiteralPath $fixtureRoot -File | Sort-Object FullName)
$plan = Get-FuzzReaderPlan $corpus 42
$repeated = Get-FuzzReaderPlan $corpus 42
Assert-Fuzz ($plan.Entries.Count -lt 64) 'The mandatory structural schedule exceeds the default hosted campaign.'
Assert-Fuzz (($plan.AvailableGroups -join ',') -eq (($plan.Entries | Where-Object Family -ne 'Control' | ForEach-Object { "$($_.Format)/$($_.Family)" }) -join ',')) 'The structural schedule skipped an available format/family.'
Assert-Fuzz (($plan.Entries.Field.Offset -join ',') -eq ($repeated.Entries.Field.Offset -join ',')) 'The seed did not reproduce the structural schedule.'
Assert-Fuzz (@($plan.Entries | Select-Object -First 3 | Where-Object Family -eq 'Control').Count -eq 3) 'Healthy controls did not precede structural cases for every format.'
Assert-Fuzz (@($plan.Catalog | Where-Object { $_.SHA256.Length -ne 64 }).Count -eq 0) 'The schedule omitted corpus identity hashes.'

$state = @{ Calls = 0 }
$probe = {
    param([byte[]] $Candidate)
    $state.Calls++
    [pscustomobject]@{ Signature = if ($Candidate -contains 99) { 'Failed:KnownAssertion' } else { 'Failed:DifferentAssertion' } }
}
$original = [byte[]](1, 2, 99, 4, 5, 6, 7, 8)
$minimum = Invoke-FuzzMinimization -Bytes $original -Signature 'Failed:KnownAssertion' -Probe $probe -MaxAttempts 20 -BudgetSeconds 10
Assert-Fuzz ($minimum.Bytes.Length -eq 1 -and $minimum.Bytes[0] -eq 99) 'Shrinking failed to retain the triggering byte.'
Assert-Fuzz ($minimum.Attempts -le 20 -and $state.Calls -eq $minimum.Attempts) 'Shrinking exceeded the attempt budget.'
Assert-Fuzz ($minimum.Signature -eq 'Failed:KnownAssertion') 'Shrinking changed the failure signature.'
Assert-Fuzz ($original.Length -eq 8 -and $original[2] -eq 99) 'Shrinking changed the original reproduction.'
$bounded = Invoke-FuzzMinimization -Bytes $original -Signature 'Failed:KnownAssertion' -Probe $probe -MaxAttempts 1 -BudgetSeconds 10
Assert-Fuzz ($bounded.Attempts -eq 1) 'The minimizer ignored its explicit attempt bound.'
$reject = Invoke-FuzzMinimization -Bytes $original -Signature 'Failed:KnownAssertion' -Probe { [pscustomobject]@{ Signature = 'Failed:Other' } } -MaxAttempts 4 -BudgetSeconds 10
Assert-Fuzz ($reject.Bytes.Length -eq $original.Length) 'A distinct failure was accepted as a reproduction.'
$timeout = Invoke-FuzzMinimization -Bytes $original -Signature 'Timeout' -Probe { [pscustomobject]@{ Signature = 'Timeout' } } -MaxAttempts 16 -BudgetSeconds 10
Assert-Fuzz ($timeout.Attempts -le 2) 'Hanging reproductions received an unbounded retry budget.'
$memory = Invoke-FuzzMinimization -Bytes $original -Signature 'MemoryLimit' -Probe { [pscustomobject]@{ Signature = 'MemoryLimit' } } -MaxAttempts 16 -BudgetSeconds 10
Assert-Fuzz ($memory.Attempts -le 2) 'Resource-limit reproductions received an excessive retry budget.'
Write-Host 'All synthetic fuzz input checks passed.'
