# Deterministic mutation metadata and shrinking helpers; these never launch fuzzers.
function Protect-FuzzHeader([byte[]] $Bytes) {
    # Access's fixed header RC4 mask (EncryptionManager.HeaderRc4Key).
    $state = [int[]](0..255)
    $key = @(0xC7, 0xDA, 0x39, 0x6B)
    $j = 0
    for ($i = 0; $i -lt 256; $i++) {
        $j = ($j + $state[$i] + $key[$i % 4]) -band 255
        $state[$i], $state[$j] = $state[$j], $state[$i]
    }
    $i = 0
    $j = 0
    $end = [Math]::Min($Bytes.Length, $(if ($Bytes[0x14] -eq 0) { 0x96 } else { 0x98 }))
    for ($offset = 0x18; $offset -lt $end; $offset++) {
        $i = ($i + 1) -band 255
        $j = ($j + $state[$i]) -band 255
        $state[$i], $state[$j] = $state[$j], $state[$i]
        $Bytes[$offset] = $Bytes[$offset] -bxor $state[($state[$i] + $state[$j]) -band 255]
    }
}

function Get-FuzzStructuralFields([byte[]] $Bytes) {
    $fields = [Collections.Generic.List[object]]::new()
    $format = 'CFB'
    function Add-Field([string] $Family, [string] $Name, [int] $Offset, [int] $Width, [bool] $Masked = $false) {
        if ($Offset -ge 0 -and $Offset + $Width -le $Bytes.Length) {
            $fields.Add([pscustomobject]@{ Family = $Family; Name = $Name; Offset = $Offset; Width = $Width; Masked = $Masked; Format = $format })
        }
    }
    if ($Bytes.Length -ge 512 -and [Convert]::ToHexString($Bytes[0..7]) -eq 'D0CF11E0A1B11AE1') {
        # MS-CFB header lengths and chain heads reach the encrypted-container parser.
        foreach ($field in @(@('SectorShift', 30, 2), @('FatCount', 44, 4), @('DirectoryHead', 48, 4), @('MiniFatHead', 60, 4), @('MiniFatCount', 64, 4), @('DifatHead', 68, 4), @('DifatCount', 72, 4))) {
            Add-Field 'Encryption.Container' "CFB.$($field[0])" $field[1] $field[2]
        }
        return $fields.ToArray()
    }
    if ($Bytes.Length -lt 0x98 -or $Bytes[0x14] -gt 6) { return }
    $signature = [Text.Encoding]::ASCII.GetString($Bytes, 4, 15)
    if ($signature -notin "Standard Jet DB", "Standard ACE DB") { return }
    $jet3 = $Bytes[0x14] -eq 0
    $format = if ($jet3) { 'Jet3' } elseif ($Bytes[0x14] -eq 1) { 'Jet4' } else { 'ACE' }
    $size = if ($jet3) { 2048 } else { 4096 }
    $rows = if ($jet3) { 8 } else { 12 }
    $slots = $rows + 2
    Add-Field 'Header' 'Header.Version' 0x14 1
    Add-Field 'Header' 'Header.CodePage' 0x3C 2 $true
    Add-Field 'Encryption.Header' 'Encryption.EncodingKey' 0x3E 4 $true
    Add-Field 'Encryption.Header' 'Encryption.Password' 0x42 4 $true
    $header = [byte[]]$Bytes.Clone()
    Protect-FuzzHeader $header
    $encrypted = [BitConverter]::ToUInt32($header, 0x3E) -ne 0
    if ($format -eq 'ACE' -and $Bytes.Length -ge 0x2A7) {
        $descriptorLength = [BitConverter]::ToUInt16($Bytes, 0x299)
        if ($descriptorLength -ge 8 -and $descriptorLength -le ($size - 0x29B)) {
            Add-Field 'Encryption.Descriptor' 'Encryption.DescriptorLength' 0x299 2
            Add-Field 'Encryption.Descriptor' 'Encryption.DescriptorVersion' 0x29B 2
            if ([BitConverter]::ToUInt16($Bytes, 0x29D) -eq 2) {
                Add-Field 'Encryption.Descriptor' 'Encryption.ProviderHeaderSize' 0x2A3 4
            }
        }
    }
    # Encrypted page bytes have no trustworthy page-type or field metadata.
    if ($encrypted) { return $fields.ToArray() }
    $seen = @{}
    for ($page = 1; ($page + 1) * $size -le $Bytes.Length; $page++) {
        $start = $page * $size
        $type = [int]$Bytes[$start]
        if ($type -notin 1, 2, 3, 4, 5) { continue }
        if (-not $seen.ContainsKey($type)) { $seen[$type] = 0 }
        if ($seen[$type]++ -ge 4) { continue }
        switch ($type) {
            1 {
                Add-Field 'Data.Count' 'Data.NumRows' ($start + $rows) 2
                Add-Field 'Data.Count' 'Data.FreeSpace' ($start + 2) 2
                Add-Field 'Data.Pointer' 'Data.Owner' ($start + 4) 4
                if ([BitConverter]::ToUInt16($Bytes, $start + $rows) -gt 0) {
                    Add-Field 'Data.Pointer' 'Data.RowOffset' ($start + $slots) 2
                }
            }
            2 {
                Add-Field 'Schema.Pointer' 'Schema.NextPage' ($start + 4) 4
                Add-Field 'Schema.Count' 'Schema.Length' ($start + 8) 4
                $counts = if ($jet3) { @(12, 21, 23, 25, 27, 31) } else { @(16, 41, 43, 45, 47, 51) }
                for ($c = 0; $c -lt $counts.Count; $c++) {
                    $width = if ($c -in 1, 2, 3) { 2 } else { 4 }
                    Add-Field 'Schema.Count' (@('Schema.NumRows', 'Schema.MaxCols', 'Schema.VarCols', 'Schema.NumCols', 'Schema.NumIndexes', 'Schema.NumRealIndexes')[$c]) ($start + $counts[$c]) $width
                }
                $used = if ($jet3) { 35 } else { 55 }
                foreach ($pointer in @($used, ($used + 4))) {
                    Add-Field 'Schema.Pointer' 'Schema.UsageMap' ($start + $pointer) 4
                    $row = [int]$Bytes[$start + $pointer]
                    $mapPage = [int]$Bytes[$start + $pointer + 1] -bor ([int]$Bytes[$start + $pointer + 2] -shl 8) -bor ([int]$Bytes[$start + $pointer + 3] -shl 16)
                    $mapStart = $mapPage * $size
                    if ($mapPage -le 0 -or $mapStart + $size -gt $Bytes.Length -or $Bytes[$mapStart] -ne 1) { continue }
                    $rowCount = [BitConverter]::ToUInt16($Bytes, $mapStart + $rows)
                    if ($row -ge $rowCount -or $slots + $rowCount * 2 -gt $size) { continue }
                    $rowStart = [BitConverter]::ToUInt16($Bytes, $mapStart + $slots + $row * 2) -band 0x1FFF
                    if ($rowStart -lt $slots + $rowCount * 2 -or $rowStart + 5 -gt $size -or $Bytes[$mapStart + $rowStart] -gt 1) { continue }
                    Add-Field 'Usage.Pointer' 'Usage.RowPointer' ($mapStart + $rowStart + 1) 4
                }
            }
            { $_ -in 3, 4 } {
                Add-Field 'Index.Count' 'Index.FreeSpace' ($start + 2) 2
                Add-Field 'Index.Count' 'Index.PrefixLength' ($start + $(if ($jet3) { 20 } else { 24 })) $(if ($jet3) { 1 } else { 2 })
                foreach ($offset in $(if ($jet3) { @(8, 12, 16) } else { @(12, 16, 20) })) {
                    Add-Field 'Index.Pointer' 'Index.Chain' ($start + $offset) 4
                }
            }
        }
    }
    return $fields.ToArray()
}

function New-FuzzStructuralInput([byte[]] $Bytes, $Field, [int] $Variant) {
    $mutated = [byte[]]$Bytes.Clone()
    if ($Field.Masked) { Protect-FuzzHeader $mutated }
    $maximum = [long][Math]::Pow(256, $Field.Width) - 1
    $values = @(0L, 1L, $maximum, ([long]$Bytes.Length + 1))
    $value = $values[$Variant % $values.Count] -band $maximum
    $before = [Convert]::ToHexString($mutated[$Field.Offset..($Field.Offset + $Field.Width - 1)])
    $same = $true
    for ($index = 0; $index -lt $Field.Width; $index++) {
        $next = [byte](($value -shr (8 * $index)) -band 255)
        if ($next -ne $mutated[$Field.Offset + $index]) { $same = $false }
        $mutated[$Field.Offset + $index] = $next
    }
    # Every scheduled structural case must change its selected field.
    if ($same) { $mutated[$Field.Offset] = $mutated[$Field.Offset] -bxor 1 }
    $after = [Convert]::ToHexString($mutated[$Field.Offset..($Field.Offset + $Field.Width - 1)])
    if ($Field.Masked) { Protect-FuzzHeader $mutated }
    return [pscustomobject]@{
        Bytes = $mutated
        Mutation = [pscustomobject]@{ Family = $Field.Family; Field = $Field.Name; Format = $Field.Format; Offset = $Field.Offset; Width = $Field.Width; Masked = $Field.Masked; Before = $before; After = $after; Variant = $Variant % $values.Count }
    }
}

function Get-FuzzReaderPlan($Corpus, [int] $Seed) {
    $entries = [Collections.Generic.List[object]]::new()
    $catalog = [Collections.Generic.List[object]]::new()
    foreach ($fixture in $Corpus) {
        $bytes = [IO.File]::ReadAllBytes($fixture.FullName)
        $fields = @(Get-FuzzStructuralFields $bytes)
        $catalog.Add([pscustomobject]@{ File = $fixture; Fields = $fields; SHA256 = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)) })
    }
    foreach ($format in @('Jet3', 'Jet4', 'ACE')) {
        $healthy = @($catalog | Where-Object { @($_.Fields | Where-Object { $_.Format -eq $format -and $_.Family -eq 'Index.Pointer' }).Count -gt 0 })
        $preferredName = switch ($format) { 'Jet3' { 'indexTestV1997.mdb' } 'Jet4' { 'indexCursorTestV2000.mdb' } 'ACE' { 'indexCursorTestV2007.accdb' } }
        $preferred = @($healthy | Where-Object { $_.File.Name -eq $preferredName })
        if ($preferred.Count -gt 0) { $healthy = $preferred }
        if ($healthy.Count -gt 0) { $entries.Add([pscustomobject]@{ Source = $healthy[0]; Field = $null; Family = 'Control'; Format = $format }) }
    }
    foreach ($entry in $catalog | Where-Object { $_.File.Name -match '^(NativeJet[34](Password|Rc4)|NativeAce(Agile|Standard)|AesEncrypted|linkerTestV2007|odbcLinkerTestV2007)\.' }) {
        $entries.Add([pscustomobject]@{ Source = $entry; Field = $null; Family = 'Control'; Format = $entry.Fields[0].Format })
    }
    $groups = @{}
    foreach ($entry in $catalog) {
        foreach ($field in $entry.Fields) {
            $group = "$($field.Format)/$($field.Family)"
            if (-not $groups.ContainsKey($group)) { $groups[$group] = [Collections.Generic.List[object]]::new() }
            $groups[$group].Add([pscustomobject]@{ Source = $entry; Field = $field; Family = $field.Family; Format = $field.Format })
        }
    }
    $random = [Random]::new($Seed)
    foreach ($group in $groups.Keys | Sort-Object) { $entries.Add($groups[$group][$random.Next($groups[$group].Count)]) }
    return [pscustomobject]@{ Entries = $entries.ToArray(); Catalog = $catalog.ToArray(); AvailableGroups = @($groups.Keys | Sort-Object) }
}

function Invoke-FuzzMinimization {
    param([byte[]] $Bytes, [string] $Signature, [scriptblock] $Probe, [int] $MaxAttempts = 8, [double] $BudgetSeconds = 30, [double] $MinimumAttemptSeconds = 0)
    $best = [byte[]]$Bytes.Clone()
    $attempts = 0
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $limit = if ($Signature -in 'Timeout', 'MemoryLimit') { [Math]::Min(2, $MaxAttempts) } else { $MaxAttempts }
    $chunk = [Math]::Max(1, [int][Math]::Ceiling($best.Length / 2.0))
    while ($best.Length -gt 0 -and $attempts -lt $limit -and $timer.Elapsed.TotalSeconds -lt $BudgetSeconds) {
        $reduced = $false
        for ($start = 0; $start -lt $best.Length -and $attempts -lt $limit; $start += $chunk) {
            $remaining = $BudgetSeconds - $timer.Elapsed.TotalSeconds
            if ($remaining -le 0 -or $remaining -lt $MinimumAttemptSeconds) { break }
            $remove = [Math]::Min($chunk, $best.Length - $start)
            $candidate = [byte[]]::new($best.Length - $remove)
            [Array]::Copy($best, 0, $candidate, 0, $start)
            [Array]::Copy($best, $start + $remove, $candidate, $start, $best.Length - $start - $remove)
            $attempts++
            $outcome = & $Probe $candidate $remaining $attempts
            if ($outcome.Signature -eq $Signature) { $best = $candidate; $reduced = $true; break }
        }
        if (-not $reduced) {
            if ($chunk -eq 1) { break }
            $chunk = [Math]::Max(1, [int][Math]::Floor($chunk / 2.0))
        } else { $chunk = [Math]::Min($chunk, [Math]::Max(1, $best.Length)) }
        if ($BudgetSeconds - $timer.Elapsed.TotalSeconds -lt $MinimumAttemptSeconds) { break }
    }
    $timer.Stop()
    return [pscustomobject]@{ Bytes = $best; Signature = $Signature; Attempts = $attempts; Seconds = $timer.Elapsed.TotalSeconds; OriginalBytes = $Bytes.Length; MinimizedBytes = $best.Length }
}
