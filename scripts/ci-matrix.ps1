# Compilation and execution inventories are shared by the workflow and its ancestor gate.
function Get-CiAnalyzerMatrix {
    return [ordered]@{ include = @(
        [ordered]@{ name = 'library'; project = 'src/JetDatabaseWriter'; pack = $true }
        [ordered]@{ name = 'tools'; project = '.github/ci-tools.slnf'; options = '"-p:AnalyzeExcept=JetDatabaseWriter JetDatabaseWriter.Tests"' }
        [ordered]@{ name = 'tests native'; project = 'tests/JetDatabaseWriter.Tests'; options = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests'; runner = $true; asset = 'native' }
        [ordered]@{ name = 'tests standard'; project = 'tests/JetDatabaseWriter.Tests'; options = '-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests -p:TestNetStandard=true'; 'restore-options' = '-p:TestNetStandard=true'; runner = $true; asset = 'standard' }
    ) }
}

function Get-CiTestMatrix {
    $runtimes = @(
        @{ name = 'net10.0'; tf = 'net10.0'; asset = 'native' }
        @{ name = 'net10.0 netstandard'; tf = 'net10.0-netstandard'; asset = 'standard' }
        @{ name = 'net11.0'; tf = 'net11.0'; asset = 'native' }
    )
    $namespaces = 'JetDatabaseWriter.Tests.Schema JetDatabaseWriter.Tests.RoundTrip JetDatabaseWriter.Tests.Writer'
    $sweeps = [ordered]@{
        jet3 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet3Tests'
        jet4 = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityJet4Tests'
        ace = 'JetDatabaseWriter.Tests.Writer.StatementAtomicityAceTests'
    }
    $partitions = [ordered]@{
        ordinary = '--filter-not-namespace ' + $namespaces
        'schema-writer' = '--filter-namespace ' + $namespaces + ' --filter-not-class ' + ($sweeps.Values -join ' ')
    }
    foreach ($sweep in $sweeps.GetEnumerator()) { $partitions[$sweep.Key] = '--filter-class ' + $sweep.Value }
    return [ordered]@{ include = @(
        foreach ($runtime in $runtimes) {
            foreach ($partition in $partitions.GetEnumerator()) {
                [ordered]@{
                    name = 'tests ' + $runtime.name + ' ' + $partition.Key
                    tf = $runtime.tf
                    asset = $runtime.asset
                    partition = $partition.Key
                    'test-options' = $partition.Value
                }
            }
        }
    ) }
}
