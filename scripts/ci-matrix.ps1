# Analyzer execution and validated-ancestor inventories share this expansion.
function Get-CiAnalyzerMatrix {
    $runtimes = @(
        @{ name = 'net10.0'; tf = 'net10.0'; restore = '' }
        @{ name = 'net10.0 netstandard'; tf = 'net10.0-netstandard'; restore = '-p:TestNetStandard=true' }
        @{ name = 'net11.0'; tf = 'net11.0'; restore = '' }
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
        [ordered]@{ name = 'library'; project = 'src/JetDatabaseWriter'; pack = $true }
        [ordered]@{ name = 'tools'; project = '.github/ci-tools.slnf'; options = '"-p:AnalyzeExcept=JetDatabaseWriter JetDatabaseWriter.Tests"' }
        foreach ($runtime in $runtimes) {
            foreach ($partition in $partitions.GetEnumerator()) {
                [ordered]@{
                    name = 'tests ' + $runtime.name + ' ' + $partition.Key
                    project = 'tests/JetDatabaseWriter.Tests'
                    options = ('-f net10.0 -p:AnalyzeOnly=JetDatabaseWriter.Tests ' + $runtime.restore).TrimEnd()
                    'restore-options' = $runtime.restore
                    tf = $runtime.tf
                    runner = $true
                    partition = $partition.Key
                    'test-options' = $partition.Value
                }
            }
        }
    ) }
}
