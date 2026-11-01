# Execute a reviewed DAO120 inventory against the matching analyzed runner artifact.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string] $RunnerDirectory,
    [Parameter(Mandatory)][string] $ResultsDirectory
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot/install-native-access.ps1"

function Get-NativeCompatibilityMethods {
    return @(
        'JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.SinglePk_AndSingleColumnFk_SurviveCompactAndRepair'
        'JetDatabaseWriter.Tests.RoundTrip.AccessRoundTripTests.CompositePk_AndMultiColumnFk_SurviveCompactAndRepair'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoOpenRecordset_RowCount_MatchesWriterOutput'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoIndexTraversal_Seek_LocatesRowByPrimaryKey'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoAutoNumber_Continuation_NextIdFollowsLastWriterInsert'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoAuthoredMemo_WithEmbeddedNuls_ReaderReturnsExactContent'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoMemoAndOleLvalFidelity_EmbeddedNulsCjkAndBinary_RoundTripExactly'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoRelationshipEnforcement_FkViolation_RaisesError'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoCompactDatabase_OnEncryptedOutput_ReopenSucceeds'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoCompact_MultiTableStress_SurvivesCompactAndRepair'
        'JetDatabaseWriter.Tests.RoundTrip.DaoValidationTests.DaoCompact_ComplexColumnsWithLvalPayload_SurviveCompactAndRepair'
        'JetDatabaseWriter.Tests.RoundTrip.DaoCreateIndexTests.Dao120_AddedIndex_RetainsNativeSeekMutationAndCompact'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipMetadataTests.Dao120_CreateAndRename_RetainNativeMetadata'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipMetadataTests.Dao120_EarlierUniqueParentIndex_RetainsNativeChoice'
        'JetDatabaseWriter.Tests.RoundTrip.DaoNestedTransactionTests.DaoAndWriter_NestedScopes_HaveMatchingRowsAndDepth'
        'JetDatabaseWriter.Tests.RoundTrip.DaoPropertyMutationTests.EditedProperties_WithPreservedLayoutVariants_OpenEnforceAndCompactInDao'
        'JetDatabaseWriter.Tests.RoundTrip.DaoStorageMaintenanceTests.FreshWriterCreatedDatabase_DaoCompactSmokeTest'
        'JetDatabaseWriter.Tests.ComplexColumns.AttachmentWrapperDaoTests.NativeAttachmentWrappers_ExtensionlessAndEmptyFilesSurviveWriterAndCompactExtraction'
        'JetDatabaseWriter.Tests.ComplexColumns.AppendOnlyHistoryDaoTests.NativeAssignmentControls_LibraryReadsExactVersions'
        'JetDatabaseWriter.Tests.ComplexColumns.AppendOnlyHistoryDaoTests.LibraryMemoWrites_NativeReadUpdateAndCompactPreserveHistory'
        'JetDatabaseWriter.Tests.ComplexColumns.AppendOnlyHistoryDaoTests.SharedHistory_LibraryAndNativeAssignmentsKeepEachMemoVersion'
        'JetDatabaseWriter.Tests.ComplexColumns.AppendOnlyHistoryDaoTests.SharedHistoryUniqueIndex_ValidatesTheCombinedStatementBeforeMutation'
        'JetDatabaseWriter.Tests.Writer.InPlaceRowUpdateDaoTests.Dao120_NonKeyEdits_RetainNativeRowsAndCompact'
        'JetDatabaseWriter.Tests.Indexes.DuplicateBookmarkOrderingTests.Dao120_DuplicateTreesPermitNativeInsertUpdateDeleteAndCompact'
        'JetDatabaseWriter.Tests.Indexes.ForeignKeyIncrementalEvidenceTests.Dao120_SingleLeaf_InsertUsesIncrementalPathAndBoundedReads'
        'JetDatabaseWriter.Tests.Indexes.ForeignKeyIncrementalEvidenceTests.Dao120_MultiLevel_GrowthKeyChangesDeleteAndCompactPreserveMembership'
        'JetDatabaseWriter.Tests.RoundTrip.VariableValueWidthDaoTests.Dao120_WidthRefusal_RetainsNativeIndexAndCompact'
    )
}

function Assert-NativeResults([string] $ResultsDirectory, [string[]] $Methods) {
    & "$PSScriptRoot/assert-test-results.ps1" -ResultsDirectory $ResultsDirectory -RequireNoSkips -RequiredMethods $Methods
}

function Invoke-NativeCompatibility([string] $RunnerDirectory, [string] $ResultsDirectory) {
    Assert-NativeAccessAvailable
    $runner = [IO.Path]::GetFullPath($RunnerDirectory)
    if (-not [IO.File]::Exists((Join-Path $runner 'JetDatabaseWriter.Tests.dll'))) { throw "Analyzed native test runner is missing in '$runner'." }
    $methods = @(Get-NativeCompatibilityMethods)
    $arguments = @('test', '--test-modules', 'JetDatabaseWriter.Tests.dll', '--root-directory', $runner,
        '--minimum-expected-tests', [string]$methods.Count, '--filter-method') + $methods + @(
        '--long-running', '60', '--xunit-diagnostics', 'on', '--no-ansi', '--output', 'Detailed', '--timeout', '50m',
        '--report-xunit-xml', '--report-xunit-xml-filename', 'tests.xml', '--results-directory', $ResultsDirectory)
    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) { throw "Native compatibility tests failed with exit code $LASTEXITCODE." }
    Assert-NativeResults $ResultsDirectory $methods
}

if ($MyInvocation.InvocationName -ne '.') { Invoke-NativeCompatibility $RunnerDirectory $ResultsDirectory }
