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
        'JetDatabaseWriter.Tests.RoundTrip.PublicTextCollationDaoTests.Dao120_ExplicitChoice_SurvivesMutationSeekAndCompact'
        'JetDatabaseWriter.Tests.RoundTrip.PublicTextCollationDaoTests.Dao120_NativeAuthoredColumns_CompactTextToHeaderDefaultWhileRetainingMemo'
        'JetDatabaseWriter.Tests.Schema.DropIndexDaoTests.NativeTable_IndexRemovalSurvivesReadWriteAndCompact'
        'JetDatabaseWriter.Tests.Schema.RenameIndexDaoTests.NativeNamesAndRelationships_SurviveRenameMutationAndCompact'
        'JetDatabaseWriter.Tests.Schema.RenameTableDaoTests.Dao120_RenamePreservesNativeDependenciesAndConstraints'
        'JetDatabaseWriter.Tests.Schema.LinkedTableRelationshipDaoTests.Dao120_WriterDropMatchesNativeRelationshipCleanupAndSurvivesCompact'
        'JetDatabaseWriter.Tests.Schema.TableExpressionFunctionPolicyTests.Dao120_TableDefaultsAndRulesRefuseApplicationFunctions'
        'JetDatabaseWriter.Tests.Schema.NativeAllowZeroLengthTests.Dao120Fields_ManagedWritesAndRewritesKeepEmptyStringPolicy'
        'JetDatabaseWriter.Tests.Schema.NativeAllowZeroLengthTests.Dao120ComplexItem_EmptyStringPreservesAbsentChildPolicy'
        'JetDatabaseWriter.Tests.Schema.NativeDisplayPropertyTests.Dao120ColumnEdit_PreservesDisplayTextThroughReopenAndCompact'
        'JetDatabaseWriter.Tests.Schema.NativeLookupPropertyTests.Dao120SchemaEdit_PreservesLookupAndSavedQueryText'
        'JetDatabaseWriter.Tests.Schema.NativeSqlSpecificQueryPreservationTests.Dao120_SqlSpecificQueriesPreserveNativeMetadataThroughEditAndCompact'
        'JetDatabaseWriter.Tests.Schema.NativeApplicationObjectPreservationTests.Dao120_PublicMutationPreservesInertDefinitionsThroughCompact'
        'JetDatabaseWriter.Tests.Schema.NativeApplicationObjectPreservationTests.Dao120_NativeCompactPreservesApplicationValuesByColumnName'
        'JetDatabaseWriter.Tests.Schema.NativeApplicationObjectPreservationTests.Dao120_PublicMutationPreservesWorkgroupOwnersAndGrantsThroughCompact'
        'JetDatabaseWriter.Tests.Schema.NativeVariableTextPreservationTests.NativeVariableText_Dao120AcceptsPreservedSlotsAndCompact'
        'JetDatabaseWriter.Tests.ValueEncoding.NativeFixedNullScalarTests.NativeScalarNullSlots_Dao120MatchesWriterAndCompacts'
        'JetDatabaseWriter.Tests.ValueEncoding.NativeDroppedNullSlotTests.DroppedFixedAndVariableSlots_Dao120AcceptsWriterNullRowsAndCompact'
        'JetDatabaseWriter.Tests.ValueEncoding.NativeMemoCompressionTests.WriterMemoBoundary_DaoReadsExactValuesBeforeAndAfterCompact'
        'JetDatabaseWriter.Tests.ValueEncoding.NativeMemoCompressionTests.WriterLiteralFeff_DaoReadsEveryValueAndCompactedCopy'
        'JetDatabaseWriter.Tests.ValueEncoding.NativeCompressedTextMarkerTests.WriterCompressedText_Dao120ReadsUpdatesAndCompactsExactValues'
        'JetDatabaseWriter.Tests.Writer.Jet4NativeBootstrapTests.FreshJet4_DaoOpensWritesAndCompacts'
        'JetDatabaseWriter.Tests.Pages.LongValueUsageMapTests.Dao120_NativeColumnMapsSurviveWriterDeleteReuseAndCompact'
        'JetDatabaseWriter.Tests.Pages.NativeDataPageFreeSpaceTests.Jet4AndAce_FreeMapDeletionAndReuse_MatchDao120'
        'JetDatabaseWriter.Tests.Pages.OwnedUsageMapGrowthTests.Dao_RecordCount_SeesRowsOnPagesPastInlineWindow'
        'JetDatabaseWriter.Tests.Indexes.FixedBinaryIndexTests.FixedBinary_DaoSeeksPersistedPaddedKeyAndRefusesDuplicate'
        'JetDatabaseWriter.Tests.Reader.FixedTextFixtureTests.FixedText_DaoPadsShortValuesAndCompactsWriterValues'
        'JetDatabaseWriter.Tests.ValueEncoding.FixedBinaryTests.WriterBinary_DaoReadsZeroPaddingAndCompacts'
        'JetDatabaseWriter.Tests.ValueDecoding.NativeDateSerialTests.WriterUpdate_NativeIndexedReadAndCompactRetainNegativeSerial'
        'JetDatabaseWriter.Tests.Encryption.NativeStandardDatabaseTests.StandardProvider_DaoReadsWritesAndCompacts'
        'JetDatabaseWriter.Tests.RoundTrip.DaoEncryptionMaintenanceTests.CreateEncrypted_DaoReadsWritesAndCompacts'
        'JetDatabaseWriter.Tests.RoundTrip.DaoNativeAceProviderTests.HistoricalProvider_DaoReadsWritesAndCompacts'
        'JetDatabaseWriter.Tests.RoundTrip.DaoNativeEncryptionTests.NativeEncryptedMutation_DaoReadsWritesAndCompacts'
        'JetDatabaseWriter.Tests.RoundTrip.DaoNativeJetSecurityTests.SecurityTransformation_DaoAuthenticatesWritesAndCompacts'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipCatalogIdentityTests.DaoRelationshipDrop_RecreateRollbackAndCompact'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipCatalogIdentityTests.Dao120_RenameAcrossNativeSortBoundaries_IsImmediatelyVisible'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipDropTests.DaoTableDefsDelete_OfRelatedNorthwindTable_IsRefused'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipDropTests.DaoTableDefsDelete_OfTableInRelationshipWithoutIntegrity_IsDeleted'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRelationshipDropTests.DaoTableDefsDelete_OfSelfReferencingTable_IsDeleted'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRenameColumnCaseTests.DaoCaseOnlyFieldRename_MatchesWriterBehaviour'
        'JetDatabaseWriter.Tests.RoundTrip.DaoRenameColumnCaseTests.WriterCaseOnlyRename_OpensReadsAndCompactsInDao'
        'JetDatabaseWriter.Tests.ComplexColumns.ComplexItemMutationDaoTests.NativeItems_ReplaceRemoveExtractAndCompactPreserveIdentities'
        'JetDatabaseWriter.Tests.ComplexColumns.ComplexSchemaDaoTests.NativeComplexTable_CompleteRewritePreservesValuesIndexesCountersAndAppendOnly'
        'JetDatabaseWriter.Tests.ComplexColumns.AppendOnlyHistorySchemaTests.SharedMemoRenameAndDrop_NativeReadAndCompactPreserveSurvivingHistory'
        'JetDatabaseWriter.Tests.Schema.NativeSchemaDaoTests.NativeTable4_RewriteSurvivesDaoOpenAndCompact'
        'JetDatabaseWriter.Tests.Schema.NativeSchemaDaoTests.DaoAuthoredWideAttachment_RewriteSurvivesNativeMutationAndCompact'
        'JetDatabaseWriter.Tests.Schema.IndexDescriptorPreservationTests.Dao120_NativeDescriptorsSurviveRewriteMutationAndCompact'
        'JetDatabaseWriter.Tests.Schema.IndexDescriptorPreservationTests.Dao120_NewPrimaryKeyPreservesForeignKeyNameAndProperties'
        'JetDatabaseWriter.Tests.Maintenance.CompactReclaimingDatabaseTests.CompactedDatabase_Dao120ReadsMutatesAndRecompacts'
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
