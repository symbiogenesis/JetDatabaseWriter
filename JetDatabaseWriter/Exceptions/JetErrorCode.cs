namespace JetDatabaseWriter.Exceptions;

/// <summary>Stable identifiers for database failures.</summary>
public enum JetErrorCode
{
    /// <summary>Identifies None.</summary>
    None = 0,

    /// <summary>Identifies TableNotFound.</summary>
    TableNotFound = 101,

    /// <summary>Identifies ColumnNotFound.</summary>
    ColumnNotFound = 102,

    /// <summary>Identifies IndexNotFound.</summary>
    IndexNotFound = 103,

    /// <summary>Identifies RelationshipNotFound.</summary>
    RelationshipNotFound = 104,

    /// <summary>Identifies RowNotFound.</summary>
    RowNotFound = 105,

    /// <summary>Identifies RowNotUnique.</summary>
    RowNotUnique = 106,

    /// <summary>Identifies CatalogObjectNotFound.</summary>
    CatalogObjectNotFound = 107,

    /// <summary>Identifies SystemTableMissing.</summary>
    SystemTableMissing = 108,

    /// <summary>Identifies ComplexItemNotFound.</summary>
    ComplexItemNotFound = 109,

    /// <summary>Identifies RelationshipTargetNotFound.</summary>
    RelationshipTargetNotFound = 110,

    /// <summary>Identifies TableExists.</summary>
    TableExists = 201,

    /// <summary>Identifies ColumnExists.</summary>
    ColumnExists = 202,

    /// <summary>Identifies RelationshipExists.</summary>
    RelationshipExists = 203,

    /// <summary>Identifies ObjectExists.</summary>
    ObjectExists = 204,

    /// <summary>Identifies UniqueViolation.</summary>
    UniqueViolation = 301,

    /// <summary>Identifies ForeignKeyMissingParent.</summary>
    ForeignKeyMissingParent = 302,

    /// <summary>Identifies ForeignKeyRestrictDelete.</summary>
    ForeignKeyRestrictDelete = 303,

    /// <summary>Identifies ForeignKeyRestrictUpdate.</summary>
    ForeignKeyRestrictUpdate = 304,

    /// <summary>Identifies CascadeDepthExceeded.</summary>
    CascadeDepthExceeded = 305,

    /// <summary>Identifies NotNullViolation.</summary>
    NotNullViolation = 306,

    /// <summary>Identifies ValidationRuleViolation.</summary>
    ValidationRuleViolation = 307,

    /// <summary>Identifies TableValidationRuleViolation.</summary>
    TableValidationRuleViolation = 308,

    /// <summary>Identifies LastColumn.</summary>
    LastColumn = 401,

    /// <summary>Identifies KeyColumnInRelationship.</summary>
    KeyColumnInRelationship = 402,

    /// <summary>Identifies InvalidExpression.</summary>
    InvalidExpression = 403,

    /// <summary>Identifies UnsupportedExpression.</summary>
    UnsupportedExpression = 404,

    /// <summary>Identifies ColumnReferencedByExpression.</summary>
    ColumnReferencedByExpression = 405,

    /// <summary>Identifies TableInRelationship.</summary>
    TableInRelationship = 406,

    /// <summary>Identifies InvalidObjectName.</summary>
    InvalidObjectName = 407,

    /// <summary>Identifies DefaultNotAllowed.</summary>
    DefaultNotAllowed = 408,

    /// <summary>Identifies DatabaseInUse.</summary>
    DatabaseInUse = 501,

    /// <summary>Identifies LockFileFull.</summary>
    LockFileFull = 502,

    /// <summary>Identifies LockTimeout.</summary>
    LockTimeout = 503,

    /// <summary>Identifies TransactionAlreadyActive.</summary>
    TransactionAlreadyActive = 504,

    /// <summary>Identifies TransactionEnded.</summary>
    TransactionEnded = 505,

    /// <summary>Identifies TransactionNotActive.</summary>
    TransactionNotActive = 506,

    /// <summary>Identifies DatabaseFileExists.</summary>
    DatabaseFileExists = 507,

    /// <summary>Identifies HotJournalConflict.</summary>
    HotJournalConflict = 508,

    /// <summary>Identifies WriterFaulted.</summary>
    WriterFaulted = 509,

    /// <summary>Identifies ReentrantWriterCall.</summary>
    ReentrantWriterCall = 510,

    /// <summary>Identifies PasswordRequired.</summary>
    PasswordRequired = 601,

    /// <summary>Identifies PasswordIncorrect.</summary>
    PasswordIncorrect = 602,

    /// <summary>Identifies LinkedSourceNotPermitted.</summary>
    LinkedSourceNotPermitted = 603,

    /// <summary>Identifies NotEncrypted.</summary>
    NotEncrypted = 604,

    /// <summary>Identifies AlreadyEncrypted.</summary>
    AlreadyEncrypted = 605,

    /// <summary>Identifies CorruptCatalog.</summary>
    CorruptCatalog = 701,

    /// <summary>Identifies CorruptTableDefinition.</summary>
    CorruptTableDefinition = 702,

    /// <summary>Identifies UnreadableLongValue.</summary>
    UnreadableLongValue = 703,

    /// <summary>Identifies MalformedValue.</summary>
    MalformedValue = 704,

    /// <summary>Identifies CorruptComplexColumn.</summary>
    CorruptComplexColumn = 705,

    /// <summary>Identifies CorruptIndex.</summary>
    CorruptIndex = 706,

    /// <summary>Identifies CorruptEncryptedPackage.</summary>
    CorruptEncryptedPackage = 707,

    /// <summary>Identifies UnknownColumnType.</summary>
    UnknownColumnType = 708,

    /// <summary>Identifies ValueTooLarge.</summary>
    ValueTooLarge = 801,

    /// <summary>Identifies RowTooLarge.</summary>
    RowTooLarge = 802,

    /// <summary>Identifies JournalBudgetExceeded.</summary>
    JournalBudgetExceeded = 803,

    /// <summary>Identifies IndexesUnmaintainable.</summary>
    IndexesUnmaintainable = 804,

    /// <summary>Identifies NumericOverflow.</summary>
    NumericOverflow = 805,

    /// <summary>Identifies IndexEntryTooLarge.</summary>
    IndexEntryTooLarge = 806,

    /// <summary>Identifies PasswordTooLong.</summary>
    PasswordTooLong = 807,

    /// <summary>Identifies UnsupportedTextCollation.</summary>
    UnsupportedTextCollation = 808,

    /// <summary>Identifies ColumnCountLimit.</summary>
    ColumnCountLimit = 809,

    /// <summary>Identifies FeatureNotSupported.</summary>
    FeatureNotSupported = 901,

    /// <summary>Identifies LinkedTableHasNoIndexes.</summary>
    LinkedTableHasNoIndexes = 903,

    /// <summary>Identifies OdbcRowsNotAvailable.</summary>
    OdbcRowsNotAvailable = 904,

    /// <summary>Identifies VersionHistoryNotEditable.</summary>
    VersionHistoryNotEditable = 905,

    /// <summary>Identifies ComplexColumnsNotSupported.</summary>
    ComplexColumnsNotSupported = 906,

    /// <summary>Identifies CatalogIndexMaintenanceFailed.</summary>
    CatalogIndexMaintenanceFailed = 1001,

    /// <summary>Identifies SystemIndexMaintenanceFailed.</summary>
    SystemIndexMaintenanceFailed = 1002,
}
