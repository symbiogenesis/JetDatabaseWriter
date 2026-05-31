namespace JetDatabaseWriter.Catalog.Models;

internal sealed record CatalogObjectArtifact(
    int ObjectId,
    int ParentId,
    string ObjectName,
    short ObjectType,
    uint CatalogFlags,
    byte[]? Owner = null,
    byte[]? LvProp = null)
{
    public CatalogObjectIdPolicy ObjectIdPolicy { get; init; } = CatalogObjectIdPolicy.Fixed;

    public string? Database { get; init; }

    public bool EncodeDatabaseAsMemoLval { get; init; }

    public string? Connect { get; init; }

    public string? ForeignName { get; init; }

    public bool EncodeForeignNameForTextLink { get; init; }

    public CatalogObjectAcePolicy AcePolicy { get; init; } = CatalogObjectAcePolicy.None;

    public bool RollbackCatalogRowOnIndexFailure { get; init; }

    public static CatalogObjectArtifact Relationship(string relationshipName, CatalogObjectAcePolicy acePolicy = CatalogObjectAcePolicy.RelationshipObject)
        => new(
            0,
            Constants.SystemObjects.RelationshipsParentId,
            relationshipName,
            Constants.SystemObjects.RelationshipType,
            0,
            Owner: Constants.SystemObjects.DefaultOwnerBlob)
        {
            ObjectIdPolicy = CatalogObjectIdPolicy.AllocateNonTable,
            AcePolicy = acePolicy,
        };

    public static CatalogObjectArtifact LinkedTable(
        string linkedTableName,
        string? sourceDatabasePath,
        string foreignName,
        string? connectString,
        short objectType,
        byte[]? cachedSchemaLvProp = null)
    {
        bool isTextLinkedTable = objectType == Constants.SystemObjects.LinkedTableType && !string.IsNullOrEmpty(connectString);
        byte[]? lvProp = cachedSchemaLvProp;
        if (lvProp is null && objectType == Constants.SystemObjects.LinkedOdbcType)
        {
            lvProp = Constants.SystemObjects.DefaultLvPropPlaceholder;
        }

        return new(
            0,
            Constants.SystemObjects.TablesParentId,
            linkedTableName,
            objectType,
            GetLinkedTableFlags(objectType, connectString),
            Owner: Constants.SystemObjects.DefaultOwnerBlob,
            LvProp: lvProp)
        {
            ObjectIdPolicy = CatalogObjectIdPolicy.AllocateNonTable,
            Database = sourceDatabasePath,
            EncodeDatabaseAsMemoLval = objectType == Constants.SystemObjects.LinkedTableType,
            Connect = connectString,
            ForeignName = foreignName,
            EncodeForeignNameForTextLink = isTextLinkedTable,
            AcePolicy = CatalogObjectAcePolicy.LinkedObject,
            RollbackCatalogRowOnIndexFailure = true,
        };
    }

    private static uint GetLinkedTableFlags(short objectType, string? connectString)
        => objectType switch
        {
            Constants.SystemObjects.LinkedOdbcType => Constants.SystemObjects.LinkedOdbcFlags,
            _ when objectType == Constants.SystemObjects.LinkedTableType && !string.IsNullOrEmpty(connectString) => Constants.SystemObjects.LinkedTextTableFlags,
            _ => Constants.SystemObjects.LinkedTableFlags,
        };
}
