namespace JetDatabaseWriter.Catalog;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetDatabaseWriter.Catalog.Models;
using JetDatabaseWriter.Encryption;
using JetDatabaseWriter.Indexes;
using JetDatabaseWriter.Models;
using JetDatabaseWriter.Pages.Paging;
using JetDatabaseWriter.Schema;
using JetDatabaseWriter.Schema.Models;
using static JetDatabaseWriter.Schema.JetTypeInfo;

/// <summary>
/// Creates core catalog tables, permissions and header pointers in a fresh database.
/// The caller owns the creation transaction; artifact execution owns table allocation,
/// while this service orders catalog creation before linking the header and permissions.
/// Complex-value storage and its ACE-only template tables have a separate owner.
/// </summary>
/// <param name="format">The database format.</param>
/// <param name="tableDefs">Reads the created permission table.</param>
/// <param name="pager">Reads and patches the database header in the creation transaction.</param>
/// <param name="indexes">Inserts permission rows with their indexes.</param>
/// <param name="catalogArtifacts">Creates the core tables and catalog objects.</param>
internal sealed class SystemCatalogBootstrapper(
    JetFormat format,
    TableDefReader tableDefs,
    Pager pager,
    IndexMaintainer indexes,
    CatalogArtifactWriter catalogArtifacts)
{
    private readonly JetFormat format = format;
    private readonly TableDefReader tableDefs = tableDefs;
    private readonly Pager pager = pager;

    /// <summary>Creates the core system tables supported by the database format.</summary>
    /// <param name="coreSystemTableStartPage">The reserved first core TDEF page, or zero to allocate.</param>
    /// <param name="cancellationToken">Cancels creation within the caller's transaction.</param>
    internal async ValueTask CreateCoreSystemTablesAsync(long coreSystemTableStartPage, CancellationToken cancellationToken)
    {
        if (!this.format.SupportsCoreCatalogTables)
        {
            return;
        }

        const uint systemTableFlags = Constants.SystemObjects.SystemTableMask & Constants.SystemObjects.SystemObjectFlag;
        var plan = new CatalogArtifactPlan(
            [
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Aces,
                    [
                        new ColumnDefinition("ObjectId", typeof(int)),
                        new ColumnDefinition("SID", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x32 },
                        new ColumnDefinition("ACM", typeof(int)),
                        new ColumnDefinition("FInheritable", typeof(bool)),
                    ],
                    [new IndexDefinition("ObjectId", "ObjectId") { IsRequired = true }],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage,
                    EmitLvProp: false),
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Queries,
                    [
                        new ColumnDefinition("ObjectId", typeof(int)),
                        new ColumnDefinition("Attribute", typeof(byte)),
                        new ColumnDefinition("Order", typeof(byte[]), maxLength: 255) { DescriptorFlagsOverride = 0x12 },
                        new ColumnDefinition("Name1", typeof(string), maxLength: 255),
                        new ColumnDefinition("Name2", typeof(string), maxLength: 255),
                        new ColumnDefinition("Expression", typeof(string)),
                        new ColumnDefinition("Flag", typeof(short)),
                        new ColumnDefinition("LvExtra", typeof(int)),
                    ],
                    [new IndexDefinition("ObjectIdAttribute", ["ObjectId", "Attribute", "Order"]) { IsPrimaryKey = true }],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage > 0 ? coreSystemTableStartPage + 1 : 0,
                    EmitLvProp: false),
                new CatalogTableArtifact(
                    Constants.SystemTableNames.Relationships,
                    [
                        new ColumnDefinition("szRelationship", typeof(string), maxLength: 255),
                        new ColumnDefinition("grbit", typeof(int)),
                        new ColumnDefinition("ccolumn", typeof(int)),
                        new ColumnDefinition("icolumn", typeof(int)),
                        new ColumnDefinition("szObject", typeof(string), maxLength: 255),
                        new ColumnDefinition("szColumn", typeof(string), maxLength: 255),
                        new ColumnDefinition("szReferencedObject", typeof(string), maxLength: 255),
                        new ColumnDefinition("szReferencedColumn", typeof(string), maxLength: 255),
                    ],
                    [
                        new IndexDefinition("szRelationship", "szRelationship") { IgnoreNulls = true },
                        new IndexDefinition("szObject", "szObject") { IgnoreNulls = true },
                        new IndexDefinition("szReferencedObject", "szReferencedObject") { IgnoreNulls = true },
                    ],
                    systemTableFlags,
                    ReservedTdefPageNumber: coreSystemTableStartPage > 0 ? coreSystemTableStartPage + 2 : 0,
                    EmitLvProp: false),
            ],
            []);

        CatalogObjectArtifact[] objects = BuildCoreCatalogObjectArtifacts();
        byte[]? header = null;
        if (this.format.UsesHeaderMaskedSecuritySids)
        {
            byte[] borrowed = await this.pager.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
            try
            {
                header = borrowed.AsSpan(0, this.format.PageSize).ToArray();
            }
            finally
            {
                PageBuffers.Return(borrowed);
            }

            objects = objects.Select(artifact => artifact with
            {
                Owner = Jet4SecuritySid.Encode(this.format, header, artifact.ObjectName == "MSysDb" ? [0x03, 0x01] : [0x02, 0x03]),
            }).ToArray();
        }

        if (header is not null)
        {
            _ = await catalogArtifacts.ExecutePlanAsync(new CatalogArtifactPlan([], objects), cancellationToken).ConfigureAwait(false);
            byte[] systemOwner = Jet4SecuritySid.Encode(this.format, header, [0x02, 0x03]);
            plan = plan with { TableArtifacts = plan.TableArtifacts.Select(table => table with { Owner = systemOwner }).ToArray() };
        }
        else
        {
            plan = plan with { CatalogObjects = objects };
        }

        long[] tablePages = await catalogArtifacts.ExecutePlanAsync(plan, cancellationToken).ConfigureAwait(false);
        long acesTdefPage = tablePages[0];
        long queriesTdefPage = tablePages[1];
        long relationshipsTdefPage = tablePages[2];
        await this.PatchHeaderSystemTablePagesAsync(acesTdefPage, queriesTdefPage, relationshipsTdefPage, cancellationToken).ConfigureAwait(false);
        if (header is not null)
        {
            await this.InitializeJet4PermissionsAsync(header, acesTdefPage, queriesTdefPage, relationshipsTdefPage, cancellationToken).ConfigureAwait(false);
        }
    }

    private static CatalogObjectArtifact[] BuildCoreCatalogObjectArtifacts()
        =>
        [
            new CatalogObjectArtifact(
                2,
                Constants.SystemObjects.TablesParentId,
                Constants.SystemTableNames.Objects,
                Constants.SystemObjects.UserTableType,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.TablesParentId,
                Constants.SystemObjects.RootParentId,
                "Tables",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.DatabasesParentId,
                Constants.SystemObjects.RootParentId,
                "Databases",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.RelationshipsParentId,
                Constants.SystemObjects.RootParentId,
                "Relationships",
                3,
                Constants.SystemObjects.SystemObjectFlag),
            new CatalogObjectArtifact(
                Constants.SystemObjects.DatabaseObjectId,
                Constants.SystemObjects.DatabasesParentId,
                "MSysDb",
                2,
                Constants.SystemObjects.SystemObjectFlag),
        ];

    private async ValueTask InitializeJet4PermissionsAsync(
        byte[] header,
        long acesPage,
        long queriesPage,
        long relationshipsPage,
        CancellationToken cancellationToken)
    {
        TableDef definition = await this.tableDefs.ReadRequiredTableDefAsync(acesPage, Constants.SystemTableNames.Aces, cancellationToken).ConfigureAwait(false);
        byte[] owner = Jet4SecuritySid.Encode(this.format, header, [0x03, 0x01]);
        byte[] users = Jet4SecuritySid.Encode(this.format, header, [0x02, 0x01]);
        byte[] inheritedOwner = Jet4SecuritySid.GetOwnerPlaceholder(this.format, header);
        (int ObjectId, byte[] Sid, int Acm, bool Inheritable)[] permissions =
        [
            (2, owner, 0x60000, false),
            (checked((int)acesPage), owner, 0x60000, false),
            (checked((int)queriesPage), owner, 0x60000, false),
            (checked((int)relationshipsPage), owner, 0xE0000, false),
            (Constants.SystemObjects.TablesParentId, inheritedOwner, 0xF00FE, true),
            (Constants.SystemObjects.TablesParentId, owner, 0x60001, false),
            (Constants.SystemObjects.RelationshipsParentId, inheritedOwner, 0xF00FE, true),
            (Constants.SystemObjects.RelationshipsParentId, owner, 0x60001, false),
            (Constants.SystemObjects.DatabasesParentId, owner, 0x60000, false),
            (Constants.SystemObjects.DatabaseObjectId, owner, 0x6000E, false),
            (Constants.SystemObjects.DatabaseObjectId, users, 0xE, false),
            (checked((int)queriesPage), users, 0x14, false),
            (checked((int)relationshipsPage), users, 0x14, false),
            (2, users, 0x14, false),
            (Constants.SystemObjects.TablesParentId, users, 0xFFEFF, true),
            (Constants.SystemObjects.RelationshipsParentId, users, 0xFFFFF, true),
        ];
        foreach ((int objectId, byte[] sid, int acm, bool inheritable) in permissions)
        {
            object[] values = definition.CreateNullValueRow();
            definition.SetValueByName(values, "ObjectId", objectId);
            definition.SetValueByName(values, "SID", sid);
            definition.SetValueByName(values, "ACM", acm);
            definition.SetValueByName(values, "FInheritable", inheritable);
            await indexes.InsertSystemRowAndMaintainAsync(acesPage, definition, Constants.SystemTableNames.Aces, values, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask PatchHeaderSystemTablePagesAsync(
        long acesTdefPage,
        long queriesTdefPage,
        long relationshipsTdefPage,
        CancellationToken cancellationToken)
    {
        byte[] header = await this.pager.ReadPageAsync(0, cancellationToken).ConfigureAwait(false);
        try
        {
            EncryptionManager.TransformHeaderMask(header);
            Wi32(header, 0x20, 2);
            Wi32(header, 0x24, checked((int)acesTdefPage));
            Wi32(header, 0x28, checked((int)queriesTdefPage));
            Wi32(header, 0x2C, checked((int)relationshipsTdefPage));
            EncryptionManager.TransformHeaderMask(header);
            await this.pager.WritePageAsync(0, header, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            PageBuffers.Return(header);
        }
    }

}
