namespace JetDatabaseWriter.Benchmarks.Models;

/// <summary>
/// DTO bound to the Access-authored <c>ProductCategories</c> table in
/// NorthwindTraders.accdb, whose <c>ProductCategoryImage</c> Attachment column
/// holds 16 images.
/// </summary>
internal sealed class ProductCategory
{
    public int ProductCategoryId { get; set; }

    public byte[]? ProductCategoryImage { get; set; }
}
