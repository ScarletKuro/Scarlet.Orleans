using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Options for EF Core grain storage.
/// </summary>
public sealed class ScarletEntityFrameworkCoreGrainStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// What to do with the schema at startup. Defaults to <see cref="SchemaMode.Validate"/>, which is also the mode to
    /// use when the application's own EF Core migrations create the Orleans tables.
    /// </summary>
    public SchemaMode SchemaMode { get; set; } = SchemaMode.Validate;

    /// <summary>
    /// Whether clearing state deletes the row, rather than keeping it with no payload and a new ETag.
    /// </summary>
    public bool DeleteStateOnClear { get; set; }

    /// <inheritdoc/>
    public IGrainStorageSerializer GrainStorageSerializer { get; set; } = null!;

    /// <summary>
    /// The silo lifecycle stage at which the schema is checked.
    /// </summary>
    public int InitStage { get; set; } = ServiceLifecycleStage.ApplicationServices;
}
