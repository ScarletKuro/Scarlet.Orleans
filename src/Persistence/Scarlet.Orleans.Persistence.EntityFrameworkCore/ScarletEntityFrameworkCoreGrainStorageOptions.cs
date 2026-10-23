using Orleans;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.EntityFrameworkCore;

/// <summary>
/// Options for EF Core grain storage.
/// </summary>
/// <remarks>
/// Retries of transient database errors come from the execution strategy configured on the context, for example
/// <c>EnableRetryOnFailure</c>. Inserts and updates are safe to repeat; deletes run exactly once.
/// </remarks>
public sealed class ScarletEntityFrameworkCoreGrainStorageOptions : IStorageProviderSerializerOptions
{
    /// <summary>
    /// What to check about the schema at startup. Defaults to <see cref="SchemaMode.Validate"/>.
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
