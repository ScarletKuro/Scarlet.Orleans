using Scarlet.Orleans.Relational;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Persistence;

internal static class PersistenceSchema
{
    /// <summary>The ledger key of this component.</summary>
    public const string Component = "persistence";

    public const string TableName = "orleans_grain_state";

    /// <summary>
    /// The oldest schema version this code runs against. Raised only by a release that needs a newer schema.
    /// </summary>
    public const int MinimumVersion = 1;

    public static SchemaMigrator CreateMigrator(RelationalEngine engine) =>
        new(Component, engine, SchemaScript.Load(typeof(PersistenceSchema).Assembly, engine), MinimumVersion);
}
