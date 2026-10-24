using Microsoft.EntityFrameworkCore;
using Scarlet.Orleans.Relational.Schema;

namespace Scarlet.Orleans.Relational.EntityFrameworkCore;

/// <summary>
/// Checks the versions of one component recorded in <c>orleans_schema_history</c>, whichever flavour recorded them.
/// </summary>
internal static class SchemaValidation
{
    /// <param name="context">A context whose model maps the ledger (<see cref="SchemaHistoryModel"/>).</param>
    /// <param name="component">The ledger key of the component.</param>
    /// <param name="minimumVersion">The oldest schema version the code runs against.</param>
    /// <param name="latestVersion">The newest schema version the code knows.</param>
    /// <param name="migrationStep">The name of the component's migration step, named in the errors.</param>
    /// <param name="cancellationToken">Cancels the ledger read.</param>
    public static async Task ValidateAsync(DbContext context, string component, int minimumVersion, int latestVersion, string migrationStep, CancellationToken cancellationToken)
    {
        List<int> versions;
        try
        {
            versions = await SchemaHistoryModel.Set(context)
                .Where(row => EF.Property<string>(row, SchemaHistoryModel.Component) == component)
                .Select(row => EF.Property<int>(row, SchemaHistoryModel.Version))
                .OrderBy(version => version)
                .ToListAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            throw new SchemaVersionException(
                $"Cannot read the {component} schema history. Create the Orleans tables with an EF Core migration that calls migrationBuilder.{migrationStep}({latestVersion}).",
                exception);
        }

        if (versions.Count == 0)
        {
            throw new SchemaVersionException(
                $"No {component} schema version is recorded. Call migrationBuilder.{migrationStep}({latestVersion}) in the migration that creates the Orleans tables.");
        }

        for (var i = 0; i < versions.Count; i++)
        {
            if (versions[i] != i + 1)
            {
                throw new SchemaVersionException($"The {component} schema history is missing version {i + 1}.");
            }
        }

        var current = versions[^1];
        if (current > latestVersion)
        {
            throw new SchemaVersionException(
                $"The {component} schema is at version {current}, newer than the latest version {latestVersion} this code supports. Upgrade the package.");
        }

        if (current < minimumVersion)
        {
            throw new SchemaVersionException(
                $"The {component} schema is at version {current}, and this code needs at least version {minimumVersion}. Add an EF Core migration that calls migrationBuilder.{migrationStep}({minimumVersion}).");
        }
    }
}
