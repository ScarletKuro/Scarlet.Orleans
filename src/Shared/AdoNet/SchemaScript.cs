using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// One versioned schema script of a component for one engine.
/// </summary>
internal sealed record SchemaScript(int Version, string Name, string Text)
{
    /// <summary>
    /// SHA-256 of the script with line endings normalised, so a checkout's line-ending conversion is not an edit.
    /// </summary>
    public string Checksum { get; } = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Text.ReplaceLineEndings("\n"))));

    /// <summary>
    /// Loads a component's scripts for an engine from resources named <c>{Engine}-{Version:0000}-{Description}.sql</c>.
    /// </summary>
    /// <remarks>MariaDB runs the MySQL scripts unless it has its own.</remarks>
    public static IReadOnlyList<SchemaScript> Load(Assembly assembly, RelationalEngine engine)
    {
        var scripts = LoadFor(assembly, engine.ToString());
        if (scripts.Count == 0 && engine == RelationalEngine.MariaDb)
        {
            scripts = LoadFor(assembly, nameof(RelationalEngine.MySql));
        }

        if (scripts.Count == 0)
        {
            throw new NotSupportedException($"{assembly.GetName().Name} has no schema scripts for {engine}.");
        }

        for (var i = 0; i < scripts.Count; i++)
        {
            if (scripts[i].Version != i + 1)
            {
                throw new InvalidOperationException($"Schema scripts of {assembly.GetName().Name} for {engine} must be numbered 1..n without gaps.");
            }
        }

        return scripts;
    }

    private static List<SchemaScript> LoadFor(Assembly assembly, string engine)
    {
        var prefix = engine + "-";
        var scripts = new List<SchemaScript>();
        foreach (var resource in assembly.GetManifestResourceNames())
        {
            if (!resource.StartsWith(prefix, StringComparison.Ordinal) || !resource.EndsWith(".sql", StringComparison.Ordinal))
            {
                continue;
            }

            var version = int.Parse(resource.AsSpan(prefix.Length, 4), provider: System.Globalization.CultureInfo.InvariantCulture);
            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var reader = new StreamReader(stream, Encoding.UTF8);
            scripts.Add(new SchemaScript(version, resource, reader.ReadToEnd()));
        }

        scripts.Sort(static (x, y) => x.Version.CompareTo(y.Version));
        return scripts;
    }
}
