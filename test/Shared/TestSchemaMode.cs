namespace Scarlet.Orleans.Tests;

/// <summary>
/// How the component under test treats the schema. Each flavour maps it to its own <c>SchemaMode</c>.
/// </summary>
public enum TestSchemaMode
{
    None,
    Validate,
    Apply,
}
