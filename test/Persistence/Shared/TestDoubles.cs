using System.Text.Json;
using Orleans.Serialization.Activators;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class TestState
{
    public int Value { get; set; }

    public string? Text { get; set; }
}

public sealed class JsonTestSerializer : IGrainStorageSerializer
{
    public BinaryData Serialize<T>(T? input) => new(JsonSerializer.SerializeToUtf8Bytes(input));

    public T? Deserialize<T>(BinaryData input) => JsonSerializer.Deserialize<T>(input.ToMemory().Span);
}

public sealed class TestActivatorProvider : IActivatorProvider
{
    public IActivator<T> GetActivator<T>() => new Activator<T>();

    private sealed class Activator<T> : IActivator<T>
    {
        public T Create() => Activator.CreateInstance<T>();
    }
}

/// <summary>
/// How the storage under test treats the schema. Each flavour maps it to its own <c>SchemaMode</c>.
/// </summary>
public enum TestSchemaMode
{
    None,
    Validate,
    Apply,
}
