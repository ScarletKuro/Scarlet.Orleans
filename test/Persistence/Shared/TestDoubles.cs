using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Orleans.Serialization.Activators;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Scarlet.Orleans.Persistence.Tests;

public sealed class TestState
{
    public int Value { get; set; }

    public string? Text { get; set; }
}

[JsonSerializable(typeof(TestState))]
internal sealed partial class TestJsonContext : JsonSerializerContext;

/// <summary>
/// Serializes <see cref="TestState"/> through source-generated metadata, so it also runs under NativeAOT.
/// </summary>
public sealed class JsonTestSerializer : IGrainStorageSerializer
{
    public BinaryData Serialize<T>(T? input) => new(JsonSerializer.SerializeToUtf8Bytes(input!, TypeInfo<T>()));

    public T? Deserialize<T>(BinaryData input) => JsonSerializer.Deserialize(input.ToMemory().Span, TypeInfo<T>());

    private static JsonTypeInfo<T> TypeInfo<T>() =>
        (JsonTypeInfo<T>)(TestJsonContext.Default.GetTypeInfo(typeof(T)) ?? throw new NotSupportedException(typeof(T).FullName));
}

/// <summary>
/// Creates <see cref="TestState"/> instances without reflection, so it also runs under NativeAOT.
/// </summary>
public sealed class TestActivatorProvider : IActivatorProvider
{
    public IActivator<T> GetActivator<T>() => new Activator<T>();

    private sealed class Activator<T> : IActivator<T>
    {
        public T Create() => typeof(T) == typeof(TestState) ? (T)(object)new TestState() : throw new NotSupportedException(typeof(T).FullName);
    }
}

