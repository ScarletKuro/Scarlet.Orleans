namespace Scarlet.Orleans.Relational.Schema;

/// <summary>
/// The database schema does not match what this code supports.
/// </summary>
internal sealed class SchemaVersionException(string message, Exception? innerException = null) : InvalidOperationException(message, innerException);
