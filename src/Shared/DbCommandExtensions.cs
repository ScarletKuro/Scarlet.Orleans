using System.Data;
using System.Data.Common;

namespace Scarlet.Orleans.Relational;

internal static class DbCommandExtensions
{
    /// <summary>
    /// Adds a parameter referenced as <c>@name</c>, the placeholder syntax every supported driver accepts.
    /// </summary>
    public static void AddParameter(this DbCommand command, string name, object? value, DbType type)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = "@" + name;
        parameter.DbType = type;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
