# In-box Orleans ADO.NET grain directory scripts (vendored)

Schema scripts for `Microsoft.Orleans.GrainDirectory.AdoNet`, used for the benchmark comparison.
Main scripts are linked from the persistence benchmarks.

- Source: https://github.com/dotnet/orleans/tree/v10.4.0/src/AdoNet
- Commit: `d114d7522a53b8cce4c6b97d44293e5c7273d766`
- License: MIT, Copyright (c) .NET Foundation

MySQL client delimiter corrections: terminate procedure bodies with `END$$` and reset `DELIMITER ;` before
`OrleansQuery` inserts. The upstream script repeats `DELIMITER $$` without matching terminators. Table definitions,
query text and procedure bodies are unchanged. SQL Server batches are split at `GO` by the shared script loader.
