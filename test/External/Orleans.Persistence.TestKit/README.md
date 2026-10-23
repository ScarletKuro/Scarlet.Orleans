# Orleans.Persistence.TestKit (vendored)

The `IGrainStorage` conformance suites of Orleans, run against every flavour and engine of this repository:
`GrainStorageTestRunner`, the Accordant model-based `GrainStorageModelBasedTestRunner`, and the silo-hosted
`GrainStorageTestFixture`.

- Source: https://github.com/dotnet/orleans/tree/v10.4.0/src/Orleans.Persistence.TestKit
- Commit: `d114d7522a53b8cce4c6b97d44293e5c7273d766`
- License: MIT, Copyright (c) .NET Foundation

Orleans builds this test kit but does not publish it (`IsPackable` is false), so its sources are copied here unmodified.
To update, copy the files from a newer Orleans tag and update the commit above. Do not edit them in place.
