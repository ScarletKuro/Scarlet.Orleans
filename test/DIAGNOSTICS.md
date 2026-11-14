# Integration-test failures and diagnostics

The CI database jobs run test modules sequentially. Within a module, ordinary test classes may run concurrently.
Tests that kill database sessions or discard TCP responses belong to the **Connection faults** collection, which
does not run alongside other collections. Concurrency inside those tests is preserved: their competing writers and
fault injectors still run together. The inherited collection applies to both ADO.NET and EF Core suites, and the
NativeAOT variants. See [xUnit's collection parallelism rules](https://xunit.net/docs/running-tests-in-parallel).

This containment avoids exposing ordinary tests to destructive fault workloads, database teardown and shared driver
pool activity at the same time. It is not proof that a particular historical disconnect was caused by another test.

## What CI retains on failure

The ordinary CI test jobs enable xUnit's TRX reporter. Their diagnostic artifact includes:

- `TestResults/**` at the repository root, plus any project-local `TestResults` folders: results for every test,
  including start/end times and full failure messages/stacks. The root path matters: the reporter uses it by default.
- `test/**/test-logs/**`: existing Orleans runner logs and a `database-<process-id>.log` per test process. The database
  audit records UTC times, database identity, current test when available, create/drop boundaries, deliberate session
  killing, successful SQL Server KILL target lists, and errors in the killer loop. Proxy entries include target/listener
  endpoints, connections, byte counts by direction, drops and exceptions. No packet contents or connection strings
  are written.
- For the SQL Server matrix, `artifacts/sqlserver-diagnostics/**`: container output, container state/restart/OOM data,
  a resource snapshot, SQL Server error logs, server version/start time, user sessions, and the existing `system_health`
  ring buffer. The capture script performs read-only queries before Actions tears the service container down.

Some ring-buffer XML can be truncated by SQL Server. Preserve the original file; use the server error logs and
test/database timestamps as additional evidence. Snapshot failures are retained as text and do not replace the test
failure. The capture step does not dump the container environment or an administrative connection string.

To reproduce a focused SQL Server test run, set `SCARLET_TEST_SQLSERVER`, then use:

```sh
dotnet test --project test/GrainDirectory/Scarlet.Orleans.GrainDirectory.AdoNet.Tests -c Release --report-xunit-trx -- --filter-class '*SqlServer*'
```

To exercise the server capture locally with the usual test container:

```sh
bash .github/scripts/capture-sqlserver-diagnostics.sh scarlet-mssql
```

Correlate failed-test times with `kill.begin`, `kill.targets`, `kill.end`, and database teardown in the audit. Then check
server error logs for session termination, restart, fatal errors or resource pressure. A transport error during a
lookup or schema initialization does not itself demonstrate a directory consistency failure or a SQL deadlock.

## Investigation of run 43648388755

[SQL Server 2022 job](https://github.com/ScarletKuro/Scarlet.Orleans/actions/runs/43648388755/job/132611194020),
commit `cf32c033b649fdef3eea290532f732f88aaa3e9b`, failed two ADO.NET grain-directory cases:

| Case | Failure location | Observed error |
|---|---|---|
| `ChaosTest_OverlappingLifecycles_LeaveTheDirectoryEmpty` | Directory lookup | TCP Provider error 2, connection terminated |
| `HashCollision_NeitherReturnsNorReplacesTheOtherGrainsEntry` | Schema-lock acquisition during initialization | Same transport error |

These are separate from the SQL Server streaming receipt-write deadlocks. Neither was an assertion about incorrect
directory contents. The artifact lacked grain-directory runner logs, complete test results, fault timing, and server
logs, so the original disconnect's cause remains unproven. Retrying every transport error in production would not be
an evidence-based fix: a dropped write response can leave its outcome unknown.

Local repetition also exposed a different harness defect: the lossy proxy's initial SQL Server login timed out before
fault injection began. Three original runs and three runs with fault-test isolation alone each passed the other 34
SQL Server directory cases but failed that proxy setup. Isolation alone therefore did not fix this local failure.

`ChaosProxy` awaited network operations on the caller's synchronization context. If the driver synchronously waits
for a handshake response, the proxy can wait for that same context to dispatch its relay. Its network awaits now use
`ConfigureAwait(false)`. A focused TCP echo regression creates the proxy on a deliberately paused synchronization
context: it times out with the original proxy and passes with the corrected proxy, without needing SQL Server or
random response drops. This proves the harness defect; it does not assign the historical Linux CI disconnect to it.

The local Windows SqlClient proxy-login timeout also persists after this correction: it forwards 94 request bytes and
54 response bytes with zero injected drops, then fails the pre-login handshake. Thus this investigation does **not**
claim the full SQL Server suite is green or that all transport failures are fixed. The two historical failing cases
pass locally. Keep the Windows native-driver/proxy login failure separate from the original Linux CI disconnect and
from the proven synchronization-context regression.

Raw local investigation output is under `artifacts/investigation/43648388755/` (ignored by Git). Keep the historical
CI failure classified as **unresolved transport failure**, and use the expanded artifact if it recurs.

Final targeted validation: 17 SQL Server conformance cases passed for each of ADO.NET and EF Core, including the two
historical failures; the SQL Server session-killing case and the TCP proxy regression passed. TRX timing confirms that
the isolated collection started after ordinary tests finished. The server-capture script was exercised against the
local container and the workflow's reporter/upload paths were checked. These targeted passes do not include a pass
of the Windows SQL Server lossy-proxy login case described above.
