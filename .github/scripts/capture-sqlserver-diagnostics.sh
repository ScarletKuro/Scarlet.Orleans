#!/usr/bin/env bash
# Read-only snapshot before Actions tears the service container down. Never dump its environment/connection strings.
set -uo pipefail
container="$1"
destination="artifacts/sqlserver-diagnostics"
mkdir -p "$destination"
timeout 15 docker logs --timestamps "$container" > "$destination/container.log" 2>&1 || true
timeout 15 docker inspect --format '{{json .State}}' "$container" > "$destination/container-state.json" 2>&1 || true
timeout 15 docker stats --no-stream "$container" > "$destination/resources.txt" 2>&1 || true
timeout 15 docker exec "$container" bash -c 'for file in /var/opt/mssql/log/errorlog*; do
  if [ -f "$file" ]; then printf "\n=== %s ===\n" "$file"; cat "$file"; fi
done' > "$destination/errorlogs.txt" 2>&1 || true
timeout 30 docker exec -i "$container" bash -c '
  sqlcmd=/opt/mssql-tools18/bin/sqlcmd
  if [ ! -x "$sqlcmd" ]; then sqlcmd=/opt/mssql-tools/bin/sqlcmd; fi
  "$sqlcmd" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -l 5 -t 15 -y 0 -i /dev/stdin
' < "$(dirname "$0")/sqlserver-diagnostics.sql" > "$destination/server-health.txt" 2>&1 || true
