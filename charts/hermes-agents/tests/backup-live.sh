#!/usr/bin/env bash
set -euo pipefail

: "${HERMES_IMAGE:?HERMES_IMAGE is required}"
: "${RESTIC_IMAGE:?RESTIC_IMAGE is required}"

run_id="hermes-backup-test-$$"
fixture="${run_id}-fixture"
restored="${run_id}-restored"
writer="${run_id}-writer"
tmp="$(mktemp -d)"
cleanup() {
  docker rm -f "$writer" >/dev/null 2>&1 || true
  docker volume rm "$fixture" "$restored" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

docker volume create "$fixture" >/dev/null
docker volume create "$restored" >/dev/null
mkdir -p "$tmp/archive" "$tmp/repository" "$tmp/restore"

docker run --rm --entrypoint /opt/hermes/.venv/bin/python \
  -v "$fixture:/opt/data" "$HERMES_IMAGE" -c '
import pathlib, sqlite3
home = pathlib.Path("/opt/data")
(home / "SOUL.md").write_text("# Live backup fixture\n", encoding="utf-8")
(home / "config.yaml").write_text("model: fixture\n", encoding="utf-8")
db = sqlite3.connect(home / "state.db")
db.execute("PRAGMA journal_mode=WAL")
db.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
db.execute("INSERT INTO events(value) VALUES ('"'"'before-backup'"'"')")
db.commit()
db.close()
'

docker run -d --name "$writer" --entrypoint /opt/hermes/.venv/bin/python \
  -v "$fixture:/opt/data" "$HERMES_IMAGE" -c '
import sqlite3, time
db = sqlite3.connect("/opt/data/state.db")
for value in range(10000):
    db.execute("INSERT INTO events(value) VALUES (?)", (f"live-{value}",))
    db.commit()
    time.sleep(0.01)
' >/dev/null

for _ in $(seq 1 100); do
  rows="$(docker exec "$writer" /opt/hermes/.venv/bin/python -c 'import sqlite3; print(sqlite3.connect("/opt/data/state.db").execute("SELECT count(*) FROM events").fetchone()[0])')"
  if (( rows > 1 )); then
    break
  fi
  sleep 0.05
done
test "${rows:-0}" -gt 1
test "$(docker inspect --format '{{.State.Running}}' "$writer")" = true

docker run --rm --entrypoint /opt/hermes/.venv/bin/hermes \
  -v "$fixture:/opt/data:ro" -v "$tmp/archive:/backup" \
  "$HERMES_IMAGE" backup --output /backup/hermes.zip

docker run --rm --entrypoint /opt/hermes/.venv/bin/python \
  -v "$tmp/archive:/backup:ro" "$HERMES_IMAGE" -c '
import pathlib, shutil, sqlite3, tempfile, zipfile
archive = pathlib.Path("/backup/hermes.zip")
assert archive.stat().st_size > 0
with zipfile.ZipFile(archive) as source:
    assert source.testzip() is None
    assert {pathlib.PurePosixPath(name).name for name in source.namelist()} & {"config.yaml", ".env", "state.db"}
    for member in source.infolist():
        if pathlib.PurePosixPath(member.filename).suffix != ".db":
            continue
        with tempfile.NamedTemporaryFile(suffix=".db") as target:
            with source.open(member) as database:
                shutil.copyfileobj(database, target)
            target.flush()
            db = sqlite3.connect(f"file:{target.name}?mode=ro", uri=True)
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            db.close()
'

docker run --rm -e RESTIC_PASSWORD=disposable-test-password \
  -v "$tmp/repository:/repo" "$RESTIC_IMAGE" init --repo /repo
docker run --rm -e RESTIC_PASSWORD=disposable-test-password \
  -v "$tmp/repository:/repo" -v "$tmp/archive:/backup:ro" "$RESTIC_IMAGE" \
  --repo /repo backup --host agent-live-fixture --tag hermes-agent \
  --tag agent:agent-live-fixture /backup/hermes.zip
docker run --rm -e RESTIC_PASSWORD=disposable-test-password \
  -v "$tmp/repository:/repo" "$RESTIC_IMAGE" --repo /repo check
docker run --rm -e RESTIC_PASSWORD=disposable-test-password \
  -v "$tmp/repository:/repo" "$RESTIC_IMAGE" --repo /repo snapshots --json \
  --host agent-live-fixture --tag hermes-agent --tag agent:agent-live-fixture >"$tmp/snapshots.json"
python3 - "$tmp/snapshots.json" <<'PY'
import json, pathlib, sys
snapshots = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert len(snapshots) == 1
assert snapshots[0]["hostname"] == "agent-live-fixture"
assert {"hermes-agent", "agent:agent-live-fixture"} <= set(snapshots[0]["tags"])
PY
docker run --rm -e RESTIC_PASSWORD=disposable-test-password \
  -v "$tmp/repository:/repo:ro" -v "$tmp/restore:/restore" "$RESTIC_IMAGE" \
  --repo /repo restore latest --target /restore --host agent-live-fixture \
  --tag hermes-agent --tag agent:agent-live-fixture

docker run --rm --entrypoint /opt/hermes/.venv/bin/hermes \
  -v "$restored:/opt/data" -v "$tmp/restore:/restore:ro" "$HERMES_IMAGE" \
  import /restore/backup/hermes.zip --force
docker run --rm --entrypoint /opt/hermes/.venv/bin/python \
  -v "$restored:/opt/data:ro" "$HERMES_IMAGE" -c '
import pathlib, sqlite3
home = pathlib.Path("/opt/data")
assert home.joinpath("SOUL.md").read_text(encoding="utf-8") == "# Live backup fixture\n"
db = sqlite3.connect(f"file:{home / '"'"'state.db'"'"'}?mode=ro", uri=True)
assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
assert db.execute("SELECT count(*) FROM events").fetchone()[0] >= 2
assert db.execute("SELECT value FROM events ORDER BY id LIMIT 1").fetchone()[0] == "before-backup"
assert db.execute("SELECT count(*) FROM events WHERE value LIKE '"'"'live-%'"'"'").fetchone()[0] >= 1
db.close()
'
