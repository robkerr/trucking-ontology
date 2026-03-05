"""
create_trucking_eventhouse.py — Create an Eventhouse, KQL Database, and tables in Fabric.

Creates `eh_trucking` (Eventhouse) and `trucking_db` (KQL Database) in the target
workspace, then executes the table DDLs from ``eventhouse_setup.kql``.
All operations are idempotent — existing resources are detected and skipped.

Usage:
    python scripts/create_trucking_eventhouse.py \\
        --workspace-id <WORKSPACE_GUID>

    # or via environment variable:
    export FABRIC_WORKSPACE_ID=<WORKSPACE_GUID>
    python scripts/create_trucking_eventhouse.py

Requirements:
    pip install sempy-labs requests azure-identity
"""

import argparse
import os
import pathlib
import sys
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EVENTHOUSE_NAME = "eh_trucking"
KQL_DB_NAME     = "trucking_db"
KUSTO_SCOPE     = "https://kusto.kusto.windows.net/.default"
KQL_SETUP_FILE  = pathlib.Path(__file__).resolve().parent / "eventhouse_setup.kql"

# ---------------------------------------------------------------------------
# Embedded DDL statements (sourced from eventhouse_setup.kql)
# ---------------------------------------------------------------------------

_KQL_DDL_STATEMENTS = [
    """.create-merge table TelemetryEvent (
    event_id: string,
    event_type: string,
    timestamp: datetime,
    source: string,
    truck_id: string,
    trip_id: string,
    driver_id: string,
    latitude: real,
    longitude: real,
    speed_mph: real,
    heading_degrees: int,
    fuel_pct: real,
    engine_temp_f: int,
    oil_pressure_psi: int,
    odometer_miles: long,
    engine_rpm: int,
    ambient_temp_f: int,
    def_level_pct: real
)""",
    """.alter table TelemetryEvent policy retention ```{ "SoftDeletePeriod": "365.00:00:00", "Recoverability": "Enabled" }```""",
    """.create-merge table EngineFaultEvent (
    event_id: string,
    event_type: string,
    timestamp: datetime,
    source: string,
    truck_id: string,
    trip_id: string,
    driver_id: string,
    spn: int,
    fmi: int,
    fault_description: string,
    severity: string,
    occurrence_count: int,
    latitude: real,
    longitude: real,
    action: string
)""",
    """.create-merge table GeofenceEvent (
    event_id: string,
    event_type: string,
    timestamp: datetime,
    source: string,
    truck_id: string,
    trip_id: string,
    driver_id: string,
    terminal_id: string,
    terminal_name: string,
    geofence_event: string,
    latitude: real,
    longitude: real
)""",
    """.create-merge table HOSStatusChangeEvent (
    event_id: string,
    event_type: string,
    timestamp: datetime,
    source: string,
    driver_id: string,
    trip_id: string,
    truck_id: string,
    previous_status: string,
    new_status: string,
    driving_hours_used: real,
    driving_hours_remaining: real,
    duty_hours_used: real,
    duty_hours_remaining: real,
    cycle_hours_used: real,
    cycle_hours_remaining: real,
    break_time_remaining_minutes: int,
    latitude: real,
    longitude: real
)""",
    """.create-merge table LoadStatusEvent (
    event_id: string,
    event_type: string,
    timestamp: datetime,
    source: string,
    load_id: string,
    trip_id: string,
    customer_id: string,
    load_number: string,
    previous_status: string,
    new_status: string,
    terminal_id: string,
    latitude: real,
    longitude: real,
    estimated_arrival: datetime,
    notes: string
)""",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_client():
    import sempy.fabric as fabric
    return fabric.FabricRestClient()


def _poll_lro(client, resp, label, list_fn=None, timeout=300):
    """Poll a Fabric long-running operation (202 Accepted) until it succeeds.

    Checks all common Fabric LRO headers. If none are present, falls back to
    calling list_fn() (a zero-arg callable) until it returns a non-None value.
    """
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code != 202:
        resp.raise_for_status()

    # Fabric uses several header names across different item types
    op_url = (
        resp.headers.get("Operation-Location")
        or resp.headers.get("operation-location")
        or resp.headers.get("Location")
        or resp.headers.get("location")
        or resp.headers.get("x-ms-operation-location")
    )

    print(f"  Provisioning {label}...", end="", flush=True)
    deadline = time.time() + timeout

    if op_url:
        # Standard LRO polling via operation URL
        while time.time() < deadline:
            time.sleep(5)
            r = client.get(op_url)
            r.raise_for_status()
            body = r.json()
            status = body.get("status", "").lower()
            if status == "succeeded":
                print(" done.")
                return body
            elif status == "failed":
                raise RuntimeError(f"{label} provisioning failed: {body}")
            print(".", end="", flush=True)
    elif list_fn:
        # Fallback: poll the list endpoint until the item appears
        while time.time() < deadline:
            time.sleep(5)
            result = list_fn()
            if result is not None:
                print(" done.")
                return result
            print(".", end="", flush=True)
    else:
        # No polling URL and no fallback — just wait a moment and continue
        time.sleep(10)
        print(" done (no operation URL; assumed complete).")
        return None

    raise TimeoutError(f"{label} provisioning timed out after {timeout}s.")


def ensure_eventhouse(workspace_id: str, client=None) -> str:
    """Return EVENTHOUSE_ID, creating the Eventhouse if it does not exist."""
    if client is None:
        client = _get_client()

    resp = client.get(f"v1/workspaces/{workspace_id}/eventhouses")
    resp.raise_for_status()
    existing = next(
        (e for e in resp.json().get("value", []) if e["displayName"] == EVENTHOUSE_NAME),
        None,
    )
    if existing:
        eh_id = existing["id"]
        print(f"  ℹ️  Eventhouse '{EVENTHOUSE_NAME}' already exists — EVENTHOUSE_ID = {eh_id}")
        return eh_id

    print(f"  Creating Eventhouse '{EVENTHOUSE_NAME}'...")
    resp = client.post(
        f"v1/workspaces/{workspace_id}/eventhouses",
        json={"displayName": EVENTHOUSE_NAME},
    )

    def _eh_ready():
        r = client.get(f"v1/workspaces/{workspace_id}/eventhouses")
        r.raise_for_status()
        return next(
            (e for e in r.json().get("value", []) if e["displayName"] == EVENTHOUSE_NAME), None
        )

    _poll_lro(client, resp, EVENTHOUSE_NAME, list_fn=_eh_ready)

    # Re-fetch to get the stable item ID
    resp2 = client.get(f"v1/workspaces/{workspace_id}/eventhouses")
    resp2.raise_for_status()
    eh = next(e for e in resp2.json()["value"] if e["displayName"] == EVENTHOUSE_NAME)
    eh_id = eh["id"]
    print(f"  ✅ Eventhouse created — EVENTHOUSE_ID = {eh_id}")
    return eh_id


def ensure_kql_database(workspace_id: str, eventhouse_id: str, client=None) -> str:
    """Return KQL_DB_ID, creating the KQL Database inside the Eventhouse if it does not exist."""
    if client is None:
        client = _get_client()

    resp = client.get(f"v1/workspaces/{workspace_id}/kqlDatabases")
    resp.raise_for_status()
    existing = next(
        (d for d in resp.json().get("value", []) if d["displayName"] == KQL_DB_NAME),
        None,
    )
    if existing:
        db_id = existing["id"]
        print(f"  ℹ️  KQL Database '{KQL_DB_NAME}' already exists — KQL_DB_ID = {db_id}")
        return db_id

    print(f"  Creating KQL Database '{KQL_DB_NAME}' inside Eventhouse...")
    resp = client.post(
        f"v1/workspaces/{workspace_id}/kqlDatabases",
        json={
            "displayName": KQL_DB_NAME,
            "creationPayload": {
                "databaseType": "ReadWrite",
                "parentEventhouseItemId": eventhouse_id,
            },
        },
    )

    def _db_ready():
        r = client.get(f"v1/workspaces/{workspace_id}/kqlDatabases")
        r.raise_for_status()
        return next(
            (d for d in r.json().get("value", []) if d["displayName"] == KQL_DB_NAME), None
        )

    _poll_lro(client, resp, KQL_DB_NAME, list_fn=_db_ready)

    # Re-fetch to get stable ID
    resp2 = client.get(f"v1/workspaces/{workspace_id}/kqlDatabases")
    resp2.raise_for_status()
    db = next(d for d in resp2.json()["value"] if d["displayName"] == KQL_DB_NAME)
    db_id = db["id"]
    print(f"  ✅ KQL Database created — KQL_DB_ID = {db_id}")
    return db_id


def create_kql_tables(
    workspace_id: str,
    eventhouse_id: str,
    kql_db_name: str,
    client=None,
) -> list:
    """Create KQL tables in the target database via the Eventhouse management API.

    Resolves the ``queryServiceUri`` from the Eventhouse item, then executes each
    embedded DDL statement (.create-merge table and .alter table policy) against the
    Kusto management endpoint.

    Token acquisition order:
      1. ``notebookutils.credentials.getToken`` — used inside a Fabric notebook.
      2. ``azure.identity.DefaultAzureCredential`` — fallback for CLI / local runs.

    Returns a list of table names that were successfully created or merged.
    """
    import requests as _requests

    if client is None:
        client = _get_client()

    # 1. Resolve queryServiceUri from the Eventhouse item
    resp = client.get(f"v1/workspaces/{workspace_id}/eventhouses/{eventhouse_id}")
    resp.raise_for_status()
    props = resp.json().get("properties", {})
    query_uri = props.get("queryServiceUri") or props.get("queryUri")
    if not query_uri:
        raise RuntimeError(
            f"queryServiceUri not found in Eventhouse properties. "
            f"Available keys: {list(props.keys())}"
        )

    # 2. Acquire a Kusto bearer token
    token = None
    try:
        import notebookutils  # available inside a Fabric notebook
        token = notebookutils.credentials.getToken("https://kusto.kusto.windows.net")
    except Exception:
        from azure.identity import DefaultAzureCredential
        token = DefaultAzureCredential().get_token(KUSTO_SCOPE).token

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    mgmt_url = f"{query_uri.rstrip('/')}/v1/rest/mgmt"

    # 3. Execute each DDL statement
    created_tables: list = []
    total = len(_KQL_DDL_STATEMENTS)
    for i, stmt in enumerate(_KQL_DDL_STATEMENTS, 1):
        first_line = stmt.splitlines()[0][:80]
        try:
            r = _requests.post(
                mgmt_url,
                headers=headers,
                json={"db": kql_db_name, "csl": stmt, "properties": {}},
                timeout=60,
            )
            r.raise_for_status()
            print(f"  ✅ [{i}/{total}] {first_line}")
            # Collect table name from .create-merge table statements
            tokens = stmt.split()
            if len(tokens) >= 3 and tokens[0] == ".create-merge" and tokens[1] == "table":
                table_name = tokens[2]
                if table_name not in created_tables:
                    created_tables.append(table_name)
        except Exception as exc:
            print(f"  ✗ [{i}/{total}] {first_line}")
            print(f"       Error: {exc}")

    return created_tables


def _parse_kql_commands(kql_path):
    """Parse a .kql file into individual executable management commands."""
    text = pathlib.Path(kql_path).read_text(encoding="utf-8")
    commands = []
    current = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        if stripped.startswith("."):
            if current:
                cmd = "\n".join(current).strip()
                if cmd:
                    commands.append(cmd)
            current = [line]
        elif current:
            current.append(line)
    if current:
        cmd = "\n".join(current).strip()
        if cmd:
            commands.append(cmd)
    return commands


def run_kql_setup(workspace_id, kql_db_id, kql_path=KQL_SETUP_FILE, client=None):
    """Execute KQL management commands from a .kql file against the database."""
    import requests as _requests
    from azure.identity import DefaultAzureCredential

    if client is None:
        client = _get_client()

    # Resolve the Kusto query-service URI from database properties
    resp = client.get(f"v1/workspaces/{workspace_id}/kqlDatabases/{kql_db_id}")
    resp.raise_for_status()
    props = resp.json().get("properties", {})
    query_uri = props.get("queryServiceUri") or props.get("queryUri")
    if not query_uri:
        raise RuntimeError("queryServiceUri not found in KQL database properties.")

    commands = _parse_kql_commands(kql_path)
    if not commands:
        print("  ⚠️  No KQL commands found in", kql_path)
        return

    token = DefaultAzureCredential().get_token(KUSTO_SCOPE).token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    mgmt_url = f"{query_uri.rstrip('/')}/v1/rest/mgmt"

    for i, cmd in enumerate(commands, 1):
        first_line = cmd.splitlines()[0][:70]
        print(f"    [{i}/{len(commands)}] {first_line}")
        r = _requests.post(
            mgmt_url, headers=headers,
            json={"db": KQL_DB_NAME, "csl": cmd},
        )
        r.raise_for_status()

    print(f"  ✅ Executed {len(commands)} KQL command(s) from {pathlib.Path(kql_path).name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Create Eventhouse and KQL Database for the Trucking workshop."
    )
    parser.add_argument(
        "--workspace-id",
        default=os.environ.get("FABRIC_WORKSPACE_ID"),
        help="Fabric workspace GUID (or set FABRIC_WORKSPACE_ID env var)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.workspace_id:
        print("ERROR: --workspace-id is required (or set FABRIC_WORKSPACE_ID).")
        sys.exit(1)

    print("=" * 60)
    print(f"  Eventhouse : {EVENTHOUSE_NAME}")
    print(f"  KQL DB     : {KQL_DB_NAME}")
    print(f"  Workspace  : {args.workspace_id}")
    print("=" * 60)

    client = _get_client()

    print("\n[1/4] Eventhouse...")
    eventhouse_id = ensure_eventhouse(args.workspace_id, client)

    print("\n[2/4] KQL Database...")
    kql_db_id = ensure_kql_database(args.workspace_id, eventhouse_id, client)

    print("\n[3/4] KQL Table DDLs (embedded)...")
    created = create_kql_tables(args.workspace_id, eventhouse_id, KQL_DB_NAME, client)
    print(f"  Tables ready: {', '.join(created) if created else '(none)'}")

    print("\n[4/4] KQL Table DDLs (from file)...")
    run_kql_setup(args.workspace_id, kql_db_id, client=client)

    print(f"\n✅ Eventhouse  : {EVENTHOUSE_NAME} ({eventhouse_id})")
    print(f"   KQL Database : {KQL_DB_NAME} ({kql_db_id})")
    print("\nDone.")


if __name__ == "__main__":
    main()
