"""
create_trucking_eventhouse.py — Create an Eventhouse and KQL Database in Fabric.

Creates `eh_trucking` (Eventhouse) and `trucking_db` (KQL Database) in the target
workspace. Both operations are idempotent — existing resources are detected and
skipped; only the IDs are returned.

Usage:
    python scripts/create_trucking_eventhouse.py \
        --workspace-id <WORKSPACE_GUID>

    # or via environment variable:
    export FABRIC_WORKSPACE_ID=<WORKSPACE_GUID>
    python scripts/create_trucking_eventhouse.py

Requirements:
    pip install sempy-labs
"""

import argparse
import os
import sys
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EVENTHOUSE_NAME = "eh_trucking"
KQL_DB_NAME     = "trucking_db"

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

    print("\n[1/2] Eventhouse...")
    eventhouse_id = ensure_eventhouse(args.workspace_id, client)

    print("\n[2/2] KQL Database...")
    kql_db_id = ensure_kql_database(args.workspace_id, eventhouse_id, client)

    print(f"\n✅ Eventhouse  : {EVENTHOUSE_NAME} ({eventhouse_id})")
    print(f"   KQL Database : {KQL_DB_NAME} ({kql_db_id})")
    print("\nDone.")


if __name__ == "__main__":
    main()
