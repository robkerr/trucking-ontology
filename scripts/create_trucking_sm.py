"""
create_trucking_sm.py — Patch, publish, and refresh the TruckingSM semantic model.

Expects TruckingSM.SemanticModel files to already be present locally (e.g. downloaded
by the setup notebook). Patches the Direct Lake connection in expressions.tmdl to point
to the target Lakehouse, publishes via fabric-cicd, then triggers a full refresh.

Usage:
    python scripts/create_trucking_sm.py \
        --workspace-id  <WORKSPACE_GUID> \
        --lakehouse-id  <LAKEHOUSE_GUID> \
        --sm-dir        <path/to/TruckingSM.SemanticModel>

    # or via environment variables (sm-dir defaults to ./builtin/TruckingSM.SemanticModel):
    export FABRIC_WORKSPACE_ID=<WORKSPACE_GUID>
    export FABRIC_LAKEHOUSE_ID=<LAKEHOUSE_GUID>
    python scripts/create_trucking_sm.py

Requirements:
    pip install fabric-cicd sempy-labs
"""

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SM_MODEL_NAME = "TruckingSM"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_fabric_cicd():
    import importlib.metadata

    # pip install in a subprocess doesn't make the module available in-process
    # until caches are cleared and the module is explicitly (re)imported.
    try:
        import fabric_cicd  # noqa: F401
        version = importlib.metadata.version("fabric-cicd")
        print(f"  fabric-cicd {version} already installed.")
        return
    except (ImportError, importlib.metadata.PackageNotFoundError):
        pass

    print("  Installing fabric-cicd...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "fabric-cicd", "-q"])

    # Flush the import system so the newly installed package is found
    import importlib
    importlib.invalidate_caches()
    import sys as _sys
    _sys.modules.pop("fabric_cicd", None)
    import fabric_cicd  # noqa: F401

    version = importlib.metadata.version("fabric-cicd")
    print(f"  fabric-cicd {version} installed.")


def patch_expressions(sm_dir: Path, workspace_id: str, lakehouse_id: str):
    expressions_file = sm_dir / "definition" / "expressions.tmdl"
    if not expressions_file.exists():
        raise FileNotFoundError(
            f"expressions.tmdl not found at {expressions_file}\n"
            "Make sure the SM files were downloaded before calling this function."
        )
    src = expressions_file.read_text(encoding="utf-8")
    patched = re.sub(
        r"https://onelake\.dfs\.fabric\.microsoft\.com/[0-9a-f-]{36}/[0-9a-f-]{36}",
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}",
        src,
    )
    if f"{workspace_id}/{lakehouse_id}" not in patched:
        raise RuntimeError(
            "Patch failed — could not find OneLake URL in expressions.tmdl.\n"
            "Verify WORKSPACE_ID and LAKEHOUSE_ID are correct GUIDs."
        )
    expressions_file.write_text(patched, encoding="utf-8")
    print(
        f"  expressions.tmdl patched.\n"
        f"  OneLake URL: https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}"
    )


def find_semantic_model(workspace_id: str):
    """Return the semantic model ID if SM_MODEL_NAME already exists, else None."""
    import sempy.fabric as fabric

    client = fabric.FabricRestClient()
    resp = client.get(f"v1/workspaces/{workspace_id}/semanticModels")
    resp.raise_for_status()
    models = resp.json().get("value", [])
    match = next((m for m in models if m.get("displayName") == SM_MODEL_NAME), None)
    return match["id"] if match else None


def publish_model(workspace_id: str, repository_dir: Path):
    from fabric_cicd import FabricWorkspace, publish_all_items

    print(f"  Publishing {SM_MODEL_NAME} to workspace {workspace_id}...")
    workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=str(repository_dir),
        item_type_in_scope=["SemanticModel"],
    )
    publish_all_items(workspace)
    print(f"  Semantic model deployed successfully.")


def trigger_refresh(workspace_id: str):
    import sempy.fabric as fabric

    client = fabric.FabricRestClient()

    # Find model ID via Fabric REST API
    resp = client.get(f"v1/workspaces/{workspace_id}/semanticModels")
    resp.raise_for_status()
    models = resp.json().get("value", [])
    match = [m for m in models if m.get("displayName") == SM_MODEL_NAME]
    if not match:
        raise RuntimeError(
            f"{SM_MODEL_NAME} not found in workspace.\n"
            "Make sure the publish step completed successfully."
        )
    sm_id = match[0]["id"]
    print(f"  Found semantic model: {SM_MODEL_NAME} ({sm_id})")

    # Trigger refresh via Power BI REST API
    pbi_base = "https://api.powerbi.com/v1.0/myorg"
    resp = client.post(
        f"{pbi_base}/groups/{workspace_id}/datasets/{sm_id}/refreshes",
        json={"notifyOption": "NoNotification"},
    )
    resp.raise_for_status()
    print("  Refresh triggered. Polling every 15 seconds...\n")

    # Poll until done
    for attempt in range(40):
        time.sleep(15)
        resp = client.get(
            f"{pbi_base}/groups/{workspace_id}/datasets/{sm_id}/refreshes?$top=1"
        )
        resp.raise_for_status()
        refreshes = resp.json().get("value", [])
        if not refreshes:
            print("    Waiting for refresh to appear in history...")
            continue
        latest   = refreshes[0]
        status   = latest.get("status", "Unknown")
        end_time = latest.get("endTime", "in progress")
        print(f"    [{attempt+1:02d}] status={status:<12}  endTime={end_time}")
        if status == "Completed":
            print("\n  Semantic model refreshed successfully.")
            return
        elif status == "Failed":
            error = latest.get("serviceExceptionJson", "(no details)")
            raise RuntimeError(f"Refresh failed.\nDetails: {error}")

    print("  WARNING: Refresh still running after 10 min. Check the workspace manually.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Patch, publish, and refresh TruckingSM semantic model in Fabric."
    )
    parser.add_argument(
        "--workspace-id",
        default=os.environ.get("FABRIC_WORKSPACE_ID"),
        help="Fabric workspace GUID (or set FABRIC_WORKSPACE_ID env var)",
    )
    parser.add_argument(
        "--lakehouse-id",
        default=os.environ.get("FABRIC_LAKEHOUSE_ID"),
        help="Lakehouse GUID (or set FABRIC_LAKEHOUSE_ID env var)",
    )
    parser.add_argument(
        "--sm-dir",
        default=os.environ.get("FABRIC_SM_DIR", "./builtin/TruckingSM.SemanticModel"),
        help="Path to the TruckingSM.SemanticModel folder (default: ./builtin/TruckingSM.SemanticModel)",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Skip the post-deploy refresh step",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if not args.workspace_id:
        print("ERROR: --workspace-id is required (or set FABRIC_WORKSPACE_ID).")
        sys.exit(1)
    if not args.lakehouse_id:
        print("ERROR: --lakehouse-id is required (or set FABRIC_LAKEHOUSE_ID).")
        sys.exit(1)

    sm_dir = Path(args.sm_dir)
    if not sm_dir.exists():
        print(f"ERROR: SM directory not found: {sm_dir}")
        print("Download the TruckingSM.SemanticModel files first (e.g. run the setup notebook).")
        sys.exit(1)

    print("=" * 60)
    print(f"  Deploying {SM_MODEL_NAME} Semantic Model")
    print(f"  Workspace : {args.workspace_id}")
    print(f"  Lakehouse : {args.lakehouse_id}")
    print(f"  SM dir    : {sm_dir.resolve()}")
    print("=" * 60)

    # Check whether the model already exists
    print(f"\nChecking if '{SM_MODEL_NAME}' already exists in workspace...")
    existing_id = find_semantic_model(args.workspace_id)

    if existing_id:
        print(f"  ℹ️  '{SM_MODEL_NAME}' already deployed ({existing_id}) — skipping publish.")
    else:
        print("\n[1/3] Checking fabric-cicd...")
        ensure_fabric_cicd()

        print("\n[2/3] Patching expressions.tmdl...")
        patch_expressions(sm_dir, args.workspace_id, args.lakehouse_id)

        print("\n[3/3] Publishing to workspace...")
        publish_model(args.workspace_id, sm_dir.parent)

    if not args.skip_refresh:
        print("\n[+] Triggering semantic model refresh...")
        trigger_refresh(args.workspace_id)

    print("\nDone.")


if __name__ == "__main__":
    main()
