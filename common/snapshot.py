import json, os
from pathlib import Path

ACTIVE = os.getenv("ACTIVE_VERSION_FILE","./snapshots/active_version.json")

def set_active(snapshot_id: str, path: str = ACTIVE):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps({"snapshot_id": snapshot_id}), encoding="utf-8")

def get_active(path: str = ACTIVE) -> str:
    j = json.loads(Path(path).read_text(encoding="utf-8"))
    return j["snapshot_id"]

def active_snapshot_id(default: str | None = None) -> str:
    """
    Resolve the currently active snapshot id.
    Prefer an explicit ACTIVE_VERSION_FILE, fall back to environment.
    """
    if Path(ACTIVE).exists():
        try:
            return get_active()
        except (OSError, json.JSONDecodeError, KeyError):
            pass
    if default is None:
        default = "US-HTS-YYYY-MM-DD"
    return os.getenv("SNAPSHOT_ID", default)
