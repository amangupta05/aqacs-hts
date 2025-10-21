import os
def active_snapshot_id() -> str:
    return os.getenv("SNAPSHOT_ID", "US-HTS-YYYY-MM-DD")
