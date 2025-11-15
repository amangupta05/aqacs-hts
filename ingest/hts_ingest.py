import argparse
import os
import csv
import hashlib
import json
from pathlib import Path
import httpx

EXPORT = "https://hts.usitc.gov/reststop/exportList"

def fetch_csv(ch_from: str, ch_to: str) -> str:
    params = {"from": ch_from, "to": ch_to, "format": "CSV", "styles": "false"}
    with httpx.Client(timeout=30.0) as client:
        r = client.get(EXPORT, params=params)
        r.raise_for_status()
        return r.text

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def chapter_ranges():
    # 0101–0199, …, 9901–9999
    for ch in range(1, 100):
        yield f"{ch:02d}01", f"{ch:02d}99"

def main(snapshot_id: str, root: str):
    base = Path(root)/"us"/"hts"/snapshot_id
    csv_dir = base/"csv"; 
    csv_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for fr, to in chapter_ranges():
        txt = fetch_csv(fr, to)
        h = sha256(txt)
        out = csv_dir/f"ch_{fr[:2]}.csv"
        out.write_text(txt, encoding="utf-8")
        manifest.append({"path": str(out), "sha256": h, "from": fr, "to": to})
        print(f"saved {out} ({h[:8]})")
    (base/"manifest.jsonl").write_text("\n".join(json.dumps(m) for m in manifest), encoding="utf-8")
    print(f"snapshot complete: {snapshot_id}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True, help="US-HTS-YYYY-MM-DD")
    ap.add_argument("--root", default=os.getenv("S3_ROOT","./snapshots"))
    args = ap.parse_args()
    os.environ["SNAPSHOT_ID"] = args.snapshot
    main(args.snapshot, args.root)
