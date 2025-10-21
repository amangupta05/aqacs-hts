import csv, os, glob
from pathlib import Path
from rapidfuzz import fuzz, process

SNAP_ROOT = os.getenv("S3_ROOT","./snapshots")

def _norm(s: str) -> str:
    return (s or "").strip().lower().replace(" ", "").replace("/", "").replace("-", "")

HEADER_MAP = {
    "headingsubheading": "hs",
    "statsuffix": "stat",
    "articledescription": "article",
    "unitofquantity": "uoq",
    "generalrateofduty": "gen",
    "specialrateofduty": "spec",
    "column2rateofduty": "col2",
}

def _read_csv(fp):
    with open(fp, newline='', encoding="utf-8-sig") as f:
        rdr = csv.reader(f)
        headers = next(rdr)
        keys = [HEADER_MAP.get(_norm(h), _norm(h)) for h in headers]
        for row in rdr:
            yield dict(zip(keys, row))

def _mk_rec(row):
    hs = (row.get("hs") or "")
    stat = (row.get("stat") or "").strip()
    code10 = (hs + stat).replace(" ", "")
    chap = int(hs[:2]) if hs[:2].isdigit() else 0
    return {
        "hts10": code10,
        "chapter": chap,
        "heading6": hs[:7],
        "stat_suffix": stat,
        "article": row.get("article"),
        "uoq": row.get("uoq"),
        "rate_general": row.get("gen"),
        "rate_special": row.get("spec"),
        "rate_col2": row.get("col2"),
    }

class Store:
    def __init__(self):
        self._rows, self._index = [], {}
        self._load_latest()

    def _load_latest(self):
        snap = os.getenv("SNAPSHOT_ID","US-HTS-YYYY-MM-DD")
        base = Path(SNAP_ROOT)/"us"/"hts"/snap/"csv"
        base.mkdir(parents=True, exist_ok=True)
        for fp in sorted(glob.glob(str(base/"*.csv"))):
            for row in _read_csv(fp):
                rec = _mk_rec(row)
                self._rows.append(rec)
                if rec["hts10"]:
                    self._index[rec["hts10"]] = rec

    def get_by_code(self, code: str):
        return self._index.get(code.replace(" ", ""))

    def search_article(self, q: str, limit: int = 10):
        choices = [(r["article"] or "", i) for i, r in enumerate(self._rows)]
        top = process.extract(q, choices, scorer=fuzz.WRatio, limit=limit)
        return [ self._rows[i] for (_label, score, (text, i)) in top ]
