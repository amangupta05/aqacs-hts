from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from common.snapshot import active_snapshot_id
from common.sections import chapter_to_section
from common.store import Store

router = APIRouter()
store = Store()


class TariffReq(BaseModel):
    code: str

@router.post("/tariff")
def tariff(req: TariffReq):
    r = store.get_by_code(req.code)
    if not r:
        raise HTTPException(status_code=404, detail="code not found")
    chap = r["chapter"]
    return {
        "disclaimer": "DEV ONLY — non-legal; PDFs required for prod",
        "snapshot_id": active_snapshot_id(),
        "code": r["hts10"],
        "chapter": chap,
        "section": chapter_to_section(chap),
        "rates": {
            "general": r.get("rate_general"),
            "special": r.get("rate_special"),
            "col2": r.get("rate_col2"),
        },
        "dev_citation": f"HTSUS §{chapter_to_section(chap)}, Ch.{chap}, {r['hts10']}",
    }

@router.get("/search")
def search(q: str = Query(..., min_length=2), limit: int = 10):
    rows = store.search_article(q, limit=limit)
    return {
        "disclaimer": "DEV ONLY — non-legal; PDFs required for prod",
        "snapshot_id": active_snapshot_id(),
        "items": [
            {
                "code": r.get("hts10"),
                "chapter": r.get("chapter"),
                "section": chapter_to_section(r.get("chapter")),
                "article": r.get("article"),
                "uoq": r.get("uoq"),
                "rates": {
                    "general": r.get("rate_general"),
                    "special": r.get("rate_special"),
                    "col2": r.get("rate_col2"),
                },
                "dev_citation": f"HTSUS §{chapter_to_section(r.get('chapter'))}, Ch.{r.get('chapter')}, {r.get('hts10')}",
            } for r in rows
        ],
    }

