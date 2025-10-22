from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from common.snapshot import active_snapshot_id
from common.sections import chapter_to_section
from common.store import Store
import os
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

router = APIRouter()
store = Store()

# ---------- New semantic search setup ----------
_qdrant = QdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
_encoder = SentenceTransformer("intfloat/e5-base-v2")

# ------------------------------------------------

class TariffReq(BaseModel):
    code: str


@router.post("/tariff")
def tariff(req: TariffReq):
    """Return tariff info by 10-digit HTS code."""
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


# ---------- Legacy keyword search ----------
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
                "dev_citation": f"HTSUS §{chapter_to_section(r.get('chapter'))}, "
                                f"Ch.{r.get('chapter')}, {r.get('hts10')}",
            }
            for r in rows
        ],
    }


# ---------- New semantic / vector search ----------
class SemanticHit(BaseModel):
    id: str
    score: float
    payload: dict


@router.get("/semantic_search")
def semantic_search(q: str = Query(..., min_length=2), limit: int = 8):
    """Vector search against indexed HTS snapshot in Qdrant."""
    snapshot = active_snapshot_id()
    collection = f"us_hts_{snapshot}"

    vec = _encoder.encode([f"query: {q}"], normalize_embeddings=True)[0]
    results = _qdrant.search(
        collection_name=collection,
        query_vector=vec,
        limit=limit,
        with_payload=True,
    )

    return {
        "snapshot_id": snapshot,
        "items": [
            {
                "id": str(p.id),
                "score": float(p.score),
                "chapter": p.payload.get("chapter"),
                "article": p.payload.get("article"),
                "code": p.payload.get("htsno") or p.payload.get("hts10"),
                "rates": {
                    "general": p.payload.get("rate_general"),
                    "special": p.payload.get("rate_special"),
                    "col2": p.payload.get("rate_col2"),
                },
                "source_csv": p.payload.get("source_csv"),
            }
            for p in results
        ],
    }
