from functools import lru_cache
from typing import Any, Iterable

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from common.snapshot import active_snapshot_id
from common.sections import chapter_to_section
from common.store import Store
import os
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer
from transformers import pipeline

router = APIRouter()
store = Store()

# ---------- New semantic search setup ----------
_qdrant = QdrantClient(url=os.getenv("QDRANT_URL", "http://localhost:6333"))
_encoder = SentenceTransformer("intfloat/e5-base-v2")

# ------------------------------------------------

QA_MODEL_ID = os.getenv("QA_MODEL_ID", "deepset/roberta-base-squad2")
MAX_CONTEXTS = 5
MAX_CONTEXT_CHARS = 1200
MIN_QA_SCORE = float(os.getenv("QA_MIN_SCORE", "0.2"))


@lru_cache()
def get_qa_pipeline():
    """Lazily instantiate the local QA pipeline so startup stays fast."""
    return pipeline("question-answering", model=QA_MODEL_ID)


def _normalize_val(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, Iterable) and not isinstance(val, (str, bytes)):
        return ", ".join(_normalize_val(v) for v in val if _normalize_val(v))
    return str(val).strip()


def _payload_context(payload: dict) -> str:
    preferred_keys = [
        "Description",
        "Article Description",
        "article",
        "HTS Number",
        "htsno",
        "hts10",
        "Indent",
        "Unit of Quantity",
        "General Rate of Duty",
        "Special Rate of Duty",
        "Column 2 Rate of Duty",
        "Additional Duties",
        "Quota Quantity",
    ]
    seen = set()
    parts: list[str] = []
    for key in preferred_keys:
        if key in payload and key not in seen:
            val = _normalize_val(payload.get(key))
            if val:
                parts.append(f"{key}: {val}")
            seen.add(key)
    for key, val in payload.items():
        if key in seen:
            continue
        norm = _normalize_val(val)
        if norm:
            parts.append(f"{key}: {norm}")
            seen.add(key)
        if len(parts) > MAX_CONTEXT_CHARS // 20:  # avoid runaway contexts
            break
    context = " | ".join(parts)
    if len(context) > MAX_CONTEXT_CHARS:
        context = context[:MAX_CONTEXT_CHARS].rsplit(" ", 1)[0]
    return context


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


class QARequest(BaseModel):
    question: str
    limit: int = 8


@router.post("/qa")
def qa(req: QARequest):
    """Retrieve augmented generation answer backed by HTS snapshot content."""
    if not req.question.strip():
        raise HTTPException(status_code=400, detail="question cannot be empty")

    snapshot = active_snapshot_id()
    collection = f"us_hts_{snapshot}"
    query_vec = _encoder.encode([f"query: {req.question}"], normalize_embeddings=True)[0]

    limit = max(1, min(req.limit, 12))
    try:
        points = _qdrant.search(
            collection_name=collection,
            query_vector=query_vec,
            limit=limit,
            with_payload=True,
        )
    except Exception as exc:  # pragma: no cover - surface upstream client errors
        raise HTTPException(status_code=503, detail=f"qdrant error: {exc}") from exc

    if not points:
        raise HTTPException(status_code=404, detail="no relevant context found")

    contexts: list[tuple[str, Any]] = []
    sources = []
    for point in points:
        payload = point.payload or {}
        context = _payload_context(payload)
        if context:
            contexts.append((context, payload))
        sources.append(
            {
                "id": str(point.id),
                "score": float(point.score or 0.0),
                "chapter": payload.get("chapter"),
                "code": payload.get("HTS Number")
                or payload.get("htsno")
                or payload.get("hts10"),
                "description": payload.get("Description")
                or payload.get("article")
                or payload.get("Article Description"),
                "source_csv": payload.get("source_csv"),
                "row_index": payload.get("row_index"),
            }
        )

    qa_pipe = get_qa_pipeline()
    answers = []
    for context, payload in contexts[:MAX_CONTEXTS]:
        result = qa_pipe({"question": req.question, "context": context})
        if result and result.get("answer"):
            answers.append(
                {
                    "answer": result["answer"].strip(),
                    "score": float(result.get("score") or 0.0),
                    "context": context,
                    "payload": payload,
                }
            )

    if not answers:
        return {
            "snapshot_id": snapshot,
            "question": req.question,
            "answer": "No confident answer found; review the supporting sources.",
            "confidence": 0.0,
            "sources": sources,
        }

    best = max(answers, key=lambda a: a["score"])
    confidence = best["score"]
    answer_text = best["answer"] or "No confident answer found; review the supporting sources."
    if confidence < MIN_QA_SCORE:
        answer_text = "No confident answer found; review the supporting sources."
        confidence = 0.0

    snippet = best["context"][:280]
    if len(best["context"]) > 280:
        snippet = snippet.rsplit(" ", 1)[0] + "..."

    return {
        "snapshot_id": snapshot,
        "question": req.question,
        "answer": answer_text,
        "confidence": confidence,
        "supporting_excerpt": snippet,
        "sources": sources,
    }
