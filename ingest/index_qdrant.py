import os, glob, uuid, argparse
from pathlib import Path
import pandas as pd
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer

BATCH_SIZE = int(os.getenv("QDRANT_INDEX_BATCH", "512"))

def row_to_text(row: pd.Series, chapter: str) -> str:
    parts = [f"chapter: {chapter}"]
    for k, v in row.items():
        v = "" if pd.isna(v) else str(v)
        if v: parts.append(f"{k}: {v}")
    return " | ".join(parts)

def iter_rows(csv_path: Path, chapter: str):
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    df = df.loc[:, ~df.columns.duplicated(keep="first")]
    for i, row in df.iterrows():
        yield i, row_to_text(row, chapter), row.to_dict()

def ensure_collection(client: QdrantClient, name: str, dim: int):
    client.recreate_collection(
        collection_name=name,
        vectors_config=models.VectorParams(size=dim, distance=models.Distance.COSINE),
    )

def main(snapshot: str, root: str, qdrant_url: str):
    base = Path(root) / "us" / "hts" / snapshot
    csv_dir = base / "csv"
    assert csv_dir.exists(), f"missing {csv_dir}"
    # Qdrant collections cannot contain ":" so keep naming predictable but safe
    collection = f"us_hts_{snapshot}"

    client = QdrantClient(url=qdrant_url, timeout=60.0)
    model = SentenceTransformer("intfloat/e5-base-v2")
    ensure_collection(client, collection, model.get_sentence_embedding_dimension())

    texts, ids, payloads = [], [], []
    for p in sorted(csv_dir.glob("ch_*.csv")):
        chapter = p.stem.split("_")[1]
        for i, text, payload in iter_rows(p, chapter):
            texts.append(f"passage: {text}")  # E5 passage prefix
            ids.append(str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{snapshot}:{chapter}:{i}")))
            payloads.append({
                "snapshot_id": snapshot,
                "chapter": chapter,
                "row_index": int(i),
                "source_csv": p.name,
                **payload
            })
            if len(texts) >= BATCH_SIZE:
                vecs = model.encode(texts, normalize_embeddings=True, batch_size=256, show_progress_bar=True)
                client.upsert(collection, models.Batch(ids=ids, vectors=vecs, payloads=payloads))
                texts, ids, payloads = [], [], []
    if texts:
        vecs = model.encode(texts, normalize_embeddings=True, batch_size=256, show_progress_bar=True)
        client.upsert(collection, models.Batch(ids=ids, vectors=vecs, payloads=payloads))
    print(f"indexed → {collection}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)        # e.g., US-HTS-2025-10-18
    ap.add_argument("--root", default=os.getenv("SNAPSHOT_ROOT","./snapshots"))
    ap.add_argument("--qdrant", default=os.getenv("QDRANT_URL","http://localhost:6333"))
    args = ap.parse_args()
    main(args.snapshot, args.root, args.qdrant)
