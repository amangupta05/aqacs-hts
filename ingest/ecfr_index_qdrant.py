import argparse
import json
from pathlib import Path

# from qdrant_client import QdrantClient, models
from qdrant_client import QdrantClient
from qdrant_client import models
from sentence_transformers import SentenceTransformer


def iter_manifest(manifest_path: Path):
    """Yield docs from manifest.jsonl one by one."""
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def create_collection_if_needed(client: QdrantClient, name: str, dim: int):
    """Create a Qdrant collection if it doesn't exist."""
    try:
        info = client.get_collection(name)
        print(
            f"Collection '{name}' already exists "
            f"(vectors count: {info.vectors_count})"
        )
        return
    except Exception:
        print(f"Collection '{name}' does not exist, creating...")

    client.create_collection(
        collection_name=name,
        vectors_config=models.VectorParams(
            size=dim,
            distance=models.Distance.COSINE,
        ),
    )
    print(f"Created collection '{name}' with dim={dim}, distance=COSINE")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True, help="Snapshot label: US-ECFR-YYYY-MM-DD")
    parser.add_argument("--root", required=True, help="Root ECFR snapshots dir")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--qdrant-url", default="http://localhost:6333", help="Qdrant endpoint, default http://localhost:6333")
    parser.add_argument("--collection", help=("Optional override for Qdrant collection name. Default: us_ecfr_<snapshot_label>"))
    args = parser.parse_args()

    snapshot_label = args.snapshot  # "US-ECFR-YYYY-MM-DD"
    date = args.date                # "YYYY-MM-DD" 
    root_dir = Path(args.root)
    snapshot_dir = root_dir / snapshot_label
    manifest_path = snapshot_dir / "manifest.jsonl"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")


    default_collection_name = f"us_ecfr_{snapshot_label}"
    collection_name = args.collection or default_collection_name

    print(f"Snapshot label:   {snapshot_label}")
    print(f"Snapshot date:    {date}")
    print(f"Manifest path:    {manifest_path}")
    print(f"Qdrant URL:       {args.qdrant_url}")
    print(f"Collection name:  {collection_name}")

    # 1) Load embedding model (using same embedding model used for HTS)
    print("Loading embedding model: intfloat/e5-base-v2 (cpu)...")
    model = SentenceTransformer("intfloat/e5-base-v2", device="cpu")
    dim = model.get_sentence_embedding_dimension()
    print(f"Embedding dimension: {dim}")

    # 2) Connect to Qdrant
    print(f"Connecting to Qdrant at {args.qdrant_url} ...")
    client = QdrantClient(url=args.qdrant_url)

    # 3) Ensuring collection exists
    create_collection_if_needed(client, collection_name, dim)

    # 4) Stream docs in batches and upsert
    batch_size = 128
    vectors = []
    payloads = []
    ids = []
    idx = 0

    print(f"Reading manifest and indexing into '{collection_name}'...")

    for doc in iter_manifest(manifest_path):
        text = doc.get("text") or ""
        citation = doc.get("citation") or ""
        heading = doc.get("heading") or ""

        embed_text = f"{citation} {heading}\n\n{text}".strip()

        vec = model.encode(embed_text, normalize_embeddings=True)

        vectors.append(vec)
        payloads.append(doc)
        ids.append(idx)
        idx += 1

        if len(vectors) >= batch_size:
            client.upsert(
                collection_name=collection_name,
                points=models.Batch(
                    ids=ids,
                    vectors=vectors,
                    payloads=payloads,
                ),
            )
            print(f"Upserted {len(ids)} points (total so far: {idx})")
            vectors, payloads, ids = [], [], []

    # Flush any remaining
    if vectors:
        client.upsert(
            collection_name=collection_name,
            points=models.Batch(
                ids=ids,
                vectors=vectors,
                payloads=payloads,
            ),
        )
        print(f"Upserted final {len(ids)} points (grand total: {idx})")

    print("Done indexing ECFR into Qdrant.")


if __name__ == "__main__":
    main()
