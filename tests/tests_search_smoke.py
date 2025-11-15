import os
import pytest
from qdrant_client import QdrantClient

def test_collection_exists():
    snap = os.environ.get("SNAPSHOT","US-HTS-2025-10-18")
    coll = f"us_hts_{snap}"
    qc = QdrantClient(url=os.getenv("QDRANT_URL","http://localhost:6333"))
    info = qc.get_collection(coll)
    assert info.status.value == "green"
