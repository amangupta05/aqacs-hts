.PHONY: up down api test fmt ingest
up: ; docker compose up -d --build
down: ; docker compose down -v
api: ; uvicorn api.main:app --reload --host 0.0.0.0 --port 8080
ingest: ; python ingest/hts_ingest.py --snapshot $$(date +US-HTS-%F)
fmt: ; ruff check --fix . && ruff format .
test: ; pytest -q
qdrant-up:
\tdocker compose up -d qdrant

index:
\tpython -m ingest.index_qdrant --snapshot $(SNAPSHOT)

promote:
\tpython -c "from common.snapshot_active import set_active; set_active('$(SNAPSHOT)')"
