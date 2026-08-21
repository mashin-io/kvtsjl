# kvtsjl

**kvtsjl** (*kv-tasjil*) — from Arabic *tasjīl* (تسجيل), “recording” / “registration”.

A typed, composable Python facade for key-value storage. Decouples domain schemas from persistence backends with orthogonal scope partitioning and layered wrappers (fallback, mirror, tiering). Features an abstract multi-indexing layer—attach custom exact, sparse, dense, or semantic vector indices per store for unified search and hydration.

## Install

```bash
pip install kvtsjl
pip install 'kvtsjl[pydantic]'   # kvtsjl.serde.pydantic
pip install 'kvtsjl[redis]'      # kvtsjl.backends.redis
pip install 'kvtsjl[s3]'         # kvtsjl.backends.s3 (AWS S3 / MinIO)
pip install 'kvtsjl[gcs]'         # kvtsjl.backends.gcs (Google Cloud Storage)
pip install 'kvtsjl[all]'        # every optional integration
```

| Extra | Purpose |
|-------|---------|
| `pydantic` | `from kvtsjl.serde.pydantic import for_pydantic, …` |
| `redis` | `from kvtsjl.backends.redis import RedisKvStore` |
| `s3` | `from kvtsjl.backends.s3 import S3KvStore` |
| `gcs` | `from kvtsjl.backends.gcs import GcsKvStore` |
| `all` | All optional integrations (not test/lint tools) |
| `dev` | Tests, lint, type-check (+ `all`) |

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
pytest --cov=kvtsjl --cov-report=term-missing
```

Optional markers use in-process simulators: `s3` → **moto**, `redis` → **fakeredis**, `gcs` → in-memory fake bucket (no Docker/cloud required).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
