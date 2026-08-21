# kvtsjl

**kvtsjl** (*kv-tasjil*) — from Arabic *tasjīl* (تسجيل), “recording” / “registration”.

A typed, composable Python facade for key-value storage. Decouples domain schemas from persistence backends with orthogonal scope partitioning and layered wrappers (fallback, mirror, tiering). Features an abstract multi-indexing layer—attach custom exact, sparse, dense, or semantic vector indices per store for unified search and hydration.

> Status: repository initialized. Implementation coming soon.

## Install

```bash
pip install kvtsjl
pip install 'kvtsjl[pydantic]'   # kvtsjl.serde.pydantic
pip install 'kvtsjl[redis]'      # kvtsjl.backends.redis
pip install 'kvtsjl[all]'        # every optional integration
```

| Extra | Purpose |
|-------|---------|
| `pydantic` | `from kvtsjl.serde.pydantic import for_pydantic, …` |
| `redis` | `from kvtsjl.backends.redis import RedisKvStore` |
| `all` | All optional integrations (not test/lint tools) |
| `dev` | Tests, lint, type-check (+ `all`) |

More extras (e.g. `chroma`, `gcs`) will land as those integrations ship; add them to `all` when they do.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
