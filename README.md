# kvtsjl

**kvtsjl** (*kv-tasjil*) — from Arabic *tasjīl* (تسجيل), “recording” / “registration”.

A typed, composable Python facade for key-value storage. Decouples domain schemas from persistence backends with orthogonal scope partitioning and layered wrappers (fallback, mirror, tiering). Features an abstract multi-indexing layer—attach custom exact, sparse, dense, or semantic vector indices per store for unified search and hydration.

> Status: repository initialized. Implementation coming soon.

## Install

```bash
pip install kvtsjl
```

Optional backend extras (e.g. `redis`, `chroma`, `gcs`) will be added as those integrations ship.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
