# Aletheia Python SDK

This package wraps the local Temporal Memory engine binary and the remote HTTP API with the same interface.

## Local-first flow

```python
from aletheia import AletheiaClient

client = AletheiaClient.from_local(auto_start=True)
client.ingest(entity_id="user-123", text="I prefer pourover coffee.")
hits = client.query("What coffee do I prefer?", entity_id="user-123")
```

## Cloud flow

```python
from aletheia import AletheiaClient

client = AletheiaClient.from_cloud(
    "https://memory.example.com",
    api_key="tm_live_xxx",
)
```

## Binary resolution

`from_local()` and `ensure_engine()` resolve the engine in this order:

1. Explicit `binary_path`
2. `ALETHEIA_ENGINE_BINARY` or `TEMPORAL_MEMORY_ENGINE_BINARY`
3. Repo-local `target/release/temporal_memory` or `target/debug/temporal_memory`
4. Cached binary in `ALETHEIA_ENGINE_CACHE_DIR`
5. Manifest download from `ALETHEIA_ENGINE_MANIFEST_URL`

## Sidecar environment

The SDK starts the Rust engine with:

- `TEMPORAL_MEMORY_HOST`
- `TEMPORAL_MEMORY_PORT`
- `TEMPORAL_MEMORY_DATA_DIR`
- `TEMPORAL_MEMORY_API_KEY` when a local key is configured

