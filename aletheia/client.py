from __future__ import annotations

import atexit
import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

ENGINE_BINARY_NAME = "temporal_memory.exe" if os.name == "nt" else "temporal_memory"
DEFAULT_TIMEOUT = 30.0
DEFAULT_STARTUP_TIMEOUT = 60.0
DEFAULT_TEST_API_KEY = "XXX1111AAA"


class AletheiaError(RuntimeError):
    pass


class AletheiaHTTPError(AletheiaError):
    def __init__(self, status_code: int, body: str, url: str):
        super().__init__(f"Engine request failed with HTTP {status_code} for {url}: {body}")
        self.status_code = status_code
        self.body = body
        self.url = url


@dataclass(slots=True)
class IngestItem:
    entity_id: str
    text: str
    memory_id: str | None = None
    timestamp: int | None = None
    relations: Sequence[tuple[str, str]] | None = None
    kind: str | None = None
    enable_semantic_dedup: bool | None = None
    enable_consolidation: bool | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "memory_id": self.memory_id or f"{self.entity_id}::sdk::{uuid.uuid4().hex}",
            "timestamp": self.timestamp or int(time.time() * 1000),
            "textual_content": self.text,
            "relations": list(self.relations or ()),
            "kind": self.kind,
            "enable_semantic_dedup": self.enable_semantic_dedup,
            "enable_consolidation": self.enable_consolidation,
        }


@dataclass(slots=True)
class QueryHit:
    memory_id: str
    entity_id: str
    session_id: str
    turn_index: int
    created_at_ms: int
    similarity: float
    textual_content: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "QueryHit":
        return cls(
            memory_id=str(payload.get("memory_id", "")),
            entity_id=str(payload.get("entity_id", "")),
            session_id=str(payload.get("session_id", "")),
            turn_index=int(payload.get("turn_index", 0)),
            created_at_ms=int(payload.get("created_at_ms", 0)),
            similarity=float(payload.get("similarity", 0.0)),
            textual_content=str(payload.get("textual_content", "")),
        )


@dataclass(slots=True)
class EngineVersion:
    engine_version: str
    api_version: str
    auth_required: bool

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "EngineVersion":
        return cls(
            engine_version=str(payload.get("engine_version", "")),
            api_version=str(payload.get("api_version", "")),
            auth_required=bool(payload.get("auth_required", False)),
        )


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _default_cache_dir() -> Path:
    if os.name == "nt":
        return Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "aletheia"
    xdg = os.getenv("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "aletheia"


def _default_api_key() -> str:
    return (
        _env_first("ALETHEIA_API_KEY", "TEMPORAL_MEMORY_API_KEY")
        or DEFAULT_TEST_API_KEY
    )


def _platform_tag() -> str:
    os_name = platform.system().lower()
    if os_name.startswith("darwin"):
        os_part = "darwin"
    elif os_name.startswith("linux"):
        os_part = "linux"
    elif os_name.startswith("windows"):
        os_part = "windows"
    else:
        raise AletheiaError(f"Unsupported operating system: {platform.system()}")

    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        arch = "amd64"
    elif machine in {"arm64", "aarch64"}:
        arch = "arm64"
    else:
        raise AletheiaError(f"Unsupported architecture: {platform.machine()}")

    return f"{os_part}-{arch}"


def _is_remote_source(source: str) -> bool:
    parsed = urlparse(source)
    return parsed.scheme in {"http", "https"}


def _load_json_source(source: str) -> dict[str, Any]:
    if _is_remote_source(source):
        with urlopen(source, timeout=DEFAULT_TIMEOUT) as response:
            return json.loads(response.read().decode("utf-8"))
    return json.loads(Path(source).read_text(encoding="utf-8"))


def _read_bytes_source(source: str) -> bytes:
    if _is_remote_source(source):
        with urlopen(source, timeout=DEFAULT_TIMEOUT) as response:
            return response.read()
    return Path(source).read_bytes()


def _ensure_executable(path: Path) -> None:
    if os.name == "nt":
        return
    current = path.stat().st_mode
    path.chmod(current | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@dataclass
class LocalEngineManager:
    host: str = "127.0.0.1"
    port: int = 3000
    api_key: str | None = None
    binary_path: str | None = None
    cache_dir: Path | None = None
    data_dir: Path | None = None
    manifest_url: str | None = None
    version: str | None = None
    process: subprocess.Popen[Any] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = _default_api_key()

        cache_dir_value = self.cache_dir or Path(
            _env_first("ALETHEIA_ENGINE_CACHE_DIR", "TEMPORAL_MEMORY_ENGINE_CACHE_DIR")
            or _default_cache_dir()
        )
        self.cache_dir = Path(cache_dir_value).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if self.data_dir is None:
            self.data_dir = self.cache_dir / "data" / f"{self.host}-{self.port}"
        self.data_dir = Path(self.data_dir).expanduser()
        self.data_dir.mkdir(parents=True, exist_ok=True)

        if self.manifest_url is None:
            self.manifest_url = _env_first(
                "ALETHEIA_ENGINE_MANIFEST_URL",
                "ALETHEIA_ENGINE_MANIFEST",
            )

        if self.version is None:
            self.version = _env_first(
                "ALETHEIA_ENGINE_VERSION",
                "TEMPORAL_MEMORY_ENGINE_VERSION",
            )

        atexit.register(self.stop)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def ensure_engine(self, version: str | None = None) -> Path:
        requested_version = version or self.version

        explicit = self.binary_path or _env_first(
            "ALETHEIA_ENGINE_BINARY",
            "TEMPORAL_MEMORY_ENGINE_BINARY",
        )
        if explicit:
            binary = Path(explicit).expanduser()
            if not binary.is_file():
                raise AletheiaError(f"Configured engine binary was not found: {binary}")
            _ensure_executable(binary)
            return binary

        repo_binary = self._find_repo_binary()
        if repo_binary is not None:
            return repo_binary

        cached = self._cached_binary_path(requested_version)
        if cached.is_file():
            _ensure_executable(cached)
            return cached

        if self.manifest_url:
            return self._download_from_manifest(self.manifest_url, requested_version)

        raise AletheiaError(
            "No engine binary found. Set ALETHEIA_ENGINE_BINARY or build target/release/temporal_memory."
        )

    def start(
        self,
        version: str | None = None,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
    ) -> None:
        if self.process is not None and self.process.poll() is None:
            self.wait_until_ready(startup_timeout)
            return

        try:
            self.health()
            return
        except AletheiaError:
            pass

        binary = self.ensure_engine(version)
        env = os.environ.copy()
        env["TEMPORAL_MEMORY_HOST"] = self.host
        env["TEMPORAL_MEMORY_PORT"] = str(self.port)
        env["TEMPORAL_MEMORY_DATA_DIR"] = str(self.data_dir)
        if self.api_key:
            env["TEMPORAL_MEMORY_API_KEY"] = self.api_key

        self.process = subprocess.Popen(  # noqa: S603
            [str(binary)],
            cwd=str(binary.parent),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.wait_until_ready(startup_timeout)
        except Exception:
            self.stop()
            raise

    def stop(self) -> None:
        if self.process is None:
            return

        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.process = None

    def wait_until_ready(self, timeout: float = DEFAULT_STARTUP_TIMEOUT) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise AletheiaError("Local engine exited before becoming healthy.")
            try:
                self.health()
                return
            except AletheiaError:
                time.sleep(0.25)
        raise AletheiaError("Timed out waiting for the local engine to become healthy.")

    def health(self) -> dict[str, Any]:
        headers = self._auth_headers()
        request = Request(urljoin(self.base_url, "/v1/health"), headers=headers, method="GET")
        try:
            with urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 404:
                fallback = Request(
                    urljoin(self.base_url, "/health"),
                    headers=headers,
                    method="GET",
                )
                with urlopen(fallback, timeout=5) as response:
                    return json.loads(response.read().decode("utf-8"))
            raise AletheiaHTTPError(exc.code, exc.read().decode("utf-8"), request.full_url) from exc
        except URLError as exc:
            raise AletheiaError(f"Local engine is not reachable at {self.base_url}") from exc

    def _auth_headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {
            "x-api-key": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
        }

    def _cached_binary_path(self, version: str | None) -> Path:
        version_dir = version or "current"
        return self.cache_dir / "bin" / version_dir / _platform_tag() / ENGINE_BINARY_NAME

    def _find_repo_binary(self) -> Path | None:
        for parent in Path(__file__).resolve().parents:
            for relative in (
                Path("target") / "release" / ENGINE_BINARY_NAME,
                Path("target") / "debug" / ENGINE_BINARY_NAME,
            ):
                candidate = parent / relative
                if candidate.is_file():
                    _ensure_executable(candidate)
                    return candidate
        discovered = shutil.which("temporal_memory")
        if discovered:
            return Path(discovered)
        return None

    def _download_from_manifest(self, manifest_source: str, version: str | None) -> Path:
        manifest = _load_json_source(manifest_source)
        artifacts = manifest.get("artifacts") or manifest.get("binaries") or {}
        artifact = artifacts.get(_platform_tag())
        if artifact is None:
            raise AletheiaError(
                f"Manifest did not contain a binary for {_platform_tag()}."
            )

        manifest_version = version or manifest.get("version") or "current"
        output_path = self._cached_binary_path(manifest_version)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        binary_source = artifact.get("url")
        if not binary_source:
            raise AletheiaError("Manifest artifact is missing a url.")

        payload = _read_bytes_source(binary_source)
        expected_sha = artifact.get("sha256")
        if expected_sha:
            actual_sha = hashlib.sha256(payload).hexdigest()
            if actual_sha.lower() != str(expected_sha).lower():
                raise AletheiaError(
                    f"Downloaded binary checksum mismatch: expected {expected_sha}, got {actual_sha}."
                )

        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        tmp_path.write_bytes(payload)
        _ensure_executable(tmp_path)
        tmp_path.replace(output_path)
        return output_path


class AletheiaClient:
    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        engine_manager: LocalEngineManager | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or _default_api_key()
        self.timeout = timeout
        self._engine_manager = engine_manager

    @classmethod
    def from_cloud(
        cls,
        base_url: str,
        api_key: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> "AletheiaClient":
        return cls(base_url, api_key=api_key, timeout=timeout)

    @classmethod
    def from_local(
        cls,
        *,
        host: str = "127.0.0.1",
        port: int = 3000,
        api_key: str | None = None,
        binary_path: str | None = None,
        cache_dir: str | Path | None = None,
        data_dir: str | Path | None = None,
        manifest_url: str | None = None,
        version: str | None = None,
        auto_start: bool = True,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> "AletheiaClient":
        manager = LocalEngineManager(
            host=host,
            port=port,
            api_key=api_key,
            binary_path=binary_path,
            cache_dir=Path(cache_dir).expanduser() if cache_dir else None,
            data_dir=Path(data_dir).expanduser() if data_dir else None,
            manifest_url=manifest_url,
            version=version,
        )
        client = cls(manager.base_url, api_key=api_key, timeout=timeout, engine_manager=manager)
        if auto_start:
            manager.start(version=version)
        return client

    def ensure_engine(self, version: str | None = None) -> Path:
        if self._engine_manager is None:
            raise AletheiaError("ensure_engine() is available only for local clients.")
        return self._engine_manager.ensure_engine(version)

    def start_local_engine(
        self,
        *,
        version: str | None = None,
        startup_timeout: float = DEFAULT_STARTUP_TIMEOUT,
    ) -> None:
        if self._engine_manager is None:
            raise AletheiaError("start_local_engine() is available only for local clients.")
        self._engine_manager.start(version=version, startup_timeout=startup_timeout)

    def stop_local_engine(self) -> None:
        if self._engine_manager is None:
            return
        self._engine_manager.stop()

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/v1/health", fallback_path="/health")

    def engine_version(self) -> EngineVersion:
        payload = self._request_json("GET", "/v1/version", fallback_path="/version")
        return EngineVersion.from_dict(payload)

    def ingest(
        self,
        *,
        entity_id: str,
        text: str,
        memory_id: str | None = None,
        timestamp: int | None = None,
        relations: Sequence[tuple[str, str]] | None = None,
        kind: str | None = None,
        enable_semantic_dedup: bool | None = None,
        enable_consolidation: bool | None = None,
    ) -> None:
        item = IngestItem(
            entity_id=entity_id,
            text=text,
            memory_id=memory_id,
            timestamp=timestamp,
            relations=relations,
            kind=kind,
            enable_semantic_dedup=enable_semantic_dedup,
            enable_consolidation=enable_consolidation,
        )
        self._request("POST", "/ingest", payload=item.to_payload())

    def ingest_many(
        self,
        items: Iterable[IngestItem],
        *,
        batch_size: int = 32,
    ) -> None:
        if batch_size <= 0:
            raise AletheiaError("batch_size must be greater than zero.")

        chunk: list[dict[str, Any]] = []
        for item in items:
            chunk.append(item.to_payload())
            if len(chunk) >= batch_size:
                self._request("POST", "/ingest/batch", payload={"items": chunk})
                chunk = []

        if chunk:
            self._request("POST", "/ingest/batch", payload={"items": chunk})

    def query(
        self,
        query: str,
        *,
        entity_id: str | None = None,
        top_k: int = 5,
        rerank: bool = True,
    ) -> list[QueryHit]:
        payload = {
            "textual_query": query,
            "limit": top_k,
            "entity_id": entity_id,
            "enable_neural_rerank": rerank,
        }
        response = self._request_json("POST", "/query/semantic", payload=payload)
        return [QueryHit.from_dict(item) for item in response]

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        fallback_path: str | None = None,
    ) -> Any:
        try:
            return self._request(method, path, payload=payload)
        except AletheiaHTTPError as exc:
            if fallback_path and exc.status_code == 404:
                return self._request(method, fallback_path, payload=payload)
            raise

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self._ensure_local_engine()

        body = None
        headers: dict[str, str] = {
            "Accept": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.api_key:
            headers["x-api-key"] = self.api_key
            headers["Authorization"] = f"Bearer {self.api_key}"

        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        request = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            raise AletheiaHTTPError(exc.code, exc.read().decode("utf-8"), url) from exc
        except URLError as exc:
            raise AletheiaError(f"Engine is not reachable at {self.base_url}") from exc

        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))

    def _ensure_local_engine(self) -> None:
        if self._engine_manager is None:
            return
        if self._engine_manager.process is not None and self._engine_manager.process.poll() is None:
            return
        try:
            self._engine_manager.health()
            return
        except AletheiaError:
            self._engine_manager.start()


Client = AletheiaClient
