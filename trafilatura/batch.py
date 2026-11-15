"""
Batch crawling orchestration on top of single-URL fetching and extraction.

Bundles the cross-cutting concerns every crawling project re-implements:
concurrency (worker pool), rate limiting, retries, content deduplication,
incremental manifests and JSONL persistence.
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from .core import extract
from .downloads import fetch_url

LOGGER = logging.getLogger(__name__)

RESULTS_FILENAME = "crawl_results.jsonl"


@dataclass
class CrawlItem:
    "Outcome for a single URL."
    url: str
    status: str  # "ok" | "failed" | "skipped"
    text: Optional[str] = None
    attempts: int = 0
    reason: Optional[str] = None  # why skipped/failed, for diagnostics


@dataclass
class CrawlResult:
    "Aggregate outcome of a crawl() run."
    items: List[CrawlItem] = field(default_factory=list)

    @property
    def fetched(self) -> int:
        return sum(1 for i in self.items if i.status == "ok")

    @property
    def failed(self) -> int:
        return sum(1 for i in self.items if i.status == "failed")

    @property
    def skipped(self) -> int:
        return sum(1 for i in self.items if i.status == "skipped")

    @property
    def deduped(self) -> int:
        return sum(1 for i in self.items if i.reason == "duplicate")


class _RateLimiter:
    "Simple thread-safe limiter spacing requests by 1/rate seconds."

    def __init__(self, rate: Optional[float]) -> None:
        self.interval = 1.0 / rate if rate else 0.0
        self._lock = threading.Lock()
        self._next_slot = 0.0

    def acquire(self) -> None:
        if self.interval <= 0.0:
            return
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot)
            self._next_slot = slot + self.interval
        wait = slot - now
        if wait > 0.0:
            time.sleep(wait)


def _extract_text(content: str) -> str:
    "Return extracted main text, falling back to the raw content."
    try:
        text = extract(content)
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("extraction failed, using raw content: %s", exc)
        text = None
    return text if text else content


def _fingerprint(text: str) -> str:
    return sha256(" ".join(text.split()).encode("utf-8")).hexdigest()


def _load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"seen": {}}
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        LOGGER.warning("ignoring unreadable manifest %s: %s", path, exc)
        return {"seen": {}}
    data.setdefault("seen", {})
    return data


def _save_manifest(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1, sort_keys=True)
    tmp.replace(path)


def crawl(
    urls: Sequence[str],
    fetch: Optional[Callable[[str], Any]] = None,
    workers: int = 4,
    rate_limit: Optional[float] = 10.0,
    max_retries: int = 2,
    out_dir: Optional[str] = None,
    manifest: Optional[str] = None,
) -> CrawlResult:
    """Fetch and extract a batch of URLs with orchestration built in.

    Args:
        urls: URLs to process, results keep this order.
        fetch: callable url -> HTML string; defaults to trafilatura.fetch_url.
            Raising or returning None counts as a failed attempt.
        workers: thread pool size, must be >= 1.
        rate_limit: max fetch requests per second (None/0 disables).
        max_retries: extra attempts after the first one fails.
        out_dir: if set, write one JSONL line per fetched item there.
        manifest: if set, JSON file tracking already-seen URLs so
            re-runs skip them without fetching.

    Returns:
        CrawlResult with per-URL items and fetched/failed/skipped/deduped
        counters.
    """
    if not isinstance(workers, int) or workers <= 0:
        raise ValueError(f"workers must be a positive integer, got {workers!r}")
    if max_retries < 0:
        raise ValueError(f"max_retries must be >= 0, got {max_retries!r}")
    if rate_limit is not None and rate_limit < 0:
        raise ValueError(f"rate_limit must be >= 0, got {rate_limit!r}")

    fetch_fn = fetch if fetch is not None else fetch_url
    limiter = _RateLimiter(rate_limit)

    manifest_data: Optional[Dict[str, Any]] = None
    seen: Dict[str, Any] = {}
    if manifest is not None:
        manifest_data = _load_manifest(Path(manifest))
        seen = manifest_data["seen"]

    items: List[Optional[CrawlItem]] = []
    todo: List[tuple] = []  # (index, url) pairs needing a fetch
    for idx, url in enumerate(urls):
        if url in seen:
            items.append(CrawlItem(url=url, status="skipped", reason="manifest"))
        else:
            items.append(None)
            todo.append((idx, url))

    def _fetch_one(url: str) -> CrawlItem:
        attempts = 0
        for _ in range(max_retries + 1):
            attempts += 1
            limiter.acquire()
            try:
                content = fetch_fn(url)
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("fetch failed (%s) for %s", exc, url)
                continue
            if content:
                return CrawlItem(url=url, status="ok", attempts=attempts,
                                 text=_extract_text(content))
        return CrawlItem(url=url, status="failed", attempts=attempts,
                         reason="fetch error")

    if todo:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            fetched_items = list(pool.map(_fetch_one, (u for _, u in todo)))
        for (idx, _), item in zip(todo, fetched_items):
            items[idx] = item

    # content deduplication, first occurrence wins (input order)
    fingerprints = set()
    deduped = 0
    for item in items:
        if item is None or item.status != "ok" or item.text is None:
            continue
        fp = _fingerprint(item.text)
        if fp in fingerprints:
            item.status = "skipped"
            item.reason = "duplicate"
            deduped += 1
        else:
            fingerprints.add(fp)

    result = CrawlResult(items=[i for i in items if i is not None])

    if manifest_data is not None:
        for item in result.items:
            if item.status in ("ok", "skipped") and item.reason != "manifest":
                seen[item.url] = {"status": item.status}
        _save_manifest(Path(manifest), manifest_data)

    if out_dir is not None:
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        with (out_path / RESULTS_FILENAME).open("w", encoding="utf-8") as fh:
            for item in result.items:
                if item.status == "ok":
                    fh.write(json.dumps({
                        "url": item.url,
                        "status": item.status,
                        "text": item.text,
                        "attempts": item.attempts,
                    }, ensure_ascii=False) + "\n")

    LOGGER.info("crawl done: %d fetched, %d failed, %d skipped (%d deduped)",
                result.fetched, result.failed, result.skipped, result.deduped)
    return result
