#!/usr/bin/env python3
"""批量采集编排验收探针（离线，fetch 注入桩函数）。

契约：
  from trafilatura.batch import crawl
  crawl(urls, fetch=callable, workers=int, rate_limit=float, max_retries=int,
        out_dir=str, manifest=str) -> CrawlResult
  CrawlResult: .fetched .failed .skipped .deduped .items
  item: .url .status("ok"/"failed"/"skipped") .text .attempts
"""

import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

FAILED = 0
PASSED = 0


def check(code, desc, fn):
    global FAILED, PASSED
    try:
        fn()
        print(f"[PASS] {code} {desc}")
        PASSED += 1
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] {code} {desc}")
        print(f"        {type(exc).__name__}: {exc}")
        FAILED += 1


def main():
    try:
        from trafilatura.batch import crawl  # noqa: F401
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] P0 导入 trafilatura.batch 失败：{type(exc).__name__}: {exc}")
        print("-" * 60)
        print("探针汇总：0/9 通过")
        return 1

    def make_fetch(payloads, delay=0.0):
        def fetch(url):
            if delay:
                time.sleep(delay)
            v = payloads.get(url)
            if isinstance(v, Exception):
                raise v
            return v
        return fetch

    def p1():
        urls = [f"https://x/{i}" for i in range(3)]
        with tempfile.TemporaryDirectory() as d:
            r = crawl(urls, fetch=make_fetch({u: f"<p>{u}</p>" for u in urls}),
                      workers=2, rate_limit=100, out_dir=d)
        assert r.fetched == 3, r.fetched
        assert all(i.status == "ok" for i in r.items)

    def p2():
        urls = ["https://x/1", "https://x/2", "https://x/3"]
        payload = {urls[0]: "<p>a</p>", urls[1]: RuntimeError("boom"), urls[2]: "<p>c</p>"}
        with tempfile.TemporaryDirectory() as d:
            r = crawl(urls, fetch=make_fetch(payload), workers=2, rate_limit=100, out_dir=d)
        assert r.fetched == 2 and r.failed == 1, (r.fetched, r.failed)

    def p3():
        state = {"n": 0}

        def flaky(url):
            state["n"] += 1
            if state["n"] < 3:
                raise RuntimeError("flaky")
            return "<p>ok</p>"

        with tempfile.TemporaryDirectory() as d:
            r = crawl(["https://x/1"], fetch=flaky, workers=1, rate_limit=100,
                      max_retries=2, out_dir=d)
        assert r.items[0].status == "ok", r.items[0].status
        assert r.items[0].attempts == 3, r.items[0].attempts

    def p4():
        def always(url):
            raise RuntimeError("dead")

        with tempfile.TemporaryDirectory() as d:
            r = crawl(["https://x/1"], fetch=always, workers=1, rate_limit=100,
                      max_retries=2, out_dir=d)
        assert r.items[0].status == "failed"
        assert r.items[0].attempts == 3, r.items[0].attempts

    def p5():
        urls = ["https://x/1", "https://x/2"]
        same = "<p>相同正文</p>"
        with tempfile.TemporaryDirectory() as d:
            r = crawl(urls, fetch=make_fetch({u: same for u in urls}),
                      workers=1, rate_limit=100, out_dir=d)
        assert r.deduped == 1, r.deduped
        assert any(i.status == "skipped" for i in r.items)

    def p6():
        urls = ["https://x/1"]
        with tempfile.TemporaryDirectory() as d:
            man = str(Path(d) / "manifest.json")
            f = make_fetch({u: "<p>正文</p>" for u in urls})
            crawl(urls, fetch=f, workers=1, rate_limit=100, out_dir=d, manifest=man)
            r2 = crawl(urls, fetch=f, workers=1, rate_limit=100, out_dir=d, manifest=man)
        assert r2.skipped >= 1, r2.skipped
        assert r2.items[0].status == "skipped"

    def p7():
        urls = [f"https://x/{i}" for i in range(6)]
        f = make_fetch({u: f"<p>{u}</p>" for u in urls}, delay=0.12)
        with tempfile.TemporaryDirectory() as d:
            t0 = time.time()
            crawl(urls, fetch=f, workers=1, rate_limit=100, out_dir=d)
            t1 = time.time()
            crawl(urls, fetch=f, workers=4, rate_limit=100, out_dir=d)
            t2 = time.time()
        assert (t2 - t1) < (t1 - t0) * 0.75, (t1 - t0, t2 - t1)

    def p8():
        urls = [f"https://x/{i}" for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            r = crawl(urls, fetch=make_fetch({u: f"<p>{u}</p>" for u in urls}),
                      workers=2, rate_limit=100, out_dir=d)
            files = list(Path(d).glob("*.jsonl"))
            assert files, "没有落盘 jsonl"
            lines = sum(1 for _ in files[0].open(encoding="utf-8"))
            assert lines == r.fetched, (lines, r.fetched)

    def p9():
        try:
            with tempfile.TemporaryDirectory() as d:
                crawl(["https://x/1"], fetch=lambda u: "<p>x</p>", workers=0,
                      rate_limit=100, out_dir=d)
        except (ValueError, TypeError):
            return
        raise AssertionError("workers=0 未报错")

    check("P1", "正常批量抓取", p1)
    check("P2", "单条失败不中断", p2)
    check("P3", "失败后重试成功", p3)
    check("P4", "重试用尽记为失败", p4)
    check("P5", "相同正文去重", p5)
    check("P6", "增量跳过已抓 URL", p6)
    check("P7", "并发确实更快", p7)
    check("P8", "结果落盘 jsonl", p8)
    check("P9", "非法 workers 报错", p9)

    print("-" * 60)
    print(f"探针汇总：{PASSED}/{PASSED + FAILED} 通过")
    return 0 if FAILED == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
