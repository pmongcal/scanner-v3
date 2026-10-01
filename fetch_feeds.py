#!/usr/bin/env python3
"""
Intelligence Scanner — RSS Aggregator (GitHub Actions edition)

Pulls every feed in feed_registry.json, applies conservative negative-only
filtering, deduplicates against a persistent seen_ids store, and writes
articles.json.
Also appends a timestamped snapshot to articles_archive/ for history.

Designed to run inside GitHub Actions where internet egress is unrestricted.
All HTTP fetches use a realistic browser UA and per-feed timeouts, and any
individual feed failure is logged but does not abort the run.

Output schema (articles.json):
{
  "generated_at": "<iso utc>",
  "feed_status": {<feed_name>: {"ok": bool, "count": int, "error": str|null, "fetched_at": iso}},
  "articles": [
    {
      "id": "<16-char sha>",
      "title": str,
      "url": str,
      "summary": str,
      "published": str,
      "source": str,
      "category": str,
      "language": str,
      "priority": int,
      "filter_status": "PASS"|"SKIP",
      "filter_reason": str
    },
    ...
  ]
}
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import feedparser
import requests

# ---- paths ---------------------------------------------------------------

ROOT = Path(__file__).resolve().parent
REGISTRY_PATH = ROOT / "feed_registry.json"
OUTPUT_PATH = ROOT / "articles.json"
ARCHIVE_DIR = ROOT / "articles_archive"
SEEN_IDS_PATH = ROOT / "seen_ids.json"
PENDING_PATH = ROOT / "pending_digest.json"  # new articles waiting for the next email
AUDIT_DIR = ROOT / "audit"                   # human-readable audit trail, committed to the repo

# ---- config --------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/130.0.0.0 Safari/537.36"
)
PER_FEED_TIMEOUT_S = 20
PARALLEL_WORKERS = 8
RETENTION_HOURS_DEFAULT = 96
SEEN_ID_CAP = 5000  # keep last N ids per feed to prevent unbounded growth
# Max items read from one feed per run. Was a hard 40, which silently dropped
# the tail of busy feeds such as Google News topics. A feed can override this
# with "max_items" in feed_registry.json. Anything over the cap is logged.
MAX_ITEMS_PER_FEED_DEFAULT = 200
PENDING_RUNS_CAP = 50  # keep at most N run records in the pending digest


# ---- helpers -------------------------------------------------------------

def load_registry() -> dict[str, Any]:
    with REGISTRY_PATH.open() as f:
        return json.load(f)


def load_seen() -> dict[str, list[str]]:
    if SEEN_IDS_PATH.exists():
        try:
            with SEEN_IDS_PATH.open() as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_seen(seen: dict[str, list[str]]) -> None:
    # cap each feed's seen list to avoid unbounded growth
    for k, v in list(seen.items()):
        if len(v) > SEEN_ID_CAP:
            seen[k] = v[-SEEN_ID_CAP:]
    with SEEN_IDS_PATH.open("w") as f:
        json.dump(seen, f, indent=2, ensure_ascii=False)


def article_id(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}|{title}".encode()).hexdigest()[:16]


def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s or "").strip()


def _any_match(patterns: list[str], text: str) -> bool:
    """Return True if any regex (or literal fallback) pattern matches."""
    for kw in patterns:
        try:
            if re.search(kw, text, re.IGNORECASE):
                return True
        except re.error:
            if kw.lower() in text.lower():
                return True
    return False


def classify_article(title: str, summary: str, registry: dict) -> tuple[str, str]:
    """Return (PASS|SKIP, reason) using conservative negative-only rules.

    The 40 feeds are already curated toward AI/tech/marketing/business. The
    deterministic layer therefore does not try to prove relevance. It only
    removes article types we are confident are noise. Each rule is composed of
    one or more vocab groups; every group in a rule must match. If nothing
    matches, the article passes to Cowork for editorial analysis.

    Rules can inspect either the title alone or title + short RSS summary. This
    lets high-risk categories such as product reviews use a stricter title
    pattern while still satisfying the overall requirement to use both RSS
    fields where useful.
    """
    title_text = title or ""
    combined_text = f"{title or ''} {summary or ''}".strip()
    vocab = registry.get("negative_filter_vocab_en", {})
    rules = registry.get("negative_filter_rules_en", [])

    for rule in rules:
        field = rule.get("field", "text")
        haystack = title_text if field == "title" else combined_text
        groups = rule.get("groups", [])
        if not groups:
            continue
        matched = True
        for group_name in groups:
            patterns = vocab.get(group_name, [])
            if not patterns or not _any_match(patterns, haystack):
                matched = False
                break
        if matched:
            return "SKIP", rule.get("id", "negative_filter_match")

    return "PASS", "no_negative_rule_matched"


def load_pending() -> dict:
    if PENDING_PATH.exists():
        try:
            with PENDING_PATH.open() as f:
                data = json.load(f)
            if isinstance(data, dict):
                data.setdefault("articles", [])
                data.setdefault("runs", [])
                return data
        except Exception:
            pass
    return {"articles": [], "runs": []}


def save_pending(pending: dict) -> None:
    pending["runs"] = pending.get("runs", [])[-PENDING_RUNS_CAP:]
    with PENDING_PATH.open("w") as f:
        json.dump(pending, f, indent=2, ensure_ascii=False)


def _append_csv(path: Path, header: list[str], rows: list[list]) -> None:
    """Append rows to a CSV, writing the header if the file is new."""
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        w.writerows(rows)


def write_audit(now: datetime, new_articles: list[dict], feed_status: dict) -> None:
    """Write the audit folder.

    audit/skipped/YYYY-MM-DD.csv   every newly-seen article the filter marked SKIP
    audit/feed_problems.csv        one row per feed per run that failed, was
                                   truncated, or showed a possible gap
    GitHub shows CSV files as tables, and they open directly in Google Sheets.
    """
    day = now.strftime("%Y-%m-%d")
    run = now.isoformat(timespec="seconds")
    skipped = [a for a in new_articles if a.get("filter_status") == "SKIP"]
    if skipped:
        _append_csv(
            AUDIT_DIR / "skipped" / f"{day}.csv",
            ["run_at", "filter_reason", "source", "language", "title", "summary", "published", "url"],
            [[run, a.get("filter_reason"), a.get("source"), a.get("language"), a.get("title"),
              (a.get("summary") or "")[:300], a.get("published"), a.get("url")] for a in skipped],
        )
    problems = []
    for name, st in sorted(feed_status.items()):
        issues = []
        if not st.get("ok"):
            issues.append("failed")
        if st.get("truncated"):
            issues.append("over_limit")
        if st.get("possible_gap"):
            issues.append("possible_gap")
        if issues:
            problems.append([run, name, "+".join(issues), st.get("error") or "",
                             st.get("raw_count", 0), st.get("count", 0), st.get("truncated", 0), st.get("new", 0)])
    if problems:
        _append_csv(
            AUDIT_DIR / "feed_problems.csv",
            ["run_at", "feed", "issue", "error", "items_in_feed", "items_read", "items_over_limit", "new_items"],
            problems,
        )
    print(f"[aggregator] audit: {len(skipped)} skipped article(s), {len(problems)} feed problem row(s)")


def fetch_feed(feed: dict) -> tuple[dict, list[dict], str | None, int]:
    """Return (feed_meta, articles, error_msg_or_none, raw_entry_count)."""
    try:
        resp = requests.get(
            feed["url"],
            timeout=PER_FEED_TIMEOUT_S,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            },
        )
        resp.raise_for_status()
        body = resp.content
    except Exception as e:
        return feed, [], f"fetch_error: {e}", 0

    if b"Just a moment" in body or b"challenge-platform" in body:
        return feed, [], "cloudflare_challenge", 0

    parsed = feedparser.parse(body)
    if parsed.bozo and not parsed.entries:
        return feed, [], f"parse_error: {parsed.bozo_exception!r}", 0

    raw_count = len(parsed.entries)
    cap = int(feed.get("max_items") or MAX_ITEMS_PER_FEED_DEFAULT)
    articles: list[dict] = []
    for entry in parsed.entries[:cap]:
        title = (entry.get("title") or "").strip()
        url = entry.get("link") or ""
        if isinstance(url, list) and url:
            url = url[0]
        if not title or not url:
            continue
        summary = strip_html(
            entry.get("summary")
            or entry.get("description")
            or (entry.get("content", [{}])[0].get("value") if entry.get("content") else "")
            or ""
        )[:400]
        published = entry.get("published") or entry.get("updated") or ""
        articles.append(
            {
                "id": article_id(url, title),
                "title": title,
                "url": url,
                "summary": summary,
                "published": published,
                "source": feed["name"],
                "category": feed["category"],
                "language": feed["language"],
                "priority": feed["priority"],
            }
        )
    return feed, articles, None, raw_count


# ---- main ----------------------------------------------------------------

def main() -> int:
    registry = load_registry()
    feeds = registry["feeds"]
    seen = load_seen()
    now = datetime.now(timezone.utc)

    feed_status: dict[str, dict] = {}
    all_articles: list[dict] = []

    print(f"[aggregator] fetching {len(feeds)} feeds in parallel (workers={PARALLEL_WORKERS})")
    start = time.time()

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
        futures = {pool.submit(fetch_feed, f): f for f in feeds}
        for fut in as_completed(futures):
            feed, articles, err, raw_count = fut.result()
            ok = err is None
            cap = int(feed.get("max_items") or MAX_ITEMS_PER_FEED_DEFAULT)
            feed_status[feed["name"]] = {
                "ok": ok,
                "count": len(articles),
                "raw_count": raw_count,
                "cap": cap,
                "truncated": max(0, raw_count - cap),
                "new": 0,
                "possible_gap": False,
                "error": err,
                "fetched_at": now.isoformat(),
                "category": feed["category"],
            }
            if ok:
                all_articles.extend(articles)
                note = f" (TRUNCATED {raw_count - cap} over cap)" if raw_count > cap else ""
                print(f"  ✓ {feed['name']:28s} {len(articles):3d} items{note}")
            else:
                print(f"  ✗ {feed['name']:28s} {err}")

    print(f"[aggregator] fetched in {time.time() - start:.1f}s; total raw items: {len(all_articles)}")

    # Apply conservative negative-only filter and annotate every article.
    # SKIP articles are retained for audit and included in the email; Cowork
    # caches them but only queues PASS articles for editorial review.
    for a in all_articles:
        status, reason = classify_article(a["title"], a["summary"], registry)
        a["filter_status"] = status
        a["filter_reason"] = reason

    # dedup across this run by id
    deduped: dict[str, dict] = {}
    for a in all_articles:
        deduped.setdefault(a["id"], a)
    all_articles = list(deduped.values())

    # mark which are newly-seen vs already-in seen_ids
    had_history = {name: bool(seen.get(name)) for name in feed_status}
    for a in all_articles:
        bucket = seen.setdefault(a["source"], [])
        a["new"] = a["id"] not in bucket
        if a["new"]:
            bucket.append(a["id"])
            if a["source"] in feed_status:
                feed_status[a["source"]]["new"] += 1

    # Gap check. If a feed we have read before returns items that are ALL new,
    # nothing overlaps with the last run, so the feed probably published more
    # between runs than it shows at once. Those in-between items were never seen.
    for name, st in feed_status.items():
        if st["ok"] and st["count"] > 0 and had_history.get(name) and st["new"] >= st["count"]:
            st["possible_gap"] = True

    save_seen(seen)

    new_articles = [a for a in all_articles if a["new"]]

    # Add this run's new articles to the pending digest. The email step sends
    # the whole pending list and only clears it after a successful send, so a
    # failed email no longer loses articles that seen_ids has already recorded.
    pending = load_pending()
    pending_ids = {a["id"] for a in pending["articles"]}
    pending["articles"].extend(a for a in new_articles if a["id"] not in pending_ids)
    pending["runs"].append({"generated_at": now.isoformat(), "feed_status": feed_status})
    save_pending(pending)
    print(f"[aggregator] pending digest now holds {len(pending['articles'])} articles "
          f"across {len(pending['runs'])} run(s)")

    write_audit(now, new_articles, feed_status)

    truncated = {n: s for n, s in feed_status.items() if s.get("truncated")}
    gaps = [n for n, s in feed_status.items() if s.get("possible_gap")]
    if truncated:
        print("[aggregator] TRUNCATED FEEDS (raise max_items for these):")
        for n, s in truncated.items():
            print(f"  - {n}: returned {s['raw_count']}, kept {s['cap']}")
    if gaps:
        print("[aggregator] POSSIBLE GAPS (no overlap with last run; feed may publish more per day than it shows):")
        for n in gaps:
            print(f"  - {n}")
    pass_articles = [a for a in all_articles if a.get("filter_status") == "PASS"]
    skip_articles = [a for a in all_articles if a.get("filter_status") == "SKIP"]
    pass_new = [a for a in new_articles if a.get("filter_status") == "PASS"]
    skip_new = [a for a in new_articles if a.get("filter_status") == "SKIP"]
    print(
        f"[aggregator] pass={len(pass_articles)} | skip={len(skip_articles)} "
        f"| new={len(new_articles)} | pass_new={len(pass_new)} | skip_new={len(skip_new)}"
    )

    output = {
        "generated_at": now.isoformat(),
        "feed_status": feed_status,
        "counts": {
            "total": len(all_articles),
            "pass": len(pass_articles),
            "skip": len(skip_articles),
            "new": len(new_articles),
            "pass_new": len(pass_new),
            "skip_new": len(skip_new),
        },
        "articles": all_articles,
    }

    with OUTPUT_PATH.open("w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"[aggregator] wrote {OUTPUT_PATH}")

    # archive snapshot
    ARCHIVE_DIR.mkdir(exist_ok=True)
    stamp = now.strftime("%Y-%m-%dT%H-%MZ")
    archive_path = ARCHIVE_DIR / f"{stamp}.json"
    with archive_path.open("w") as f:
        # Store all new articles, including SKIP items, so filter decisions
        # remain auditable over time.
        json.dump(
            {
                "generated_at": output["generated_at"],
                "counts": output["counts"],
                "feed_status": feed_status,
                "articles": new_articles,
            },
            f,
            indent=2,
            ensure_ascii=False,
        )
    print(f"[aggregator] archived {archive_path}")

    # summary table
    failed = [n for n, s in feed_status.items() if not s["ok"]]
    if failed:
        print("[aggregator] FAILED FEEDS:")
        for n in failed:
            print(f"  - {n}: {feed_status[n]['error']}")
    return 0 if len(failed) < len(feeds) else 1


if __name__ == "__main__":
    sys.exit(main())
