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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
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
STALE_DAYS = 14  # a feed whose newest item is older than this is reported as "stale"


# ---- helpers -------------------------------------------------------------

_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
}


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
        cap = 50000 if k.startswith("_") else SEEN_ID_CAP
        if len(v) > cap:
            seen[k] = v[-cap:]
    with SEEN_IDS_PATH.open("w") as f:
        json.dump(seen, f, indent=2, ensure_ascii=False)


_TRACKING_PARAM = re.compile(r"^(?:utm_.*|fbclid|gclid|mc_cid|mc_eid|cmpid|ref_src)$", re.IGNORECASE)


def normalise_url(url: str) -> str:
    """Lower-case the host, drop tracking parameters, fragments and a trailing
    slash. Google News links (which carry ?oc=5) are left as they are."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    try:
        p = urlsplit((url or "").strip())
    except ValueError:
        return (url or "").strip()
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not _TRACKING_PARAM.match(k)]
    path = p.path.rstrip("/") or "/"
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, urlencode(query), ""))


def article_id(url: str, title: str = "") -> str:
    """A story is identified by its link only. The title is ignored, because
    Google News and some sites change the outlet label or edit the headline
    after we have already seen the story."""
    return hashlib.sha256(normalise_url(url).encode()).hexdigest()[:16]


def legacy_article_id(url: str, title: str) -> str:
    """The old formula (link plus title). Kept so ids saved before the change
    still match."""
    return hashlib.sha256(f"{url}|{title}".encode()).hexdigest()[:16]


def seed_seen_from_archive(seen: dict) -> int:
    """One-off migration. Ids used to include the title, so they cannot be
    turned back into links. Every story we have ever sent is in articles_archive
    and pending_digest, so read the links from there and mark them as seen.
    Without this, the first run after the change would treat every old story
    as new."""
    if "_archive_seeded" in seen:
        return 0
    ids: set[str] = set()
    for path in sorted(ARCHIVE_DIR.glob("*.json")) if ARCHIVE_DIR.exists() else []:
        try:
            with path.open() as f:
                data = json.load(f)
        except Exception:
            continue
        for a in data.get("articles", []):
            if a.get("url"):
                ids.add(article_id(a["url"]))
                tk = google_title_key(a)
                if tk:
                    ids.add(tk)
    if PENDING_PATH.exists():
        try:
            with PENDING_PATH.open() as f:
                for a in json.load(f).get("articles", []):
                    if a.get("url"):
                        ids.add(article_id(a["url"]))
                        tk = google_title_key(a)
                        if tk:
                            ids.add(tk)
        except Exception:
            pass
    seen["_archive_seeded"] = sorted(ids)
    return len(ids)


def google_title_key(a: dict) -> str | None:
    """Google News sometimes issues a new link for a story we already have, with
    the same headline. For Google News items only, the headline is a second way
    to recognise a story. Short or generic headlines are ignored."""
    url = a.get("url", "")
    if "news.google.com" not in url:
        return None
    core, _ = split_publisher(a.get("title", ""), url)
    core = re.sub(r"\s+", " ", (core or "").strip().lower())
    if len(core) < 12:
        return None
    return "t:" + hashlib.sha256(core.encode()).hexdigest()[:16]


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


def classify_article(title: str, summary: str, registry: dict, language: str = "en") -> tuple[str, str]:
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
    rules = list(registry.get("negative_filter_rules_en", []))
    if language == "zh":
        # Chinese rules mirror the English ones, plus a few Chinese-only
        # templated-notice rules. English rules still run on Chinese titles.
        vocab = {**vocab, **registry.get("negative_filter_vocab_zh", {})}
        rules = rules + list(registry.get("negative_filter_rules_zh", []))

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


# ---- post filters: age, non-article pages, duplicates ---------------------
# These run after the negative rules and only ever turn PASS into SKIP, with a
# reason, so everything they catch still lands in the email and audit folder.

_MONTHS = r"(?:January|February|March|April|May|June|July|August|September|October|November|December)"


def parse_published(s: str) -> datetime | None:
    if not s:
        return None
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(s)
    except Exception:
        dt = None
    if dt is None:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def split_publisher(title: str, url: str) -> tuple[str, str]:
    """Google News titles end with ' - Publisher'. Return (headline, publisher)."""
    if "news.google.com" in (url or "") and title.startswith("- "):
        return "", title[2:].strip()
    if "news.google.com" in (url or "") and " - " in title:
        core, pub = title.rsplit(" - ", 1)
        return core.strip(), pub.strip()
    return title.strip(), ""


_NON_ARTICLE_PATTERNS = [
    r"^(?:registration|register|sign in|sign up|log in|login|subscribe|subscription|newsletter sign-?up)$",
    r"^author:\s",
    r"\barchives?$",
    r"^(?:latest\s+)?(?:\w+\s+){0,2}(?:news|articles)$",
    # Chinese verification / captcha pages and site section pages
    r"请完成.{0,6}验证",
    r"验证后继续",
    r"按住.{0,8}拖动",
    r"拖动.{0,8}拼图",
    r"^36Kr\s*直播$",
    r"^36氪_",
    r"项目信息-36氪",
    r"_精彩视频为您呈现$",
]


_CJK = re.compile(r"[\u4e00-\u9fff]")


def translated_copy_reason(a: dict) -> str | None:
    """A Chinese-language feed item whose headline has no Chinese characters is
    an automatic English or German copy of a story we already have in Chinese
    (36kr's Google News copy does this)."""
    if a.get("language") != "zh":
        return None
    core, _ = split_publisher(a.get("title", ""), a.get("url", ""))
    if core and not _CJK.search(core):
        return "translated_copy"
    return None


_STALE_YEAR = re.compile(r"(?:^|\s)20(?:24|25)(?![年\d])")


def stale_year_reason(a: dict) -> str | None:
    """36kr's Google News copy includes rewritten pieces whose headlines carry
    2024 or 2025 for events in 2026 ("2024国庆黄金周车市实探"). Real headlines
    write the year as 2025年, so a bare 2024 or 2025 at the start of a headline,
    or after a space, marks one of these. Applies to 36kr only."""
    if a.get("source") != "36kr":
        return None
    core, _ = split_publisher(a.get("title", ""), a.get("url", ""))
    return "stale_year_title" if _STALE_YEAR.search(core or "") else None


def non_article_reason(a: dict) -> str | None:
    core, pub = split_publisher(a.get("title", ""), a.get("url", ""))
    if not core or core in {"-", "–"}:
        return "non_article_page"
    if pub and core.lower() == pub.lower():
        return "non_article_page"
    for pat in _NON_ARTICLE_PATTERNS:
        if re.search(pat, core, re.IGNORECASE):
            return "non_article_page"
    # Short Title-Case labels from Google News site: queries are almost always
    # author, agency or section pages ("Toby Hart", "CJ WORX", "Open Mic").
    community = pub.lower() in {"hacker news", "reddit"} or a.get("category") == "social_signal" \
        or a.get("source") == "Hacker News"
    if pub and a.get("language") == "en" and not community:
        words = core.split()
        if (1 <= len(words) <= 3
                and not re.search(r"[\d:?!,]", core)
                and all(w == "&" or w[0].isupper() for w in words)):
            return "non_article_page"
    return None


def _norm_title(a: dict) -> str:
    core, _ = split_publisher(a.get("title", ""), a.get("url", ""))
    return re.sub(r"[\W_]+", "", core.lower())


def apply_post_filters(articles: list[dict], now: datetime, registry: dict) -> dict:
    cfg = registry.get("post_filters", {})
    max_age = int(cfg.get("max_age_days", 7))
    cutoff = now - timedelta(days=max_age)
    counts = {"too_old": 0, "non_article_page": 0, "duplicate": 0, "translated_copy": 0,
              "stale_year_title": 0}
    seen_titles: set[tuple[str, str]] = set()
    for a in articles:
        if a.get("filter_status") != "PASS":
            continue
        dt = parse_published(a.get("published", ""))
        if dt is not None and dt < cutoff:
            a["filter_status"], a["filter_reason"] = "SKIP", "too_old"
            counts["too_old"] += 1
            continue
        reason = non_article_reason(a)
        if reason:
            a["filter_status"], a["filter_reason"] = "SKIP", reason
            counts[reason] += 1
            continue
        reason = translated_copy_reason(a) or stale_year_reason(a)
        if reason:
            a["filter_status"], a["filter_reason"] = "SKIP", reason
            counts[reason] += 1
            continue
        key = (a.get("source", ""), _norm_title(a))
        if key[1] and key in seen_titles:
            a["filter_status"], a["filter_reason"] = "SKIP", "duplicate"
            counts["duplicate"] += 1
            continue
        seen_titles.add(key)
    return counts


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
        if st.get("state") in ("failed", "empty", "stale"):
            issues.append(st["state"])
        elif not st.get("ok"):
            issues.append("failed")
        if st.get("truncated"):
            issues.append("over_limit")
        if st.get("possible_gap"):
            issues.append("possible_gap")
        if st.get("used_fallback"):
            issues.append("used_fallback")
        if issues:
            note = st.get("error") or " | ".join(st.get("attempts") or [])
            problems.append([run, name, "+".join(issues), note,
                             st.get("raw_count", 0), st.get("count", 0), st.get("truncated", 0), st.get("new", 0)])
    if problems:
        _append_csv(
            AUDIT_DIR / "feed_problems.csv",
            ["run_at", "feed", "issue", "error", "items_in_feed", "items_read", "items_over_limit", "new_items"],
            problems,
        )
    print(f"[aggregator] audit: {len(skipped)} skipped article(s), {len(problems)} feed problem row(s)")


_HOST_LOCKS: dict[str, threading.Lock] = {}
_HOST_LOCKS_GUARD = threading.Lock()
PACED_HOSTS = {"www.reddit.com": 2.0, "old.reddit.com": 2.0}  # seconds between requests to the same host


def _get(url: str):
    """GET with per-host pacing for hosts that rate-limit bursts (Reddit)."""
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    delay = PACED_HOSTS.get(host)
    if not delay:
        return requests.get(url, timeout=PER_FEED_TIMEOUT_S, headers=_HEADERS)
    with _HOST_LOCKS_GUARD:
        lock = _HOST_LOCKS.setdefault(host, threading.Lock())
    with lock:
        resp = requests.get(url, timeout=PER_FEED_TIMEOUT_S, headers=_HEADERS)
        time.sleep(delay)
        return resp


def feed_addresses(feed: dict) -> list[str]:
    """Main address first, then any backups, in order. A feed can list backups
    in \"fallback_urls\" (several) or \"fallback_url\" (one)."""
    urls = [feed["url"]]
    urls.extend(feed.get("fallback_urls") or [])
    if feed.get("fallback_url"):
        urls.append(feed["fallback_url"])
    seen_u: list[str] = []
    for u in urls:
        if u and u not in seen_u:
            seen_u.append(u)
    return seen_u


def fetch_feed(feed: dict) -> tuple[dict, list[dict], str | None, int]:
    """Try the main address, then each backup in order. Stop at the first one
    that returns articles. A backup is also tried when the main address works
    but returns nothing, because an empty feed is not a healthy feed.

    Returns (feed_meta, articles, error_or_none, raw_entry_count). Sets
    feed[\"_used_fallback\"] when a backup produced the result, and
    feed[\"_attempts\"] to a short note on every address tried."""
    feed["_used_fallback"] = False
    attempts: list[str] = []
    best = None
    for i, url in enumerate(feed_addresses(feed)):
        result = _fetch_one(feed, url)
        err, raw_count = result[2], result[3]
        label = "main" if i == 0 else f"backup{i}"
        if err is not None:
            attempts.append(f"{label}: {err}")
        elif raw_count == 0:
            attempts.append(f"{label}: returned 0 items")
        else:
            attempts.append(f"{label}: {raw_count} items")
        if err is None and raw_count > 0:
            feed["_used_fallback"] = i > 0
            feed["_attempts"] = attempts
            return result
        if best is None or (err is None and best[2] is not None):
            best = result  # prefer a clean-but-empty result over an error
    feed["_attempts"] = attempts
    if best[2] is not None and len(attempts) > 1:
        best = (best[0], best[1], " | ".join(attempts), best[3])
    return best


_BAD_XML_CHARS = re.compile(rb"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _clean_feed_body(body: bytes) -> bytes:
    """Strip a byte-order mark, anything before the first '<', and control
    characters that XML does not allow."""
    if body.startswith(b"\xef\xbb\xbf"):
        body = body[3:]
    first = body.find(b"<")
    if first > 0:
        body = body[first:]
    return _BAD_XML_CHARS.sub(b"", body)


def _describe_body(resp, body: bytes) -> str:
    """One-line description of what the site actually sent back. This is what
    tells us whether a broken feed is bad XML or an HTML block page."""
    ctype = resp.headers.get("Content-Type", "?")
    start = body[:160].decode("utf-8", errors="replace")
    start = re.sub(r"\s+", " ", start).strip()
    return f"http={resp.status_code} type={ctype} len={len(body)} starts={start!r}"


def _fetch_one(feed: dict, url: str) -> tuple[dict, list[dict], str | None, int]:
    feed_url = url
    try:
        resp = _get(url)
        resp.raise_for_status()
        body = resp.content
    except Exception as e:
        return feed, [], f"fetch_error: {e}", 0

    if b"Just a moment" in body or b"challenge-platform" in body:
        return feed, [], "cloudflare_challenge", 0

    parsed = feedparser.parse(body)
    if not parsed.entries:
        # Some feeds carry stray bytes or leading junk that stop the parser.
        # Clean the text and try once more before giving up.
        cleaned = _clean_feed_body(body)
        if cleaned != body:
            parsed = feedparser.parse(cleaned)
    if not parsed.entries and parsed.bozo:
        return feed, [], f"parse_error: {parsed.bozo_exception!r} | {_describe_body(resp, body)}", 0
    if not parsed.entries and not re.search(rb"<(?:rss|feed|rdf:RDF)\b", body[:2000], re.IGNORECASE):
        # A web page (login wall, block page) instead of a feed.
        return feed, [], f"not_a_feed | {_describe_body(resp, body)}", 0

    raw_count = len(parsed.entries)
    cap = int(feed.get("max_items") or MAX_ITEMS_PER_FEED_DEFAULT)
    articles: list[dict] = []
    for entry in parsed.entries[:cap]:
        title = (entry.get("title") or "").strip()
        url = entry.get("link") or ""
        if isinstance(url, list) and url:
            url = url[0]
        if url and not url.lower().startswith(("http://", "https://")):
            from urllib.parse import urljoin
            base = feed.get("link_base") or parsed.feed.get("link") or feed_url
            url = urljoin(base, url)
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
    feeds = [f for f in registry["feeds"] if f.get("enabled", True)]
    switched_off = [f["name"] for f in registry["feeds"] if not f.get("enabled", True)]
    if switched_off:
        print(f"[aggregator] switched off in registry: {', '.join(switched_off)}")
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
                "used_fallback": bool(feed.get("_used_fallback")),
                "attempts": feed.get("_attempts") or [],
                "fetched_at": now.isoformat(),
                "category": feed["category"],
            }
            if ok:
                all_articles.extend(articles)
                note = f" (TRUNCATED {raw_count - cap} over cap)" if raw_count > cap else ""
                if feed.get("_used_fallback"):
                    note += " (via fallback)"
                print(f"  ✓ {feed['name']:28s} {len(articles):3d} items{note}")
            else:
                print(f"  ✗ {feed['name']:28s} {err}")

    print(f"[aggregator] fetched in {time.time() - start:.1f}s; total raw items: {len(all_articles)}")

    # Give every feed a plain-English state. "ok" only means no error, so a
    # feed that returns nothing, or only old articles, used to look healthy.
    #   working = returned articles, newest is recent
    #   empty   = no error, but zero articles came back
    #   stale   = articles came back, but the newest is older than STALE_DAYS
    #   failed  = error on every address tried
    newest: dict[str, datetime] = {}
    for a in all_articles:
        dt = parse_published(a.get("published", ""))
        if dt is not None and (a["source"] not in newest or dt > newest[a["source"]]):
            newest[a["source"]] = dt
    for name, st in feed_status.items():
        if not st["ok"]:
            st["state"] = "failed"
        elif st["raw_count"] == 0:
            st["state"] = "empty"
        elif name in newest and (now - newest[name]).days > STALE_DAYS:
            st["state"] = "stale"
        else:
            st["state"] = "working"
        st["newest_item"] = newest[name].isoformat() if name in newest else None
    states: dict[str, list[str]] = {}
    for name, st in feed_status.items():
        states.setdefault(st["state"], []).append(name)
    print("[aggregator] feed states: " + ", ".join(
        f"{k}={len(v)}" for k, v in sorted(states.items())))
    for k in ("failed", "empty", "stale"):
        for n in sorted(states.get(k, [])):
            print(f"  ! {k:6s} {n}  {feed_status[n].get('error') or ''}")

    # Apply conservative negative-only filter and annotate every article.
    # SKIP articles are retained for audit and included in the email; Cowork
    # caches them but only queues PASS articles for editorial review.
    # Feed order decides which copy wins when one link appears in two feeds.
    all_articles.sort(key=lambda a: (a.get("priority", 9), a["source"]))
    for a in all_articles:
        status, reason = classify_article(a["title"], a["summary"], registry, a.get("language", "en"))
        a["filter_status"] = status
        a["filter_reason"] = reason

    # dedup across this run by id
    deduped: dict[str, dict] = {}
    for a in all_articles:
        deduped.setdefault(a["id"], a)
    all_articles = list(deduped.values())

    post = apply_post_filters(all_articles, now, registry)
    print(f"[aggregator] post filters: too_old={post['too_old']} "
          f"non_article_page={post['non_article_page']} duplicate={post['duplicate']} "
          f"translated_copy={post['translated_copy']} "
          f"stale_year_title={post['stale_year_title']}")

    # mark which are newly-seen vs already sent. A story counts as seen if its
    # link was seen in ANY feed. Ids saved under the old formula still match.
    seeded = seed_seen_from_archive(seen)
    if seeded:
        print(f"[aggregator] migration: marked {seeded} previously sent links as seen")
    had_history = {name: bool(seen.get(name)) for name in feed_status}
    seen_all: set[str] = set()
    for ids in seen.values():
        seen_all.update(ids)
    for a in all_articles:
        bucket = seen.setdefault(a["source"], [])
        legacy = legacy_article_id(a["url"], a["title"])
        tkey = google_title_key(a)
        a["new"] = (a["id"] not in seen_all and legacy not in seen_all
                    and (tkey is None or tkey not in seen_all))
        if a["new"]:
            bucket.append(a["id"])
            seen_all.add(a["id"])
            if tkey:
                bucket.append(tkey)
                seen_all.add(tkey)
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
