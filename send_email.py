#!/usr/bin/env python3
"""
Package articles.json into an email digest and send it via Gmail SMTP.

Reads secrets from env (set via GitHub Actions secrets):
  SMTP_HOST           default: smtp.gmail.com
  SMTP_PORT           default: 587
  SMTP_USER           (required) — e.g. you@gmail.com
  SMTP_PASSWORD       (required) — Gmail app password
  SMTP_TO             (required) — e.g. you@gmail.com (can equal SMTP_USER)
  SMTP_FROM_NAME      default: Scanner V3

The digest includes ALL articles first seen since the last successful email
(normally one daily run; more if an earlier email failed). Each article carries:
  filter_status: PASS | SKIP
  filter_reason: no_negative_rule_matched | <negative rule id>

PASS articles are eligible for Cowork's editorial shortlist. SKIP articles are
included for audit visibility but are not placed into the normal scan queue.

The full machine-readable payload is embedded between explicit delimiters:
  -----BEGIN SCANNER_V3_JSON-----
  {...payload...}
  -----END SCANNER_V3_JSON-----
"""

from __future__ import annotations

import json
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ARTICLES_PATH = ROOT / "articles.json"
PENDING_PATH = ROOT / "pending_digest.json"

PREVIEW_PER_SOURCE = 3
PREVIEW_PER_REASON = 3
DELIM_BEGIN = "-----BEGIN SCANNER_V3_JSON-----"
DELIM_END = "-----END SCANNER_V3_JSON-----"


def feed_health(runs: list[dict]) -> dict:
    """Summarise truncation, gaps and failures per feed across every run in the digest."""
    health: dict[str, dict] = {}
    for run in runs:
        for name, st in (run.get("feed_status") or {}).items():
            h = health.setdefault(name, {"runs": 0, "failed_runs": 0, "truncated_items": 0, "gap_runs": 0})
            h["runs"] += 1
            if not st.get("ok"):
                h["failed_runs"] += 1
            h["truncated_items"] += int(st.get("truncated") or 0)
            if st.get("possible_gap"):
                h["gap_runs"] += 1
    return health


def build_payload(articles: dict, pending: dict | None) -> dict:
    """Embed every NEW article since the last successful email, PASS and SKIP.

    Uses pending_digest.json (filled by every fetch run) when present. Falls
    back to the new articles in articles.json for older repos.
    """
    if pending and pending.get("articles") is not None:
        new_articles = pending.get("articles", [])
        runs = pending.get("runs", [])
    else:
        new_articles = [a for a in articles.get("articles", []) if a.get("new")]
        runs = [{"generated_at": articles.get("generated_at"), "feed_status": articles.get("feed_status", {})}]
    pass_new = sum(1 for a in new_articles if a.get("filter_status") == "PASS")
    skip_new = sum(1 for a in new_articles if a.get("filter_status") == "SKIP")
    return {
        "generated_at": articles.get("generated_at"),
        "digest_id": articles.get("generated_at"),
        "fetch_runs": [r.get("generated_at") for r in runs],
        "feed_status": articles.get("feed_status", {}),
        "feed_health": feed_health(runs),
        "counts": {
            **articles.get("counts", {}),
            "new": len(new_articles),
            "pass_new": pass_new,
            "skip_new": skip_new,
        },
        "articles": new_articles,
    }


def build_subject(payload: dict) -> str:
    stamp = (payload.get("generated_at") or datetime.utcnow().isoformat()).split(".")[0].replace("+00:00", "") + "Z"
    counts = payload.get("counts", {})
    status = payload.get("feed_status", {})
    ok = sum(1 for s in status.values() if s.get("ok"))
    total = len(status)
    return (
        f"[scanner-v3] {stamp} | {counts.get('pass_new', 0)} pass + "
        f"{counts.get('skip_new', 0)} skip | {ok}/{total} feeds ok"
    )


def build_plain_body(payload: dict) -> str:
    counts = payload.get("counts", {})
    status = payload.get("feed_status", {})
    failed = [n for n, s in status.items() if not s.get("ok")]
    articles = payload.get("articles", [])
    passed = [a for a in articles if a.get("filter_status") == "PASS"]
    skipped = [a for a in articles if a.get("filter_status") == "SKIP"]

    lines = [
        f"Generated: {payload.get('generated_at')}",
        f"Counts: total={counts.get('total', 0)} pass={counts.get('pass', 0)} "
        f"skip={counts.get('skip', 0)} new={counts.get('new', 0)} "
        f"pass_new={counts.get('pass_new', 0)} skip_new={counts.get('skip_new', 0)}",
        f"Feeds ok: {len(status) - len(failed)}/{len(status)}",
    ]
    if failed:
        lines.append("Failed feeds: " + ", ".join(failed))
    lines.append(f"Fetch runs in this digest: {len(payload.get('fetch_runs', []))}")
    health = payload.get("feed_health", {})
    trunc = {n: h for n, h in health.items() if h.get("truncated_items")}
    gaps = {n: h for n, h in health.items() if h.get("gap_runs")}
    if trunc:
        lines.append("Truncated feeds (items over the cap were not read): " + ", ".join(
            f"{n} ({h['truncated_items']})" for n, h in trunc.items()))
    if gaps:
        lines.append("Possible gaps (no overlap with previous run): " + ", ".join(
            f"{n} ({h['gap_runs']} run(s))" for n, h in gaps.items()))

    # Mixed preview: a few PASS articles from every source, so one busy feed
    # can't fill the whole preview.
    by_source: dict[str, list[dict]] = {}
    for a in passed:
        by_source.setdefault(a.get("source", "?"), []).append(a)
    lines.extend(["", f"PASSED — eligible for Cowork editorial review ({len(passed)} total, "
                      f"up to {PREVIEW_PER_SOURCE} per source):"])
    if not passed:
        lines.append("  (none)")
    for src in sorted(by_source, key=lambda x: (-len(by_source[x]), x.lower())):
        items = by_source[src]
        lines.append(f"  [{src}] {len(items)} article(s)")
        for a in items[:PREVIEW_PER_SOURCE]:
            lines.append(f"    • {a.get('title')}")
            lines.append(f"      {a.get('url')}")

    # SKIP summary by reason, with a few examples each.
    by_reason: dict[str, list[dict]] = {}
    for a in skipped:
        by_reason.setdefault(a.get("filter_reason", "unknown"), []).append(a)
    lines.extend(["", f"SKIPPED — audit only ({len(skipped)} total, "
                      f"up to {PREVIEW_PER_REASON} examples per reason):"])
    if not skipped:
        lines.append("  (none)")
    for reason in sorted(by_reason, key=lambda x: -len(by_reason[x])):
        items = by_reason[reason]
        lines.append(f"  [SKIP:{reason}] {len(items)} article(s)")
        for a in items[:PREVIEW_PER_REASON]:
            lines.append(f"    • [{a.get('source')}] {a.get('title')}")
            lines.append(f"      {a.get('url')}")

    fb = [n for n, st in status.items() if st.get("used_fallback")]
    if fb:
        lines.extend(["", "Feeds read through their fallback address: " + ", ".join(fb)])

    lines.extend([
        "",
        "Full payload follows (parsed by Cowork — do not edit):",
        "",
        DELIM_BEGIN,
        json.dumps(payload, ensure_ascii=False),
        DELIM_END,
    ])
    return "\n".join(lines)


def main() -> int:
    if not ARTICLES_PATH.exists():
        print(f"[send_email] FATAL: {ARTICLES_PATH} not found", file=sys.stderr)
        return 2

    with ARTICLES_PATH.open() as f:
        articles = json.load(f)

    pending = None
    if PENDING_PATH.exists():
        try:
            with PENDING_PATH.open() as f:
                pending = json.load(f)
        except Exception:
            pending = None

    payload = build_payload(articles, pending)
    subject = build_subject(payload)
    body = build_plain_body(payload)

    smtp_host = os.environ.get("SMTP_HOST") or "smtp.gmail.com"
    smtp_port = int(os.environ.get("SMTP_PORT") or "587")
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    smtp_to = os.environ.get("SMTP_TO")
    smtp_from_name = os.environ.get("SMTP_FROM_NAME") or "Scanner V3"

    missing = [k for k, v in {
        "SMTP_USER": smtp_user,
        "SMTP_PASSWORD": smtp_password,
        "SMTP_TO": smtp_to,
    }.items() if not v]
    if missing:
        print(f"[send_email] FATAL: missing env vars: {missing}", file=sys.stderr)
        return 2

    msg = MIMEMultipart("alternative")
    msg["From"] = f"{smtp_from_name} <{smtp_user}>"
    msg["To"] = smtp_to
    msg["Subject"] = subject
    msg["X-Scanner-V3"] = "1"
    msg.attach(MIMEText(body, "plain", "utf-8"))

    print(f"[send_email] connecting {smtp_host}:{smtp_port} as {smtp_user}")
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        s.login(smtp_user, smtp_password)
        s.send_message(msg)
    print(f"[send_email] sent \"{subject}\" to {smtp_to}")

    # Clear the pending digest only after a successful send. If anything above
    # failed, the articles stay pending and go out with the next email.
    with PENDING_PATH.open("w") as f:
        json.dump({"articles": [], "runs": []}, f, indent=2)
    print("[send_email] cleared pending_digest.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
