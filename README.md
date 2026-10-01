# scanner-v3

Stable, zero-touch RSS aggregation for the CMAX Intelligence Scanner.
GitHub Actions fetches 40 feeds once a day and emails a digest containing every article first seen since the last successful email, labelled PASS or SKIP by a conservative negative-only filter.

## How it runs

1. `.github/workflows/fetch_feeds.yml` triggers `fetch_feeds.py` on a
   cron schedule (06:05 UTC daily) and on manual `workflow_dispatch`.
   Each feed is read up to 200 items (override per feed with `"max_items"` in
   `feed_registry.json`). Items over the cap and feeds with no overlap between
   runs are reported in the email so nothing is lost silently.

## Audit folder

Every run writes to `audit/` in the repo:

- `audit/skipped/YYYY-MM-DD.csv`: every new article the filter marked SKIP, with the rule that fired, source, title, summary and link. Use it to spot good stories the rules wrongly cut.
- `audit/feed_problems.csv`: one row per feed per run that failed to load, had more items than the read limit, or showed a possible gap.

GitHub displays CSV files as tables. You can also open them in Google Sheets.
2. `fetch_feeds.py` reads `feed_registry.json`, fetches each feed in
   parallel, applies conservative negative-only rules, deduplicates against
   `seen_ids.json`, and writes `articles.json`.
3. `send_email.py` packages ALL new articles into an email digest, preserving
   `filter_status` (`PASS`/`SKIP`) and `filter_reason`, then sends
   via SMTP using the secrets configured in repo settings.
4. The workflow commits `articles.json` and `articles_archive/` back
   to the repo as an audit trail.

## Required secrets

Repository → Settings → Secrets and variables → Actions:

```
SMTP_USER       Gmail address that sends the digest
SMTP_PASSWORD   Gmail App Password (generate at myaccount.google.com/apppasswords)
SMTP_TO         Where the digest lands (often same as SMTP_USER)
```

Optional:

```
SMTP_HOST       defaults to smtp.gmail.com
SMTP_PORT       defaults to 587
SMTP_FROM_NAME  defaults to "Scanner V3"
```

## Adding a feed

Edit `feed_registry.json` and commit. The workflow picks up the new
entry on its next run. Each feed needs:

```json
{
  "name": "Source Name",
  "category": "<category>",
  "url": "https://example.com/feed",
  "language": "en",
  "priority": 1
}
```

Filtering is **negative-only**. The source list itself provides the broad relevance bias; the deterministic layer only removes obvious noise. Rules are defined in `negative_filter_vocab_en` and `negative_filter_rules_en`.

A rule must match completely before an article is labelled `SKIP`. Common words such as `review`, `price`, `court`, `government`, `car`, `deal`, and `weather` have no exclusion power by themselves. If no complete negative rule matches, the article is labelled `PASS`.

Both PASS and SKIP articles are emailed so filter decisions remain auditable. Cowork caches both but only PASS articles enter the normal editorial queue.

The three Google News AU topic feeds (Business, Technology, Science) are broad discovery feeds. They are separate from the existing Google News `site:DOMAIN` fallbacks, which merely proxy specific publishers that block GitHub Actions IPs.

## When a feed starts failing

Check the workflow run log under the "Run aggregator" step. Common
failure modes:

- **`fetch_error: 403 Forbidden`** — Cloudflare is blocking the GitHub
  Actions IP range. Swap to a Google News RSS fallback:
  `https://news.google.com/rss/search?q=site:DOMAIN+when:7d&hl=en-US&gl=US&ceid=US:en`.
- **`fetch_error: 502 Bad Gateway`** — third-party feed mirror is down.
  Find an alternative URL or use Google News fallback.
- **`parse_error: not well-formed`** — the URL is returning HTML, not
  RSS. The site removed RSS support; use Google News fallback.
- **`cloudflare_challenge`** — body contains `Just a moment` or
  `challenge-platform`. Same fix as 403.

The `+when:7d` operator is important when using Google News RSS — without
it, results include archive entries going back years that all expire on
ingest.

## Local testing

```bash
pip install -r requirements.txt
python fetch_feeds.py
# Check articles.json for output
SMTP_USER=... SMTP_PASSWORD=... SMTP_TO=... python send_email.py
```
