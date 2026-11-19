# CLAUDE.md — MAG Enrichment

A **shared contact-enrichment service** that any lead-type automation can call to turn a
person's name + company into an **email** (and a **phone number** when it comes free in the
same lookup). Providers: **Hunter.io** (main) → **Lusha V3** (backup). Backed by one table
that is both a **queue** and a **cache**, so the same person+company is only ever paid for
once — important because Hunter's free plan is ~50 lookups/month and that quota is now
shared across every caller.

Split out of the `../Alternate Lead Finder` project on 2026-11-19; `alternate_leads` is
just the first caller (see WRITEBACK below), not special.

## Where things live (one Supabase project)
- **`aivitcomiywiysrfwqxt`** (MAGTestProject, ap-northeast-1) — the `contact_enrichment`
  table, the `contact-enrichment` edge function, and its cron (`contact-enrichment-weekday`,
  jobid 46, `0 6 * * 1-5` = 2 PM PHT Mon-Fri) all live here. This is the SAME project that
  holds the business tables (`alternate_leads`, the lead-source tables, etc.).
- GCP project **`claudegwscli-502400`** — holds copies of some keys in Secret Manager
  (hunter-io-api-key); not required for normal operation.

## The function
- Source: `supabase/functions/contact-enrichment/index.ts` (Deno, service role,
  `verify_jwt=false`). Deployed version 1 (2026-11-19). URL:
  `https://aivitcomiywiysrfwqxt.supabase.co/functions/v1/contact-enrichment`.
- The cron path runs work in `EdgeRuntime.waitUntil` and returns 202 immediately. Use
  `?sync=1` to await the drain and see the summary.
- Deploy (no supabase CLI installed) via the Management API multipart endpoint:
  ```bash
  TOKEN=$(tr -d ' \t\r\n' < supabase_access_token.txt)
  curl -s -X POST "https://api.supabase.com/v1/projects/aivitcomiywiysrfwqxt/functions/deploy?slug=contact-enrichment" \
    -H "Authorization: Bearer $TOKEN" \
    -F 'metadata={"entrypoint_path":"index.ts","name":"contact-enrichment","verify_jwt":false};type=application/json' \
    -F 'file=@supabase/functions/contact-enrichment/index.ts;type=application/typescript'
  ```
  Syntax check (no Deno locally): `npx -y -p typescript tsc --noEmit --skipLibCheck
  --target es2022 --module esnext --moduleResolution bundler --strict <file>` (ignore
  TS2304/TS2307 — missing Deno/esm types).

## Two ways to use it
1. **Param call (what other lead types use)** — enrich one contact on demand:
   `POST .../contact-enrichment/lookup` (or `?lookup=1`), headers `Authorization: Bearer
   <anon key>` (gateway) + `x-enrich-secret: <ENRICH_SHARED_SECRET>` (function), body
   `{firstName,lastName,companyName,companyDomain?,sourceTable?,sourceId?,writeBack?}`.
   Returns `{status, email, phone, provider, cached}`. Cache is checked first, so a repeat
   contact costs no credits. If `sourceTable` is in the WRITEBACK registry and
   `writeBack!==false`, email/phone are written back to that row. See `manual_invoke.txt`.
2. **Batch / queue** — just `INSERT` a `contact_enrichment` row (`status='pending'`, name +
   company + your `source_table`/`source_id`); the weekday drain picks it up. The function
   also has a built-in enqueuer for one registered PULL source: approved `alternate_leads`
   (`status='Send intro' AND is_processed IS NULL AND email IS NULL`). **That query is
   deliberately exact and `status` is NULL until Liv approves a lead, so it returns 0 rows
   until then — expected, not a bug.** Do not swap it for `sheet_remark`.

## How a drain run works (cron, weekday 2 PM PHT)
1. **Enqueue** approved `alternate_leads` into `contact_enrichment` (idempotent on
   `(source_table, source_id)`); domain comes from the original lead's email via
   `thread_id -> closed_sequence_threads.email` (freemail domains fall back to the org name).
2. **Drain** up to `ENRICH_MAX_PER_RUN` pending rows: **cache check first** (same
   `dedup_key` already `done` -> copy result, `provider='cache'`, zero credits), else
   Hunter (`GET /v2/email-finder`, returns phone free) -> Lusha
   (`POST /v3/contacts/search-and-enrich`, `reveal:["emails"]` only — phone is billed
   separately, so skipped).
3. **Write back** via the WRITEBACK registry.

## Mark / outcome semantics (no row burned without a real check)
- email found (either provider) -> row `status='done'`, email (+phone if free) stored +
  written back; source row flagged processed.
- both providers answered "nothing" -> `status='not_found'` (+ written back as processed).
  One-shot: reset the row to `pending` to retry.
- a provider ERRORED / quota -> row stays `pending`, `attempts++`, retries next run.
- no first/last name -> `status='error'` (`last_error='missing name'`), not retried.
- systemic auth/quota (Hunter 401/403/429, Lusha 401/402/403) disables THAT provider for
  the rest of the run so a dead key isn't hammered.

## Adding a new caller / lead type
- **Want its own row updated?** Add one entry to the `WRITEBACK` map in `index.ts`:
  `source_table -> { id, email, phone?, processed? }` (column names on that table), then
  deploy. Without an entry the result is still stored in `contact_enrichment` (canonical);
  only the write-back to the origin row is skipped.
- **Want a built-in PULL (cron enqueue) like alternate_leads?** Add an `enqueueX()` helper
  modelled on `enqueueAlternateLeads()` and call it from `runDrain()`. Otherwise the caller
  just PUSHes rows or uses the param call.

## Debug params
- `?dry=1` — preview (what would be enqueued/drained), **ZERO** provider calls, **ZERO**
  writes. Safe pre-flight before a live run.
- `?sync=1` — await the enqueue+drain and return the summary (spends credits).
- `?enqueueonly=1` — enqueue pending rows but do NOT call providers (no credits).
- `?limit=N` — lower the per-run provider cap.

## Gotchas / credit safety
- **Hunter free plan ~50 lookups/MONTH, shared across all callers.** `ENRICH_MONTHLY_CAP`
  (default 50) is a hard ceiling on real provider lookups per calendar month (cache hits
  excluded); `ENRICH_MAX_PER_RUN` (default 10) caps each drain. The Hunter key returned
  **401 on 2026-11-15** — verify it's valid before the first live batch.
- **Do NOT test with live provider calls casually** — use `?dry=1`. (Original brief's
  critical note: creds were held for the first live run.)
- **Lusha V2 is sunset (410, V2 EOL 2026-11-30)** — V3 only: base `https://api.lusha.com`,
  header `api_key`, `POST /v3/contacts/search-and-enrich`; phone reveal is billed per field
  so we request `reveal:["emails"]` only.
- **Upsert-only / never truncate** (global rule): the function only UPDATEs/UPSERTs; the
  table is never truncated or bulk-deleted. Dedup on `(source_table, source_id)`.
- **pg_cron is UTC-only.** 2 PM PHT (UTC+8) Mon-Fri = `0 6 * * 1-5` (6am UTC stays the same
  weekday — no Sunday rollover, unlike the 10pm-UTC jobs in this project).
- **Secrets** (Supabase function secrets, read by the edge function): `HUNTER_IO_API_KEY`,
  `LUSHA_API_KEY`, `ENRICH_SHARED_SECRET`, plus auto-injected `SUPABASE_URL` /
  `SUPABASE_SERVICE_ROLE_KEY`. The Management API `GET /secrets` returns only hashed
  digests; set them with `POST /secrets` (or `create_edge_function_secret`, which prompts
  for the value).

## Local files (all gitignored)
- `supabase_access_token.txt` — Supabase Management API token (deploys + secrets).
- `hunter_io_api_key.txt`, `lusha_api_key.txt` — provider keys (mirror of the Supabase
  secrets; for reference / re-setting).
- `enrich_shared_secret.txt` — the `x-enrich-secret` value for the param call; give it to
  callers (n8n, other functions). Rotate by POSTing a new `ENRICH_SHARED_SECRET`.
- `manual_invoke.txt` — ready-to-paste curl/SQL examples.
