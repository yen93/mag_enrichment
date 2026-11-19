-- Central enrichment queue + cache. Any lead-type automation enqueues a row (name +
-- company, plus its own source_table/source_id for write-back) OR calls the sync endpoint;
-- the contact-enrichment function resolves email/phone (Hunter main, Lusha backup) and
-- writes results back. Upsert-only; never truncated.
-- Applied to Supabase project aivitcomiywiysrfwqxt on 2026-11-19.
create table if not exists public.contact_enrichment (
  id            bigint generated always as identity primary key,
  created_at    timestamptz not null default now(),

  -- inputs (what callers pass)
  poc_firstname text,
  poc_lastname  text,
  company_name  text,
  company_domain text,

  -- provenance (so results can be written back to the caller's row)
  source_table  text,
  source_id     bigint,
  source_type   text,
  context       jsonb,

  -- cache / dedup key: same person+company -> reuse a prior result, no provider spend
  dedup_key text generated always as (
    lower(coalesce(poc_firstname,'')) || '|' ||
    lower(coalesce(poc_lastname,''))  || '|' ||
    lower(coalesce(nullif(company_domain,''), company_name, ''))
  ) stored,

  -- outputs
  email       text,
  email_score int,
  phone       text,
  provider    text,  -- hunter | lusha | cache

  -- lifecycle
  status      text not null default 'pending',  -- pending | done | not_found | error
  attempts    int  not null default 0,
  last_error  text,
  resolved_at timestamptz
);

-- Idempotent enqueue: one request row per (source_table, source_id). Nulls are distinct,
-- so pure sync lookups (no source) never collide.
create unique index if not exists ux_contact_enrichment_source
  on public.contact_enrichment (source_table, source_id);

create index if not exists ix_contact_enrichment_dedup   on public.contact_enrichment (dedup_key);
create index if not exists ix_contact_enrichment_status  on public.contact_enrichment (status);
create index if not exists ix_contact_enrichment_resolved on public.contact_enrichment (resolved_at);

alter table public.contact_enrichment enable row level security;
-- No policies: the edge function uses the service role (bypasses RLS); anon/auth get nothing.
