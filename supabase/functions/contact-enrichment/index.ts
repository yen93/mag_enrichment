// contact-enrichment
// ONE shared enrichment service for every lead type. Finds a contact's EMAIL (and phone
// when it comes free in the same lookup) from their name + company, via Hunter.io (main)
// -> Lusha V3 (backup). Backed by the public.contact_enrichment table, which is both a
// QUEUE and a CACHE: the same person+company is only ever paid for once.
//
// Two ways to use it:
//
// 1) PARAM CALL (what other lead-type automations use) - enrich one contact on demand:
//      POST .../contact-enrichment/lookup           (or ?lookup=1)
//      header: x-enrich-secret: <ENRICH_SHARED_SECRET>
//      body:   { "firstName":"Jane", "lastName":"Doe",
//                "companyName":"Acme Pty Ltd", "companyDomain":"acme.com",   // domain optional
//                "sourceTable":"my_leads", "sourceId":123, "writeBack":true } // optional
//      returns: { status:"done"|"not_found", email, phone, provider, cached:true|false }
//    Cache is checked first, so a repeat contact costs no credits. If sourceTable is in
//    the WRITEBACK registry and writeBack!==false, email/phone are written to that row too.
//
// 2) CRON DRAIN (batch) - default path, triggered by pg_cron:
//      - enqueues the approved alternate_leads (status='Send intro', no email, unprocessed)
//      - processes up to ENRICH_MAX_PER_RUN pending rows (cache -> providers)
//      - writes results back to each source row via the WRITEBACK registry
//    Any automation can also just INSERT a pending contact_enrichment row itself (name +
//    company + its source_table/source_id); the next drain picks it up.
//
// Debug params: ?dry=1 (preview: what would be enqueued/drained, ZERO provider calls, ZERO
// writes), ?sync=1 (await the drain and return the summary), ?limit=N (lower the drain cap),
// ?enqueueonly=1 (enqueue pending rows but do NOT call providers).
//
// Credit safety: Hunter FREE is ~50/month and that quota is now SHARED across all lead
// types. ENRICH_MONTHLY_CAP (default 50) is a hard ceiling on real provider lookups per
// calendar month; cache hits are free and never counted.

import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

declare const EdgeRuntime: { waitUntil(p: Promise<unknown>): void } | undefined;

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const HUNTER_API_KEY = Deno.env.get("HUNTER_IO_API_KEY") ?? "";
const LUSHA_API_KEY = Deno.env.get("LUSHA_API_KEY") ?? "";

// Shared secret for the on-demand param call. If unset, the /lookup endpoint is disabled
// (the cron drain still works) so credits can't be spent by an unauthenticated caller.
const ENRICH_SHARED_SECRET = Deno.env.get("ENRICH_SHARED_SECRET") ?? "";

const ENRICH_MAX_PER_RUN = parseInt(Deno.env.get("ENRICH_MAX_PER_RUN") ?? "10", 10); // provider lookups per drain
const ENRICH_MONTHLY_CAP = parseInt(Deno.env.get("ENRICH_MONTHLY_CAP") ?? "50", 10); // hard provider ceiling / month
const HUNTER_MIN_SCORE = parseInt(Deno.env.get("HUNTER_MIN_SCORE") ?? "0", 10);
// Reuse a cached "done" result forever by default (0). Set >0 to re-enrich after N days.
const CACHE_MAX_AGE_DAYS = parseInt(Deno.env.get("ENRICH_CACHE_MAX_AGE_DAYS") ?? "0", 10);

const WALL_CLOCK_MS = parseInt(Deno.env.get("WALL_CLOCK_MS") ?? "150000", 10);
const WALL_CLOCK_RESERVE_MS = parseInt(Deno.env.get("WALL_CLOCK_RESERVE_MS") ?? "8000", 10);
const PROVIDER_TIMEOUT_MS = parseInt(Deno.env.get("PROVIDER_TIMEOUT_MS") ?? "20000", 10);

// ---------------------------------------------------------------------------
// WRITEBACK REGISTRY. Maps a source_table -> which columns to write results into. Adding a
// new lead type that wants its own row updated = add ONE entry here. A source_table NOT in
// this map still gets its result stored in contact_enrichment (the canonical record); only
// the write-back to the origin row is skipped.
// ---------------------------------------------------------------------------
interface Writeback { id: string; email: string; phone?: string; processed?: string }
const WRITEBACK: Record<string, Writeback> = {
  alternate_leads: { id: "id", email: "email", phone: "phone_number", processed: "is_processed" },
  // e.g. my_new_leads: { id: "id", email: "email", phone: "mobile", processed: "enriched" },
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const domainOf = (email: string) => (email.includes("@") ? email.split("@")[1].trim().toLowerCase() : "");

const FREEMAIL = new Set([
  "gmail.com", "googlemail.com", "yahoo.com", "yahoo.com.au", "ymail.com", "hotmail.com",
  "hotmail.com.au", "outlook.com", "outlook.com.au", "live.com", "live.com.au", "icloud.com",
  "me.com", "aol.com", "bigpond.com", "bigpond.net.au", "optusnet.com.au", "iinet.net.au",
  "protonmail.com", "proton.me", "gmx.com", "mail.com", "msn.com",
]);
const cleanDomain = (d: string | null | undefined): string | null => {
  const t = (d ?? "").trim().toLowerCase().replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/.*$/, "");
  if (!t || FREEMAIL.has(t)) return null;
  return t;
};

interface Attempt { ok: boolean; email: string | null; phone: string | null; score?: number | null; note?: string }
interface Resolved { email: string | null; phone: string | null; score: number | null; provider: string }

async function fetchT(url: string, init: RequestInit): Promise<Response> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), PROVIDER_TIMEOUT_MS);
  try { return await fetch(url, { ...init, signal: ctl.signal }); }
  finally { clearTimeout(t); }
}

// --- Hunter.io (main) - email + free phone_number in one response -----------
async function hunterFind(domain: string | null, company: string | null, first: string, last: string): Promise<Attempt> {
  if (!HUNTER_API_KEY) return { ok: false, email: null, phone: null, note: "no HUNTER_IO_API_KEY" };
  const qs = new URLSearchParams({ first_name: first, last_name: last });
  if (domain) qs.set("domain", domain);
  else if (company) qs.set("company", company);
  else return { ok: false, email: null, phone: null, note: "no domain/company" };
  try {
    const res = await fetchT(`https://api.hunter.io/v2/email-finder?${qs.toString()}`, {
      headers: { "X-API-KEY": HUNTER_API_KEY, Accept: "application/json" },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const code = body?.errors?.[0]?.code ?? res.status;
      return { ok: false, email: null, phone: null, note: `hunter ${code} ${String(body?.errors?.[0]?.details ?? "").slice(0, 120)}` };
    }
    const d = body?.data ?? {};
    const email = typeof d.email === "string" && d.email.trim() ? d.email.trim() : null;
    const score = typeof d.score === "number" ? d.score : null;
    const phone = typeof d.phone_number === "string" && d.phone_number.trim() ? d.phone_number.trim() : null;
    if (email && (score ?? 0) < HUNTER_MIN_SCORE) return { ok: true, email: null, phone, score, note: `below min score (${score})` };
    return { ok: true, email, phone, score };
  } catch (err) {
    return { ok: false, email: null, phone: null, note: `hunter fetch failed: ${err}` };
  }
}

// --- Lusha V3 (backup) - email only (phone is billed separately) ------------
async function lushaFind(domain: string | null, company: string | null, first: string, last: string): Promise<Attempt> {
  if (!LUSHA_API_KEY) return { ok: true, email: null, phone: null, note: "no LUSHA_API_KEY (backup not configured)" };
  const contact: Record<string, string> = { clientReferenceId: "1", firstName: first, lastName: last };
  if (company) contact.companyName = company;
  if (domain) contact.companyDomain = domain;
  if (!company && !domain) return { ok: false, email: null, phone: null, note: "no domain/company" };
  try {
    const res = await fetchT("https://api.lusha.com/v3/contacts/search-and-enrich", {
      method: "POST",
      headers: { api_key: LUSHA_API_KEY, "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ contacts: [contact], reveal: ["emails"] }),
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) return { ok: false, email: null, phone: null, note: `lusha ${res.status} ${JSON.stringify(body).slice(0, 120)}` };
    const r = (body?.results ?? [])[0] ?? {};
    if (r?.error?.code) return { ok: true, email: null, phone: null, note: `lusha result ${r.error.code}` };
    const emails: Record<string, unknown>[] = Array.isArray(r.emails) ? r.emails : [];
    const work = emails.find((e) => (e.type as string) === "work" && typeof e.email === "string" && (e.email as string).trim());
    const any = emails.find((e) => typeof e.email === "string" && (e.email as string).trim());
    const email = ((work ?? any)?.email as string | undefined)?.trim() ?? null;
    const phones: Record<string, unknown>[] = Array.isArray(r.phones) ? r.phones : [];
    const phone = (phones.find((p) => typeof p.number === "string" && (p.number as string).trim())?.number as string | undefined)?.trim() ?? null;
    return { ok: true, email, phone };
  } catch (err) {
    return { ok: false, email: null, phone: null, note: `lusha fetch failed: ${err}` };
  }
}

interface ProviderState { hunterDown: boolean; lushaDown: boolean; monthUsed: number }

// Run Hunter -> Lusha for one contact. Respects the monthly cap and per-run provider-down
// flags. Returns the resolved result plus whether a provider ERRORED (couldn't check).
async function callProviders(
  domain: string | null, company: string | null, first: string, last: string, st: ProviderState,
): Promise<{ result: Resolved; providerError: boolean; capped: boolean }> {
  const out: Resolved = { email: null, phone: null, score: null, provider: "" };
  let providerError = false;

  if (st.monthUsed >= ENRICH_MONTHLY_CAP) return { result: out, providerError: false, capped: true };

  if (!st.hunterDown) {
    const h = await hunterFind(domain, company, first, last);
    if (h.ok) {
      st.monthUsed++;
      if (h.email) { out.email = h.email; out.phone = h.phone; out.score = h.score ?? null; out.provider = "hunter"; }
      else if (h.phone) out.phone = h.phone;
    } else {
      providerError = true;
      if (/hunter (401|403|429)/.test(h.note ?? "")) { st.hunterDown = true; console.warn(`Hunter disabled this run: ${h.note}`); }
      else console.warn(`Hunter error: ${h.note}`);
    }
  }

  if (!out.email && !st.lushaDown && st.monthUsed < ENRICH_MONTHLY_CAP) {
    const l = await lushaFind(domain, company, first, last);
    if (l.ok) {
      st.monthUsed++;
      if (l.email) { out.email = l.email; out.phone = l.phone ?? out.phone; out.provider = "lusha"; }
    } else {
      providerError = true;
      if (/lusha (401|402|403)/.test(l.note ?? "")) { st.lushaDown = true; console.warn(`Lusha disabled this run: ${l.note}`); }
      else console.warn(`Lusha error: ${l.note}`);
    }
  }
  return { result: out, providerError, capped: false };
}

// Provider lookups already charged this calendar month (cache hits excluded).
async function monthUsage(db: SupabaseClient): Promise<number> {
  const start = new Date(); start.setUTCDate(1); start.setUTCHours(0, 0, 0, 0);
  const { count } = await db
    .from("contact_enrichment")
    .select("id", { count: "exact", head: true })
    .in("provider", ["hunter", "lusha"])
    .gte("resolved_at", start.toISOString());
  return count ?? 0;
}

const dedupKey = (first: string, last: string, domain: string | null, company: string | null) =>
  `${first.toLowerCase()}|${last.toLowerCase()}|${(domain || company || "").toLowerCase()}`.trim();

// Cache hit: a prior resolved result for the same person+company (optionally within the
// freshness window). Returns the cached email/phone or null.
async function cacheLookup(db: SupabaseClient, key: string, exceptId?: number): Promise<Resolved | null> {
  let q = db.from("contact_enrichment")
    .select("id, email, phone, email_score, resolved_at")
    .eq("dedup_key", key).eq("status", "done").not("email", "is", null)
    .order("resolved_at", { ascending: false }).limit(1);
  if (exceptId != null) q = q.neq("id", exceptId);
  const { data } = await q;
  const hit = (data ?? [])[0] as Record<string, unknown> | undefined;
  if (!hit) return null;
  if (CACHE_MAX_AGE_DAYS > 0 && hit.resolved_at) {
    const ageDays = (Date.now() - new Date(hit.resolved_at as string).getTime()) / 86400000;
    if (ageDays > CACHE_MAX_AGE_DAYS) return null;
  }
  return { email: (hit.email as string) ?? null, phone: (hit.phone as string) ?? null, score: (hit.email_score as number) ?? null, provider: "cache" };
}

// Write the resolved email/phone (and a processed flag) back to the origin row.
async function writeBack(db: SupabaseClient, sourceTable: string | null, sourceId: number | null, r: { email?: string | null; phone?: string | null; processed?: boolean }): Promise<void> {
  if (!sourceTable || sourceId == null) return;
  const reg = WRITEBACK[sourceTable];
  if (!reg) return;
  const upd: Record<string, unknown> = {};
  if (r.email && reg.email) upd[reg.email] = r.email;
  if (r.phone && reg.phone) upd[reg.phone] = r.phone;
  if (r.processed && reg.processed) upd[reg.processed] = true;
  if (Object.keys(upd).length === 0) return;
  const { error } = await db.from(sourceTable).update(upd).eq(reg.id, sourceId);
  if (error) console.warn(`writeBack(${sourceTable}#${sourceId}) failed: ${error.message}`);
}

// ---- Registered PULL source: approved alternate_leads (the Liv-gated batch) ----
// Enqueues pending contact_enrichment rows (idempotent). Domain comes from the original
// lead's email via thread_id. Other lead types instead PUSH rows or use the param call.
async function enqueueAlternateLeads(db: SupabaseClient, preview: boolean): Promise<number> {
  const { data: leads, error } = await db
    .from("alternate_leads")
    .select("id, poc_firstname, poc_lastname, org, thread_id")
    .eq("status", "Send intro").is("is_processed", null).is("email", null)
    .order("id", { ascending: true });
  if (error) { console.warn(`enqueueAlternateLeads query: ${error.message}`); return 0; }
  const rows = (leads ?? []) as Record<string, unknown>[];
  if (rows.length === 0) return 0;

  const threadIds = [...new Set(rows.map((r) => r.thread_id).filter(Boolean))] as string[];
  const domainByThread = new Map<string, string>();
  if (threadIds.length) {
    const { data: cst } = await db.from("closed_sequence_threads").select("thread_id, email").in("thread_id", threadIds);
    for (const r of (cst ?? []) as Record<string, unknown>[]) {
      const tid = r.thread_id as string, d = cleanDomain(domainOf((r.email as string) ?? ""));
      if (tid && d && !domainByThread.has(tid)) domainByThread.set(tid, d);
    }
  }
  const toInsert = rows.map((r) => ({
    poc_firstname: (r.poc_firstname as string) ?? null,
    poc_lastname: (r.poc_lastname as string) ?? null,
    company_name: (r.org as string) ?? null,
    company_domain: r.thread_id ? (domainByThread.get(r.thread_id as string) ?? null) : null,
    source_table: "alternate_leads",
    source_id: r.id as number,
    source_type: "alternate_lead",
    status: "pending",
  }));
  if (preview) return toInsert.length;
  // Idempotent: skip rows already enqueued for the same (source_table, source_id).
  const { error: insErr } = await db.from("contact_enrichment").upsert(toInsert, { onConflict: "source_table,source_id", ignoreDuplicates: true });
  if (insErr) { console.warn(`enqueueAlternateLeads insert: ${insErr.message}`); return 0; }
  return toInsert.length;
}

interface PendingRow {
  id: number; poc_firstname: string | null; poc_lastname: string | null;
  company_name: string | null; company_domain: string | null; source_table: string | null; source_id: number | null; attempts: number;
}

// Finalize one contact_enrichment row + write back to its origin.
async function finalize(db: SupabaseClient, row: PendingRow, status: string, r: Resolved, lastError?: string): Promise<void> {
  await db.from("contact_enrichment").update({
    status, email: r.email, phone: r.phone, email_score: r.score, provider: r.provider || null,
    attempts: row.attempts + 1, last_error: lastError ?? null, resolved_at: new Date().toISOString(),
  }).eq("id", row.id);
  if (status === "done" || status === "not_found") {
    await writeBack(db, row.source_table, row.source_id, { email: r.email, phone: r.phone, processed: true });
  }
}

// ---- Core drain: process pending rows (cache -> providers) -----------------
async function drain(db: SupabaseClient, limit: number, st: ProviderState, runStarted: number): Promise<Record<string, number>> {
  const cap = limit > 0 ? Math.min(limit, ENRICH_MAX_PER_RUN) : ENRICH_MAX_PER_RUN;
  const stats = { processed: 0, enriched: 0, cached: 0, phones: 0, notFound: 0, errored: 0, capped: 0 };

  // Pull a working set a bit larger than the cap (cache hits are cheap and shouldn't eat it).
  const { data, error } = await db.from("contact_enrichment")
    .select("id, poc_firstname, poc_lastname, company_name, company_domain, source_table, source_id, attempts")
    .eq("status", "pending").order("id", { ascending: true }).limit(cap * 4 + 20);
  if (error) { console.warn(`drain query: ${error.message}`); return stats; }
  const pending = (data ?? []) as PendingRow[];

  let providerCalls = 0;
  for (const row of pending) {
    if (Date.now() - runStarted > WALL_CLOCK_MS - WALL_CLOCK_RESERVE_MS) { console.warn("Wall-clock budget spent - stopping drain."); break; }
    const first = (row.poc_firstname ?? "").trim(), last = (row.poc_lastname ?? "").trim();
    const domain = cleanDomain(row.company_domain), company = (row.company_name ?? "").trim() || null;
    if (!first || !last) { await finalize(db, row, "error", { email: null, phone: null, score: null, provider: "" }, "missing name"); stats.errored++; continue; }

    const key = dedupKey(first, last, domain, company);
    const cached = await cacheLookup(db, key, row.id);
    if (cached) {
      await finalize(db, row, "done", cached);
      stats.processed++; stats.cached++; stats.enriched++; if (cached.phone) stats.phones++;
      continue;
    }

    if (providerCalls >= cap) break; // only provider lookups count against the per-run cap
    const { result, providerError, capped } = await callProviders(domain, company, first, last, st);
    if (capped) { stats.capped++; break; } // monthly ceiling hit - leave the rest pending
    providerCalls++;
    stats.processed++;
    if (result.email) { await finalize(db, row, "done", result); stats.enriched++; if (result.phone) stats.phones++; }
    else if (providerError) { // couldn't fully check -> leave pending, record the error
      await db.from("contact_enrichment").update({ attempts: row.attempts + 1, last_error: "provider error" }).eq("id", row.id);
      stats.errored++;
    } else { await finalize(db, row, "not_found", result); stats.notFound++; if (result.phone) stats.phones++; }
  }
  return stats;
}

// ---- Entry: cron enqueue + drain ------------------------------------------
async function runDrain(limit: number, opts?: { enqueueOnly?: boolean }): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
  const runStarted = Date.now();
  const enqueued = await enqueueAlternateLeads(db, false);
  if (opts?.enqueueOnly) return { enqueued, drained: null };
  const st: ProviderState = { hunterDown: false, lushaDown: false, monthUsed: await monthUsage(db) };
  const stats = await drain(db, limit, st, runStarted);
  return { enqueued, monthUsedBefore: st.monthUsed, monthlyCap: ENRICH_MONTHLY_CAP, hunterDown: st.hunterDown, lushaDown: st.lushaDown, ...stats };
}

// ---- Entry: param call (on-demand single enrichment) ----------------------
async function runLookup(body: Record<string, unknown>): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
  const first = String(body.firstName ?? body.poc_firstname ?? "").trim();
  const last = String(body.lastName ?? body.poc_lastname ?? "").trim();
  const company = (String(body.companyName ?? body.company_name ?? body.org ?? "").trim()) || null;
  const domain = cleanDomain(String(body.companyDomain ?? body.company_domain ?? ""));
  const sourceTable = (body.sourceTable ?? body.source_table) as string | null ?? null;
  const sourceId = body.sourceId != null ? Number(body.sourceId) : (body.source_id != null ? Number(body.source_id) : null);
  const doWriteBack = body.writeBack !== false;
  if (!first || !last) return { status: "error", error: "firstName and lastName are required" };
  if (!company && !domain) return { status: "error", error: "companyName or companyDomain is required" };

  const key = dedupKey(first, last, domain, company);
  const cached = await cacheLookup(db, key);
  let resolved: Resolved; let status: string; let providerError = false;
  if (cached) { resolved = cached; status = "done"; }
  else {
    const st: ProviderState = { hunterDown: false, lushaDown: false, monthUsed: await monthUsage(db) };
    const { result, providerError: pe, capped } = await callProviders(domain, company, first, last, st);
    if (capped) return { status: "error", error: "monthly enrichment cap reached", monthlyCap: ENRICH_MONTHLY_CAP };
    resolved = result; providerError = pe;
    status = result.email ? "done" : (pe ? "error" : "not_found");
  }

  // Log to the cache/audit table (and write back if the caller gave a source + opted in).
  try {
    await db.from("contact_enrichment").upsert({
      poc_firstname: first, poc_lastname: last, company_name: company, company_domain: domain,
      source_table: sourceTable, source_id: sourceId, source_type: (body.sourceType ?? body.source_type ?? "lookup") as string,
      email: resolved.email, phone: resolved.phone, email_score: resolved.score,
      provider: cached ? "cache" : (resolved.provider || null),
      status: status === "error" ? "pending" : status, attempts: 1, resolved_at: new Date().toISOString(),
    }, sourceTable && sourceId != null ? { onConflict: "source_table,source_id" } : undefined);
  } catch (e) { console.warn(`lookup log failed: ${e}`); }

  if (doWriteBack && (status === "done" || status === "not_found")) {
    await writeBack(db, sourceTable, sourceId, { email: resolved.email, phone: resolved.phone, processed: true });
  }
  return { status, email: resolved.email, phone: resolved.phone, provider: cached ? "cache" : resolved.provider || null, cached: Boolean(cached), providerError };
}

// ---- Dry preview: no provider calls, no writes ----------------------------
async function runDry(): Promise<Record<string, unknown>> {
  const db = createClient(SUPABASE_URL, SERVICE_ROLE_KEY, { auth: { persistSession: false } });
  const wouldEnqueue = await enqueueAlternateLeads(db, true);
  const { count: pending } = await db.from("contact_enrichment").select("id", { count: "exact", head: true }).eq("status", "pending");
  return {
    dry: true, wouldEnqueueAlternateLeads: wouldEnqueue, pendingNow: pending ?? 0,
    monthUsed: await monthUsage(db), monthlyCap: ENRICH_MONTHLY_CAP,
    hunterConfigured: Boolean(HUNTER_API_KEY), lushaConfigured: Boolean(LUSHA_API_KEY),
    lookupEnabled: Boolean(ENRICH_SHARED_SECRET), writebackTables: Object.keys(WRITEBACK),
  };
}

Deno.serve(async (req: Request) => {
  const url = new URL(req.url);
  const params = url.searchParams;
  const path = url.pathname;
  const limit = params.get("limit") ? parseInt(params.get("limit")!, 10) : 0;

  // Param call: POST /lookup (or ?lookup=1) with the shared secret.
  if (path.endsWith("/lookup") || params.get("lookup") === "1") {
    if (!ENRICH_SHARED_SECRET) return Response.json({ status: "error", error: "lookup disabled: ENRICH_SHARED_SECRET not set" }, { status: 503 });
    if (req.headers.get("x-enrich-secret") !== ENRICH_SHARED_SECRET) return Response.json({ status: "error", error: "unauthorized" }, { status: 401 });
    let body: Record<string, unknown> = {};
    try { body = await req.json(); } catch { /* empty */ }
    try { return Response.json(await runLookup(body)); }
    catch (err) { console.error(`runLookup crashed: ${err}`); return Response.json({ status: "error", error: String(err) }, { status: 500 }); }
  }

  if (params.get("dry") === "1") {
    try { return Response.json({ status: "done", ...(await runDry()) }); }
    catch (err) { return Response.json({ status: "error", error: String(err) }, { status: 500 }); }
  }

  const enqueueOnly = params.get("enqueueonly") === "1";
  if (params.get("sync") === "1" || enqueueOnly) {
    try { return Response.json({ status: "done", ...(await runDrain(limit, { enqueueOnly })) }); }
    catch (err) { console.error(`runDrain crashed: ${err}`); return Response.json({ status: "error", error: String(err), stack: (err as Error)?.stack }, { status: 500 }); }
  }

  const work = runDrain(limit).catch((err) => console.error(`runDrain crashed: ${err}`));
  if (typeof EdgeRuntime !== "undefined") EdgeRuntime.waitUntil(work);
  return new Response(JSON.stringify({ status: "started" }), { status: 202, headers: { "Content-Type": "application/json" } });
});
