#!/usr/bin/env node
// External 24/7 prober for the Perchance Status Monitor.
//
// The page can only measure uptime while a tab is open (even in the background,
// browsers throttle timers). This small Node script probes the same HTTP
// endpoints the page probes, folds each run into rolling buckets, and writes a
// JSON feed the page ingests to fill the minutes nobody was watching.
//
// It intentionally skips the two checks that are only possible inside a real
// browser — `aigen` (a live text-generation call), `t2igen` (a live image-generation call) and `kv` (an actual IndexedDB
// roundtrip). The page's coverage metric excludes those too, so the numbers line
// up: the prober covers exactly the externally-observable surface.
//
// Usage:
//   node probe.mjs <store.json>
//
// Environment:
//   PROBER_NAME          label shown on the page           (default "external")
//   PROBER_REGION         location label                    (default "")
//   PROBER_INTERVAL_SEC   seconds between runs, for the page (default 300)
//
// The feed schema is the same one index.html's external-feed section consumes
// (see src/prober/README.md for the full contract):
//   { kind, v, prober, region, generatedAt, intervalSec,
//     b: { key: [ {m,n,ok,sum,max} ] },   // 1-minute buckets, last 24h
//     h: { key: [ {m,n,ok,sum,max} ] },   // hourly rollups,  last 7d
//     d: { key: [ {m,n,ok,sum,max} ] } }  // daily rollups,   last 120d
//   bucket.m is the bucket start (ms); n counts probes; ok counts responses
//   received (a reachable 5xx still counts here, exactly like the page's
//   recordProbe). The one exception is HTTP 403: that is the edge firewall
//   blocking this prober (bot rules), not the service answering, so it is
//   logged as `blocked` and counts as a failed sample. On the page that can
//   only ever read as one vantage point dissenting, never a confirmed outage.
//   sum/max are latency sum and peak. Re-running adds one more
//   sample to the current minute, so the file is cumulative, not a one-shot.

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";

export const BUCKET_MS = 60000;
export const HOUR_MS = 3600000;
export const DAY_MS = 86400000;
export const KEEP_B = 1440; // 24h of minutes
export const KEEP_H = 168;  // 7d of hours
export const KEEP_D = 120;  // 120d of daily rollups

// Mirrors `psServices` in index.html. `expect` is the status a healthy service
// returns (some endpoints answer 400/404 by design when probed bare) and is used
// only for logging; the page compares against its own copy.
export const ENDPOINTS = [
  { key: "home",     url: "https://perchance.org/",                                                          expect: 200 },
  { key: "upload",   url: "https://upload.perchance.org/embed",                                              expect: 200 },
  { key: "filehost", url: "https://user.uploads.dev",                                                        expect: 400 },
  { key: "editable", url: "https://editable.uploads.dev",                                                    expect: 404 },
  { key: "aiplugin", url: "https://perchance.org/api/getGeneratorsAndDependencies?generatorNames=ai-text-plugin",         expect: 200 },
  { key: "t2i",      url: "https://perchance.org/api/getGeneratorsAndDependencies?generatorNames=text-to-image-plugin",   expect: 200 },
  { key: "gallery",  url: "https://image-generation.perchance.org/gallery?channel=animal&sort=recent&timeRange=all-time&contentFilter=g", expect: 200 },
  { key: "server",   url: "https://perchance.org/api/getGeneratorsAndDependencies?generatorNames=server-plugin",          expect: 200 },
  { key: "comments", url: "https://perchance.org/api/getGeneratorsAndDependencies?generatorNames=comments-plugin",        expect: 200 },
  { key: "stats",    url: "https://perchance.org/api/getGeneratorStats?generatorName=animal",                 expect: 200 },
  { key: "search",   url: "https://perchance.org/search?q=cat",                                              expect: 200 },
  // The editor's Save button backend. An empty POST can never overwrite
  // anything; a structured {"status": ...} rejection proves the handler is up.
  { key: "save",     url: "https://perchance.org/api/save", method: "POST", body: "{}", match: '"status"',   expect: 200 },
];

const TIMEOUT_MS = 15000;
const PROBE_GAP_MS = 300;
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

function bucketStart(t, size) { return Math.floor(t / size) * size; }

// Fold one probe into a bucket store, appending to the newest bucket if it is
// for the same period (same contract as the page's histAdd).
export function fold(store, key, m, reached, ms, keep) {
  const arr = store[key] || (store[key] = []);
  let b = arr.length ? arr[arr.length - 1] : null;
  if (!b || b.m !== m) {
    b = { m: m, n: 0, ok: 0, sum: 0, max: 0 };
    arr.push(b);
  }
  b.n++;
  if (reached) {
    b.ok++;
    b.sum += ms;
    if (ms > b.max) b.max = ms;
  }
  if (arr.length > keep) arr.splice(0, arr.length - keep);
}

export function prune(store, keep) {
  Object.keys(store).forEach((k) => {
    const arr = store[k];
    if (Array.isArray(arr) && arr.length > keep) arr.splice(0, arr.length - keep);
  });
}

// One HTTP probe. Reachability (a response of any status but a 403 edge block)
// is what the buckets record; classification (down / degraded / blocked / ok)
// drives the log line only, and
// mirrors classifyObserved() on the page. Endpoints may specify a non-GET
// `method` with a `body`, plus a `match` substring the body must contain — a
// reachable reply with the wrong body (proxy error page, captcha wall) counts
// as down, exactly like the page treats a probe whose answer won't parse.
// A 403 means the edge firewall blocked this prober (bot rules), so it records
// a failed sample with state `blocked` instead of a false ok.
export async function probeOne(ep) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ep.timeoutMs || TIMEOUT_MS);
  const t0 = Date.now();
  try {
    const init = {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 perchance-status-prober/1 (+https://perchance.org/server-status)",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
      },
    };
    if (ep.method) init.method = ep.method;
    if (ep.body !== undefined) {
      init.body = ep.body;
      init.headers["content-type"] = "application/json";
    }
    const res = await fetch(ep.url, init);
    if (res.status === 403) {
      await res.arrayBuffer().catch(() => {});
      return { reachable: false, blocked: true, status: 403, error: "blocked by edge (HTTP 403)", ms: Date.now() - t0 };
    }
    let bodyOk = true;
    if (ep.match) {
      const text = await res.text().catch(() => "");
      bodyOk = text.includes(ep.match);
    } else {
      await res.arrayBuffer().catch(() => {});
    }
    return { reachable: bodyOk, status: res.status, ms: Date.now() - t0, wrongBody: !bodyOk };
  } catch (e) {
    return { reachable: false, error: e && e.name === "AbortError" ? "timeout" : String((e && e.message) || e), ms: Date.now() - t0 };
  } finally {
    clearTimeout(timer);
  }
}

export function classify(r) {
  if (r.blocked) return "blocked";
  if (!r.reachable) return "down";
  if (r.status >= 500) return "degraded";
  return "ok";
}

export async function run(store, opts = {}) {
  const now = opts.now || Date.now();
  const mb = bucketStart(now, BUCKET_MS);
  const hb = bucketStart(now, HOUR_MS);
  const db = bucketStart(now, DAY_MS);
  const results = [];
  for (let i = 0; i < ENDPOINTS.length; i++) {
    if (i) await sleep(PROBE_GAP_MS);
    const ep = ENDPOINTS[i];
    const r = await probeOne(ep);
    const ms = r.reachable ? r.ms : null;
    const state = classify(r);
    fold(store.b, ep.key, mb, r.reachable, r.ms || 0, KEEP_B);
    fold(store.h, ep.key, hb, r.reachable, r.ms || 0, KEEP_H);
    fold(store.d, ep.key, db, r.reachable, r.ms || 0, KEEP_D);
    results.push({ key: ep.key, state, status: r.status != null ? r.status : null, ms, error: r.wrongBody ? "unexpected body" : (r.error || null) });
  }
  prune(store.b, KEEP_B);
  prune(store.h, KEEP_H);
  prune(store.d, KEEP_D);
  return results;
}

export function emptyStore() { return { b: {}, h: {}, d: {} }; }

export function loadStore(path) {
  const store = emptyStore();
  try {
    const prev = JSON.parse(readFileSync(path, "utf8"));
    for (const k of ["b", "h", "d"]) {
      if (prev && prev[k] && typeof prev[k] === "object") store[k] = prev[k];
    }
  } catch (e) { /* first run, or unreadable file — start fresh */ }
  return store;
}

export function buildFeed(store, opts = {}) {
  return {
    kind: "perchance-status-probes",
    v: 1,
    prober: opts.prober || "external",
    region: opts.region || "",
    generatedAt: opts.now || Date.now(),
    intervalSec: opts.intervalSec || 300,
    b: store.b,
    h: store.h,
    d: store.d,
  };
}

// --- Hosted badge SVGs ------------------------------------------------------
// The page's badge gallery is a live snapshot, so a badge pasted into a README
// is frozen the moment you copy it. Every run therefore also writes standalone
// SVG badges next to the feed, which a README can point at *by URL* so it stays
// current on its own:
//
//   https://raw.githubusercontent.com/<you>/<repo>/prober-data/data/badges/overall.svg
//
// The look deliberately mirrors buildBadgeSvg()/badgeSpec() in index.html (same
// geometry, palette and wording) — keep the two in sync if you restyle one.
const BADGE_HEX  = { ok: "#2fce6a", slow: "#e0a800", degraded: "#e0a800", down: "#e5484d", checking: "#8a94a6", blocked: "#e0a800" };
const BADGE_WORD = { ok: "operational", slow: "slow", degraded: "degraded", down: "down", checking: "checking", blocked: "blocked" };
const SVC_SHORT  = { home: "Home", upload: "Upload", filehost: "Files", editable: "Edit", aiplugin: "AI API", t2i: "Image", gallery: "Gallery", server: "Server", comments: "Cmts", stats: "Stats", search: "Search", save: "Save" };
const BADGE_SPARK_N = 46;
const BADGE_GRAPH_W = 84;
let badgeUid = 0;

function badgeTextW(s) { return Math.round(String(s).length * 6.9) + 14; }
function badgeEsc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}
function badgeUptime(store, key) {
  const arr = store.b[key] || [];
  const from = Date.now() - DAY_MS;
  let n = 0, ok = 0;
  for (const b of arr) if (b.m + BUCKET_MS > from) { n += b.n; ok += b.ok; }
  return n ? { pct: (ok / n) * 100, n } : null;
}
function fmtUptime(pct) {
  if (pct === null || pct === undefined) return "—";
  if (pct >= 99.995) return "100%";
  return pct.toFixed(2) + "%";
}
function badgeUptimeHex(pct) {
  if (pct === null || pct === undefined) return BADGE_HEX.checking;
  return pct >= 99 ? BADGE_HEX.ok : (pct >= 95 ? BADGE_HEX.degraded : BADGE_HEX.down);
}
function badgeSparkSegments(arr, w, h, pad) {
  pad = (pad === undefined) ? 3 : pad;
  let mn = Infinity, mx = -Infinity;
  for (const v of arr) { if (v === null || v === undefined || isNaN(v)) continue; if (v < mn) mn = v; if (v > mx) mx = v; }
  if (!isFinite(mn)) return [];
  if (mx - mn < 1) mx = mn + 1;
  const innerW = w - pad * 2, innerH = h - pad * 2, n = arr.length;
  const segs = []; let cur = [];
  for (let i = 0; i < n; i++) {
    const v = arr[i];
    if (v === null || v === undefined || isNaN(v)) { if (cur.length) { segs.push(cur); cur = []; } continue; }
    const x = pad + (n < 2 ? innerW / 2 : (i / (n - 1)) * innerW);
    const y = pad + innerH - ((v - mn) / (mx - mn)) * innerH;
    cur.push((Math.round(x * 10) / 10) + "," + (Math.round(y * 10) / 10));
  }
  if (cur.length) segs.push(cur);
  return segs;
}
function badgeSvg(spec) {
  const label = spec.label, value = (spec.value === null || spec.value === undefined) ? "" : String(spec.value);
  const spark = spec.spark || null;
  const useSpark = !!(spark && spark.filter((v) => v !== null && v !== undefined && !isNaN(v)).length >= 2);
  const lw = badgeTextW(label);
  const rw = useSpark ? BADGE_GRAPH_W : badgeTextW(value);
  const w = lw + rw, hgt = 20;
  const uid = "b" + (++badgeUid);
  const title = spec.alt || (label + ": " + value);
  let svg = '<svg xmlns="http://www.w3.org/2000/svg" width="' + w + '" height="' + hgt + '" role="img" aria-label="' + badgeEsc(title) + '">' +
    '<title>' + badgeEsc(title) + '</title>' +
    '<linearGradient id="bs' + uid + '" x2="0" y2="100%"><stop offset="0" stop-color="#bbb" stop-opacity="0.1"/><stop offset="1" stop-opacity="0.1"/></linearGradient>' +
    '<clipPath id="bc' + uid + '"><rect width="' + w + '" height="' + hgt + '" rx="3" fill="#fff"/></clipPath>' +
    '<g clip-path="url(#bc' + uid + ')">' +
    '<rect width="' + lw + '" height="' + hgt + '" fill="#555"/>' +
    '<rect x="' + lw + '" width="' + rw + '" height="' + hgt + '" fill="' + spec.color + '"/>' +
    '<rect width="' + w + '" height="' + hgt + '" fill="url(#bs' + uid + ')"/>' +
    '</g>' +
    '<g fill="#fff" text-anchor="middle" font-family="Verdana,Geneva,DejaVu Sans,sans-serif" font-size="11">' +
    '<text x="' + (lw / 2) + '" y="15" fill="#010101" fill-opacity="0.3">' + badgeEsc(label) + '</text>' +
    '<text x="' + (lw / 2) + '" y="14">' + badgeEsc(label) + '</text>';
  if (useSpark) {
    for (const seg of badgeSparkSegments(spark, rw, hgt, 4)) {
      if (seg.length === 1) {
        const one = seg[0].split(",");
        svg += '<circle cx="' + (lw + Number(one[0])) + '" cy="' + one[1] + '" r="1.5" fill="#fff" fill-opacity="0.9"/>';
      } else {
        const pts = seg.map((p) => { const a = p.split(","); return (lw + Number(a[0])) + "," + a[1]; });
        svg += '<polyline fill="none" stroke="#fff" stroke-opacity="0.92" stroke-width="1.3" stroke-linejoin="round" points="' + pts.join(" ") + '"/>';
      }
    }
  } else {
    svg += '<text x="' + (lw + rw / 2) + '" y="15" fill="#010101" fill-opacity="0.3">' + badgeEsc(value) + '</text>' +
           '<text x="' + (lw + rw / 2) + '" y="14">' + badgeEsc(value) + '</text>';
  }
  return svg + '</g></svg>';
}

// A file name per variant id ("svc:home" -> "svc-home.svg", etc).
export function badgeSlug(id) { return String(id).replace(/[^a-z0-9]+/gi, "-").toLowerCase(); }

// Build every badge file from this run's outcomes plus the rolling buckets
// (for the 24h uptime figures and the homepage latency trend).
export function badgeSet(store, results) {
  const byKey = {};
  for (const r of results) byKey[r.key] = r;
  const stateOf = (k) => (byKey[k] ? byKey[k].state : "checking");
  const short = (k) => String(SVC_SHORT[k] || k).toLowerCase();
  const hexOf = (st) => BADGE_HEX[st] || BADGE_HEX.checking;

  let up = 0;
  const tot = ENDPOINTS.length;
  for (const ep of ENDPOINTS) if (stateOf(ep.key) === "ok" || stateOf(ep.key) === "slow") up++;
  const servicesCode = up === tot ? "ok" : (up === 0 ? "down" : "degraded");

  const rank = { ok: 0, slow: 1, degraded: 2, down: 3, blocked: 2 };
  let worst = "ok";
  for (const ep of ENDPOINTS) if (rank[stateOf(ep.key)] > rank[worst]) worst = stateOf(ep.key);

  let sum = 0, n = 0;
  for (const ep of ENDPOINTS) { const u = badgeUptime(store, ep.key); if (u && u.n >= 5) { sum += u.pct; n++; } }
  const avg = n ? sum / n : null;

  const homeMs = (byKey.home && !byKey.home.error) ? byKey.home.ms : null;
  const homeHex = homeMs === null ? BADGE_HEX.checking : (homeMs <= 1200 ? BADGE_HEX.ok : homeMs <= 3000 ? BADGE_HEX.degraded : BADGE_HEX.down);
  const spark = (store.b.home || []).slice(-BADGE_SPARK_N).map((b) => (b.ok > 0 ? Math.round(b.sum / b.ok) : null));

  const out = {};
  const add = (id, spec) => { out[badgeSlug(id)] = badgeSvg(spec); };
  add("overall",  { label: "perchance",          value: BADGE_WORD[worst], color: hexOf(worst), alt: "Perchance overall status" });
  add("services", { label: "perchance services", value: up + "/" + tot + " up", color: BADGE_HEX[servicesCode], alt: "Services reachable: " + up + " of " + tot });
  add("uptime",   { label: "perchance uptime",   value: avg === null ? "no data" : fmtUptime(avg) + " 24h", color: badgeUptimeHex(avg), alt: "Average 24h uptime across all services" });
  add("latency",  { label: "perchance latency",  value: homeMs === null ? "no data" : homeMs + "ms", color: homeHex, alt: "Homepage response time" });
  add("graph",    { label: "perchance",          value: "collecting", color: hexOf(worst), alt: "Homepage latency trend", spark: spark });
  for (const ep of ENDPOINTS) {
    const st = stateOf(ep.key);
    add("svc:" + ep.key, { label: "perchance " + short(ep.key), value: BADGE_WORD[st], color: hexOf(st), alt: ep.key + " - " + BADGE_WORD[st] });
    const u = badgeUptime(store, ep.key);
    const pct = (u && u.n >= 5) ? u.pct : null;
    add("up:" + ep.key,  { label: "perchance " + short(ep.key) + " uptime", value: pct === null ? "no data" : fmtUptime(pct) + " 24h", color: badgeUptimeHex(pct), alt: ep.key + " 24h uptime" });
  }
  return out;
}

async function main() {
  const outPath = process.argv[2] || "probes.json";
  const store = loadStore(outPath);
  const results = await run(store);
  const feed = buildFeed(store, {
    prober: process.env.PROBER_NAME || "external",
    region: process.env.PROBER_REGION || "",
    intervalSec: Number(process.env.PROBER_INTERVAL_SEC) || 300,
  });
  mkdirSync(dirname(outPath), { recursive: true });
  writeFileSync(outPath, JSON.stringify(feed));

  // Standalone SVG badges, so a README can link a URL that updates itself.
  const badges = badgeSet(store, results);
  const badgeDir = join(dirname(outPath), "badges");
  mkdirSync(badgeDir, { recursive: true });
  for (const name of Object.keys(badges)) writeFileSync(join(badgeDir, name + ".svg"), badges[name]);

  const bytes = Buffer.byteLength(JSON.stringify(feed));
  const down = results.filter((r) => r.state === "down").length;
  const degraded = results.filter((r) => r.state === "degraded").length;
  const blocked = results.filter((r) => r.state === "blocked").length;
  console.log(`[prober] ${results.length} probes — ${results.length - down - degraded - blocked} ok, ${degraded} degraded, ${down} down${blocked ? `, ${blocked} blocked` : ""} (${bytes} bytes) -> ${outPath}`);
  console.log(`[prober] ${Object.keys(badges).length} badge SVGs -> ${badgeDir}/`);
  results.forEach((r) => console.log(`  ${r.state === "ok" ? "OK  " : r.state === "degraded" ? "DEGR" : r.state === "blocked" ? "BLKD" : "DOWN"} ${r.key.padEnd(9)} ${r.ms}ms${r.error ? " " + r.error : ""} (HTTP ${r.status})`));
  // Always exit 0: the recorded data — including outages — is the product.
}

main().catch((e) => { console.error("[prober] fatal:", e); process.exit(1); });
