#!/usr/bin/env node
/**
 * Minimal Workday myworkdayjobs scraper (public CXS JSON).
 * - Discovers correct /wday/cxs/<tenant>/<site> tokens from landing page
 * - Falls back to provided tenant/site if discovery fails
 * - Fetches listings with A/B/C variants (GET params; POST searchText; POST query)
 * - Optionally fetches details for description
 *
 * Output: JSON array to stdout
 */

import { fetch } from "undici";

const UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

function arg(name, def = null) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx === -1) return def;
  const v = process.argv[idx + 1];
  if (!v || v.startsWith("--")) return def;
  return v;
}

function flag(name) {
  return process.argv.includes(`--${name}`);
}

async function getText(url) {
  const res = await fetch(url, { headers: { "user-agent": UA, "accept": "text/html" } });
  if (!res.ok) return null;
  return { url: res.url, text: await res.text() };
}

function extractTokens(html) {
  // /wday/cxs/<tenant>/<site>
  let m = html.match(/\/wday\/cxs\/([^/]+)\/([^/"'?\s]+)/);
  if (m) return [m[1], m[2]];
  // escaped: \/wday\/cxs\/<tenant>\/<site>
  m = html.match(/\\\/wday\\\/cxs\\\/([^\\/]+)\\\/([^\\\"'?\s]+)/);
  if (m) return [m[1], m[2]];
  // absolute
  m = html.match(/https:\/\/[^\s"']+\/wday\/cxs\/([^/]+)\/([^/"'?\s]+)/);
  if (m) return [m[1], m[2]];
  m = html.match(/https:\\\/\\\/[^\s"']+\\\/wday\\\/cxs\\\/([^\\/]+)\\\/([^\\\"'?\s]+)/);
  if (m) return [m[1], m[2]];
  return null;
}

async function discover(host, site) {
  const candidates = [
    `https://${host}/${site}`,
    `https://${host}/en-US/${site}`,
    `https://${host}/en-us/${site}`,
  ];
  for (const u of candidates) {
    const got = await getText(u);
    if (!got) continue;
    const tokens = extractTokens(got.text);
    if (tokens) return { tenant: tokens[0], site: tokens[1], origin: new URL(got.url).origin };
  }
  return null;
}

async function fetchJson(url, method = "GET", body = null) {
  const headers = {
    "user-agent": UA,
    "accept": "application/json,text/plain,*/*",
    "accept-language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    ...(body ? { "content-type": "application/json", "x-requested-with": "XMLHttpRequest" } : {}),
  };
  const res = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  let data = null;
  try { data = JSON.parse(text); } catch { /* ignore */ }
  return { ok: res.ok, status: res.status, data, text: text.slice(0, 500) };
}

async function getPage(listUrl, offset, limit, searchText) {
  // A) GET params
  const params = new URLSearchParams({ offset: String(offset), limit: String(limit), searchText });
  let r = await fetchJson(`${listUrl}?${params.toString()}`, "GET");
  if (r.ok && r.data && r.data.jobPostings) return r.data;

  // B) POST with searchText
  r = await fetchJson(listUrl, "POST", { appliedFacets: {}, searchText, limit, offset });
  if (r.ok && r.data && r.data.jobPostings) return r.data;

  // C) POST with query
  r = await fetchJson(listUrl, "POST", { appliedFacets: {}, query: searchText, limit, offset });
  if (r.ok && r.data && r.data.jobPostings) return r.data;

  // return most informative error
  const best = r;
  const msg = best.data ? JSON.stringify(best.data) : best.text;
  throw new Error(`Workday jobs fetch failed HTTP ${best.status} ${listUrl} :: ${msg}`);
}

function detailUrl(origin, tenant, site, externalPath) {
  if (!externalPath || !externalPath.includes("/job/")) return null;
  const slug = externalPath.split("/job/")[1].replace(/^\/+/, "");
  if (!slug) return null;
  return `${origin}/wday/cxs/${tenant}/${site}/job/${slug}`;
}

async function mapLimit(items, limit, fn) {
  const ret = [];
  let i = 0;
  async function worker() {
    while (true) {
      const idx = i++;
      if (idx >= items.length) return;
      ret[idx] = await fn(items[idx], idx);
    }
  }
  const n = Math.max(1, limit);
  await Promise.all(Array.from({ length: n }, worker));
  return ret;
}

async function main() {
  const company = arg("company", "");
  const host = arg("host");
  const tenantIn = arg("tenant");
  const siteIn = arg("site");
  const searchText = arg("searchText", "");
  const pageSize = parseInt(arg("pageSize", "50"), 10);
  const maxPages = parseInt(arg("maxPages", "20"), 10);
  const fetchDetails = arg("fetchDetails", "true") !== "false";
  const detailConcurrency = parseInt(arg("detailConcurrency", "6"), 10);

  if (!host || !tenantIn || !siteIn) {
    console.error("Missing required args: --host --tenant --site");
    process.exit(2);
  }

  const discovered = await discover(host, siteIn);
  const tenant = discovered?.tenant ?? tenantIn;
  const site = discovered?.site ?? siteIn;
  const origin = discovered?.origin ?? `https://${host}`;

  const base = `${origin}/wday/cxs/${tenant}/${site}`;
  const listUrl = `${base}/jobs`;

  const jobs = [];
  let offset = 0;
  for (let p = 0; p < maxPages; p++) {
    const page = await getPage(listUrl, offset, pageSize, searchText);
    const postings = page.jobPostings ?? [];
    if (!postings.length) break;
    jobs.push(...postings);
    if (postings.length < pageSize) break;
    offset += pageSize;
  }

  let details = [];
  if (fetchDetails) {
    details = await mapLimit(jobs, detailConcurrency, async (j) => {
      const ext = j.externalPath || "";
      const url = detailUrl(origin, tenant, site, ext);
      if (!url) return null;
      const r = await fetchJson(url, "GET");
      if (!r.ok || !r.data) return null;
      return { externalPath: ext, detail: r.data };
    });
  }

  const detailByPath = new Map();
  for (const d of details) {
    if (d && d.externalPath) detailByPath.set(d.externalPath, d.detail);
  }

  const out = jobs.map((j) => {
    const ext = j.externalPath || "";
    const det = detailByPath.get(ext) || null;
    let desc = "";
    if (det && det.jobPostingInfo) {
      desc = det.jobPostingInfo.jobDescription || det.jobPostingInfo.externalDescription || "";
    }
    if (!desc && det) desc = det.jobDescription || "";
    return {
      company,
      title: j.title || j.externalTitle || j.postedTitle || "",
      postedOn: j.postedOn || j.postedDate || null,
      externalPath: ext,
      locationsText: j.locationsText || null,
      url: ext ? `${origin}${ext}` : `${origin}/${site}`,
      description: desc,
      raw: { posting: j, detail: det },
    };
  });

  process.stdout.write(JSON.stringify({ discovered: { tenant, site, origin }, jobs: out }));
}

main().catch((e) => {
  console.error(String(e?.stack || e));
  process.exit(1);
});
