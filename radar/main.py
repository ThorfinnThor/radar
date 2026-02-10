
elif ats == "workday":
    tenant = c.get("tenant")
    wd_host = c.get("wd_host")  # e.g., wd1, wd3, wd5
    site = c.get("site")        # e.g., PfizerCareers
    if not tenant or not wd_host or not site:
        continue
    jobs = workday.fetch_jobs(tenant=tenant, site=site, wd_host=wd_host, limit=50, max_pages=20)
    for j in jobs:
        title = (j.get("title") or j.get("externalTitle") or "").strip()
        if any(k in title.lower() for k in keywords):
            sig = workday.normalize_job(j, account_name, tenant=tenant, site=site, wd_host=wd_host)
            db.insert_signal(conn, account_id, sig.signal_type, sig.source, sig.title, sig.evidence_url, sig.published_at, sig.payload)
            ingested += 1
