---
name: source-doctor
description: Diagnose a failing or zero-yield scraper source and apply the known remedy. Operationalizes the per-source failure knowledge (Cloudflare/Akamai → FlareSolverr, KSL PerimeterX → IP block, Facebook → session/login). Use when /api/health shows a source degraded or a scrape run is failing.
---

# Source Doctor

Diagnoses why a source is failing and applies the known fix, instead of
re-deriving it each time. Each source has a characteristic failure mode.

Runs against the production server (`ssh mothership2`, container `carscraper`).

## 1. Triage — which sources are failing?
```bash
cat <<'EOF' | ssh mothership2 'docker exec -i -e PYTHONPATH=/app carscraper python3 -'
from database import Database
db=Database(); db.open()
for r in db.cur.execute("SELECT source,status,listings_found,started_at FROM scrape_runs WHERE started_at>datetime('now','-2 days') ORDER BY started_at DESC LIMIT 20"):
    print(r["started_at"][:16], r["source"], r["status"], "found="+str(r["listings_found"]))
EOF
```
A source with `status=failed` or `found=0` against a non-trivial historical
average is broken.

## 2. Diagnose by source

**Facebook** (login-gated): check the c_user cookie / persistent profile.
0 listings usually = expired session. Remedies, in order: confirm
`FB_PROFILE_DIR`/`fb_cookies.pkl` present in `/data`; re-run
`bootstrap_fb_profile.py` (needs a human login); verify `FB_EMAIL`/
`FB_PASSWORD`/`FB_TOTP_SECRET` in the server `.env`.

**Cars.com / Autotrader** (Cloudflare / Akamai bot walls): both are solved by
FlareSolverr. Verify the sidecar is up (`docker ps | grep flaresolverr`) and
`FLARESOLVERR_URL` is set on the carscraper container. Test:
```bash
cat <<'EOF' | ssh mothership2 'docker exec -i -e PYTHONPATH=/app carscraper python3 -'
from flaresolverr import FlareSolverrClient
with FlareSolverrClient() as fs:
    h=fs.get("https://www.cars.com/", max_timeout_ms=60000) or ""
print("len",len(h),"ok" if len(h)>50000 else "BLOCKED")
EOF
```
If FlareSolverr itself is memory-bloated (it leaks ~20-25MB/req), restart it:
`ssh mothership2 'docker restart flaresolverr'`. For long batch jobs, chunk +
restart between chunks (a single session OOMs after ~40 fetches).

**KSL** (PerimeterX): a 403 with a ~1KB "Access denied" page on even the
homepage = the server IP is reputation-blocked (usually from too-aggressive
enrichment). FlareSolverr does NOT beat PerimeterX (press-and-hold captcha).
Remedy: wait it out (24-72h; the hardened 80/run pacing + circuit breaker
auto-resumes), or route KSL through a residential/mobile proxy (driver.py has
proxy support; KSL's requests.Session would need the proxy wired). Meanwhile,
VIN propagation (`propagate_titles_by_vin`) already recovers KSL titles whose
VIN twins on other sources are known.

## 3. Common cross-source remedies
- **Selenium/geckodriver version drift**: a warning, usually harmless.
- **Memory OOM** (FB invalidsessionidexception): the box is 15GB, carscraper
  capped at 3GB — check `docker stats`.
- **Silent zero-yield recorded as `completed`**: scraper_worker marks
  zero-vs-historical-average as `failed` — trust that signal.

## 4. After fixing
Trigger a scrape and confirm yield recovers:
`ssh mothership2 'curl -s -X POST http://localhost:5001/api/scrape'`, then
re-check `scrape_runs` after the run. Report what was broken, the remedy
applied, and the new yield.
