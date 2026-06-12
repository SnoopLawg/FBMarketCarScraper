---
name: deploy-verify
description: Ship a code change to the carscraper production server and PROVE it's live. Commit → push → wait for CI → pull → force-recreate → verify the new code is actually in the running container → health-check. Use after any code change destined for mothership2.
---

# Deploy & Verify

Ships the current branch to production (mothership2 → :5001) and verifies it
end-to-end. Encodes the hard-won gotchas: the Watchtower / stale-`:latest`
race bit us repeatedly (the running container had OLD code twice despite a
"successful" deploy), so this skill **always force-recreates and verifies the
new code is actually running** before declaring success.

## Preconditions
- Changes committed-worthy (run the relevant tests first: `python -m pytest -q`).
- Server reachable: `ssh mothership2 'echo ok'` (Tailscale; falls back to LAN
  192.168.0.244). If unreachable, stop and tell the user.

## Steps

### 1. Test, commit, push
```bash
source venv/bin/activate && python -m pytest -q   # must pass
git add <files> && git commit -m "<msg>"          # branch first if on main is disallowed; this repo deploys from main
git push origin main
```

### 2. Wait for CI to build the image
```bash
RUN=$(gh run list --limit 1 --json databaseId -q '.[0].databaseId')
gh run watch $RUN --exit-status; gh run view $RUN --json conclusion -q .conclusion
```
Must be `success` before deploying — otherwise GHCR still has the old image.

### 3. Pull + FORCE-RECREATE on the server
```bash
ssh mothership2 'cd /home/snoop/docker/carscraper && \
  docker compose pull carscraper && \
  docker compose up -d --force-recreate carscraper'
```
**Always `--force-recreate`.** Plain `up -d` often no-ops when Watchtower
already pulled or the digest looks unchanged, leaving old code running.

### 4. CRITICAL — verify the new code is in the running container
Grep the running container for a string unique to this change BEFORE trusting
the deploy:
```bash
ssh mothership2 'docker exec carscraper grep -c "<unique-string-from-your-diff>" /app/<changed-file>'
```
Must return ≥1. **If it returns 0, the registry image is stale or Watchtower
re-pulled an older tag** — re-run step 3, and if it persists, check
`gh run list` to confirm you watched the RIGHT run (a later commit's CI may
still be in flight). Do not report success until this grep passes.

### 5. Wait for the app to serve (startup is slow)
Startup runs find_deals + sell-car valuations (Selenium) before Flask binds —
allow up to ~3 minutes.
```bash
ssh mothership2 'for i in $(seq 1 50); do c=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/api/health); [ "$c" = "200" ] && { echo "UP after ~$((i*6))s"; break; }; sleep 6; done'
```

### 6. (Optional) verify behavior
Hit the route the change affects, or run a one-off `docker exec ... python3 -`
check against the live DB, and report the observed result.

## Notes
- The data volume (`/data`) survives recreation — DB, Config.json, cookies,
  favorites/deleted files persist.
- Compose changes (new sidecar, env, mem_limit) live in sibling repo
  `../homelab/carscraper/`; the server copy at `/home/snoop/docker/carscraper/`
  is a manual copy (not git) — `cat local-compose | ssh mothership2 'cat > .../docker-compose.yml'` then `docker compose up -d`.
- Never `cd` into other dirs in local Bash (permission prompts); use absolute
  paths and `ssh mothership2 'cd ... && ...'` for server-side.
- Report faithfully: only say "deployed and live" once steps 4 AND 5 pass.
