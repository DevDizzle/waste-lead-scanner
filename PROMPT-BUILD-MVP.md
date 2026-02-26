# PROMPT: Build Waste Lead Scanner MVP

You are building a permit-based lead generation tool for waste management companies. The project scaffolding is already in place at `/home/user/waste-lead-scanner/`. The full product spec is at `/home/user/.openclaw/workspace/specs/SPEC-WASTE-LEAD-SCANNER.md` — read it first for full context.

**Pilot client:** Arrow Waste Services — residential, commercial, and industrial waste hauling in Clayton County and Fulton County, Georgia.

## What's Already Done
- Project structure: `src/scanner/`, `src/enrichment/`, `src/scoring/`, `src/dashboard/`
- Config: `config/config.yaml` with Shovels API key, GCP project ID, Arrow client config (geo_ids, permit_tags, service_types)
- Requirements: `requirements.txt`
- Shovels API key is working and tested

## Shovels.ai API Reference

**Base URL:** `https://api.shovels.ai/v2`
**Auth:** `X-API-Key` header

**Key endpoints:**

1. `GET /permits/search` — Search permits by geo, date range, tags
   - Params: `geo_id`, `permit_from`, `permit_to`, `permit_tags` (repeatable), `property_type`, `size` (page size), `cursor` (pagination)
   - Returns: `{ items: [...], size: N, next_cursor: "..." }`
   
2. `GET /contractors/{id}` — Get contractor details by ID
   - Returns: name, business info

3. `GET /contractors/{id}/employees` — Get contractor employees (names, contact info)
   - Returns: paginated list of employees

4. `GET /counties/{geo_id}/metrics/current` — County-level permit activity stats

**Shovels geo_ids for Arrow's service area:**
- Fulton County, GA: `yjVD_3U4lG8`
- Clayton County, GA: `ygWg1SIWcvo`

**Permit tags relevant to waste (from config):**
`new_construction`, `demolition`, `addition`, `remodel`, `roofing`, `kitchen`, `bathroom`, `pool_and_hot_tub`, `grading`, `plumbing`, `electrical`, `hvac`

**IMPORTANT: Free tier is limited to 250 API calls total. Be conservative:**
- Use `size=50` for permit searches (fewer pagination calls)
- Cache contractor lookups (same contractor appears on many permits)
- Don't re-fetch data you already have in BigQuery
- For MVP, pull last 30 days of permits only

## What to Build

### 1. Scanner (`src/scanner/shovels.py`)
- Read client config from `config/config.yaml`
- For each geo_id in the client config, query `/permits/search` with:
  - `permit_from`: 30 days ago
  - `permit_to`: today
  - `permit_tags`: from client config (pass each tag individually — the API treats multiple tags as OR)
  - `size`: 50
  - Handle pagination via `next_cursor`
- Deduplicate permits by permit `id`
- Return list of raw permit dicts
- Log: total permits found, per-county counts, per-tag counts

### 2. Enrichment (`src/enrichment/contractors.py`)
- For each permit with a `contractor_id`, fetch contractor details via `/contractors/{id}`
- Cache contractors (many permits share the same contractor — don't waste API calls)
- Try to get employee info via `/contractors/{id}/employees` (best effort — may not always have data)
- Attach contractor name, employee names, and any contact info to the permit record
- Skip enrichment if contractor_id is null

### 3. Scoring (`src/scoring/scorer.py`)
Score each permit lead 1-10 based on:

| Factor | Weight | Logic |
|--------|--------|-------|
| **Waste Volume** | 40% | `new_construction`=10, `demolition`=10, `addition`=7, `remodel`=6, `roofing`=5, `kitchen`/`bathroom`=4, `grading`=8, `pool_and_hot_tub`=6, `plumbing`/`electrical`/`hvac`=2 |
| **Property Type** | 25% | `commercial`=10, `industrial`=10, `residential`=5, null=3 |
| **Recency** | 20% | Filed today=10, 7 days ago=7, 14 days=5, 21 days=3, 30 days=1 |
| **Job Value** | 15% | If `job_value` > 0: scale 1-10 based on value ($0=1, $50K=5, $200K+=10). If 0 or null, default to 5. |

Final score = weighted sum, rounded to 1 decimal.

### 4. Storage (BigQuery)
- Dataset: `waste_leads` in project `profitscout-fida8`
- Create tables if they don't exist:
  - `raw_permits`: All pulled permits (full JSON stored in a `permit_data` JSON column + key fields as columns: permit_id, address, city, county, zip, lat, lng, contractor_id, permit_type, tags, file_date, job_value, property_type, status)
  - `scored_leads`: Scored permits (all raw fields + score, score_breakdown JSON, contractor_name, contractor_employees JSON)
  - `contractors`: Cached contractor info (contractor_id, name, employees JSON, last_fetched)
- Use `WRITE_TRUNCATE` for raw_permits and scored_leads (full refresh each run)
- Use `WRITE_APPEND` with dedup for contractors (cache)
- Use the GCP service account credentials already configured on the machine (`google.auth.default()`)

### 5. Dashboard (`src/dashboard/app.py`)
Streamlit app with:

**Sidebar:**
- Client name display ("Arrow Waste Services")  
- Date range of scan
- Min score slider (default 5)
- Permit type filter (checkboxes)
- Property type filter

**Main content:**

**Section 1: Summary bar**
- Total leads found
- High priority (score ≥ 7)
- Commercial permits count
- New construction + demolition count

**Section 2: Lead cards** (sorted by score descending)
Each card shows:
- Score badge (color coded: ≥7 green, 5-6.9 amber, <5 gray)
- Address (street, city, zip)
- Permit type + description (if available)
- Property type badge
- Filed date
- Job value (if available)
- Contractor name + employee names (if available)
- Tags as colored badges
- Lat/lng map pin (small inline map or link to Google Maps)

**Section 3: Map view**
- Folium map centered on the service area
- All leads plotted as markers, color-coded by score
- Click marker → popup with permit details

**Section 4: Contractor leaderboard**
- Table: Contractor name | # of permits | Avg score | Top permit types
- Sorted by permit count descending
- "This contractor pulled 12 permits in your area — are they your customer?"

### 6. Main Pipeline (`src/main.py`)
- CLI entry point: `python -m src.main --client arrow [--skip-enrich] [--limit N]`
- Flow: load config → scan permits → enrich with contractor data → score → save to BigQuery
- Print summary at end: total scanned, enriched, scored, top 5 leads
- `--skip-enrich`: skip contractor API calls (saves API quota)
- `--limit N`: limit to N permits (for testing)

## Critical Rules
- **250 API call limit** — be extremely conservative. Cache everything. Don't re-fetch.
- **Don't over-engineer** — this is an MVP. No auth, no multi-tenant, no email digest yet.
- **Use existing GCP auth** — `google.auth.default()` works on this machine. Project: `profitscout-fida8`.
- **Handle nulls gracefully** — many permit fields (job_value, contractor_id, property_type, description) can be null.
- **Log everything** — print permit counts, API calls made, errors. We need to track API usage.
- **Test with `--limit 10` first** to verify the pipeline works before pulling full data.

## Run Order
1. `pip install -r requirements.txt`
2. `python -m src.main --client arrow --limit 10` (test run)
3. Verify data in BigQuery `waste_leads.scored_leads`
4. `streamlit run src/dashboard/app.py` (verify dashboard)
5. `python -m src.main --client arrow` (full run)

## After Building
Show me:
1. How many API calls were used
2. Total permits found per county
3. Top 5 leads by score
4. Screenshot or description of the dashboard
