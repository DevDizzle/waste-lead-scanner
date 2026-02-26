# Waste Lead Scanner: Future Architecture & Data Strategy

This document outlines the strategic roadmap for transforming the Waste Lead Scanner from an MVP into a potent, high-conversion lead generation engine for B2B waste management services.

## Current Limitations (MVP)
* **Data Freshness:** Third-party aggregator APIs (like Shovels) often have a multi-week lag between permit filing and API availability. This means sales teams reach out *after* a roll-off dumpster is already on site.
* **Missing Contact Data:** Many permits list "Unknown" for contractors, or lack direct phone numbers and emails, making it difficult for sales reps to initiate contact.

## Strategic Roadmap

### Phase 1: Upgrade the Data Engine (Freshness & Coverage)
To beat competitors to the job site, we must ingest data faster and more accurately.

1. **Direct County Scraping (High Priority Strategy):**
   * **Tooling:** Utilize **ScrapingBee** (already proven successful internally for Redfin data) to bypass bot-protection and scrape targeted county permit portals directly.
   * **Execution:** Build custom web scrapers for the highest-value territories (e.g., Fulton and Clayton counties). Run these scripts nightly to capture permits the *day after* they are filed.
2. **BuildZoom Integration (Secondary Source):** 
   * Evaluate the **BuildZoom Data API** as a replacement or supplement for generic permit APIs. BuildZoom specializes in matching permits directly to licensed contractors and boasts daily updates.
3. **LLM-Powered Description Parsing:**
   * Instead of relying solely on generic permit tags, pass the raw permit description text through an LLM (e.g., Gemini) to extract high-intent waste signals (keywords: *"gutting," "tear down," "full remodel," "roof replacement"*).

### Phase 2: The Enrichment Pipeline (Finding the Decision Maker)
A permit is only valuable if we can contact the person writing the check. We will build an automated "Skip Tracing" enrichment layer.

* **Scenario A: Contractor is Known**
  * Route the contractor company name through a B2B contact API (e.g., Apollo.io, Hunter.io) to append verified cell phone numbers and emails for Project Managers or Business Owners.
* **Scenario B: Contractor is Unknown (Owner-Builder/LLC)**
  * Take the site address and query a Real Estate/Property API (e.g., Regrid, local tax assessor data via ScrapingBee) to find the True Owner or holding LLC.
  * If an LLC is identified, automatically query the Secretary of State registry to find the Registered Agent's name and contact information.

### Phase 3: Actionable Intent Signals (Timing the Pitch)
Waste management sales rely entirely on timing.

* **Status Tracking:** Shift focus from the "File Date" to the "Status." A permit moving from *Submitted* to *Issued/Approved* is the exact trigger point for the sales team to call.
* **Visual Verification (Enterprise Deals):** For massive commercial jobs ($1M+ value), explore integrating Street View or satellite imagery APIs to visually verify if construction fences or early ground-breaking has occurred.

### Phase 4: Sales Workflow Automation
Move data out of passive dashboards and into active sales workflows.

* **CRM Push:** Automatically sync scored leads (e.g., Score > 7) directly into the company's CRM (Salesforce, HubSpot, GoHighLevel) as a "New Deal," assigned to the appropriate territory rep.
* **Driver Route Scouting:** Send a daily automated SMS or email digest to outside sales reps or drivers: *"Here are 5 new high-value demolition permits along your route today. Do a drive-by and drop a flyer."*
* **Automated Direct Mail:** Connect the pipeline to a service like Lob.com. The moment a new residential remodel permit is approved (where the owner is the likely buyer), a postcard is automatically printed and mailed to the property address offering dumpster services.

---

*Document generated based on strategic planning session. To be used as a reference for future sprints.*