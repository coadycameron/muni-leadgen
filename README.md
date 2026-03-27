# Municipal Lead Gen Repo

This repo is a muni-specific fork of your QAQC contractor lead-gen flow.

## What changed

The contractor discovery and company saturation logic is gone.

The municipal flow is:

1. Import the full named-places workbook into Firestore
2. Keep only places where `Priority = Highest - Target`
3. Randomly reserve 10 eligible municipalities per run
4. Send those 10 municipalities into the Gemini research prompt
5. Verify returned emails with the existing waterfall
6. Build `WRITER_INPUT` directly in Python
7. Send the kept leads into the Gemini writer prompt
8. Push kept leads to Firestore, HubSpot, and Sheets
9. Reopen municipalities later when HubSpot marks the active contact as `no_response`, `bounced`, or `invalid`

## Repo layout

- `municipal_leadgen.py`
  - main Cloud Run entrypoint
- `import_municipalities_to_firestore.py`
  - one-time or periodic import from `named_places_population_usa.xlsx`
- `sync_muni_contact_states_from_hubspot.py`
  - sync job that reopens municipalities based on HubSpot outcome properties
- `muni_leadgen/`
  - muni-specific Firestore, HubSpot, Gemini, and transform modules
- `leadgen_common/`
  - reused verifier, cache, dedupe, and Sheets helpers from the QAQC repo
- `prompts/`
  - your attached research and writer prompts
- `schemas/`
  - your attached structured output schemas

## Firestore model

### Collection: `muni_master`
Document id is `municipality_name|state`.

Key fields:

- `municipality_key`
- `municipality_name`
- `state`
- `type`
- `population_2024`
- `priority`
- `random_bucket`
- `lead_status`
- `open_for_research`
- `reserved_by_run_id`
- `reserved_at`
- `active_contact_email`
- `engaged_contact_email`
- `blocked_emails`
- `stale_contact_count`
- `last_outcome`
- `next_research_eligible_at`
- `lead_gen_restrict_sync`

### Subcollection: `contacts`
One doc per contact attempt under each municipality.

Key fields:

- `contact_full_name`
- `contact_preferred_name`
- `contact_title`
- `contact_email`
- `research_confidence`
- `personalization_tier`
- `personalization_anchor_text`
- `current_method_or_workflow`
- `verified_context_facts`
- `writer_caution`
- `contact_source_url`
- `catalyst_source_url`
- `corroboration_source_url`
- `email_verification_status`
- `subject_line`
- `email_body`
- `sequence_outcome`
- `contact_status`
- `stale`
- `hubspot_contact_id`

## HubSpot assumptions

This repo is designed around **custom properties plus workflows**.

### Contact properties expected
Set these internal names to match your portal or override with env vars.

- `aimunilead`
- `tp_muni_key`
- `tp_muni_priority`
- `tp_muni_status`
- `tp_muni_sequence_outcome`
- `tp_muni_contact_status`
- `tp_muni_personalization_tier`
- `tp_muni_anchor`
- `tp_muni_contact_source_url`
- `tp_muni_catalyst_source_url`
- `tp_muni_corroboration_source_url`
- `tp_muni_writer_version`
- `tp_muni_research_version`
- `leadgenrestrictsync`
- `external_bounce`

### Company properties expected
Recommended:

- `aimuniaccount`
- `tp_muni_key`
- `tp_muni_priority`
- `tp_muni_status`
- `lead-gen-restrict-sync`

## Stale contact handling

This repo does **not** infer stale from “not in sequence anymore”.

Instead, the sync script trusts explicit terminal outcomes:

- `replied`
- `meeting_booked`
- `no_response`
- `bounced`
- `invalid`
- `restricted`
- `manual_stop`

Recommended HubSpot workflow pattern:

1. On sequence enrollment:
   - `tp_muni_contact_status = active_sequence`
   - `tp_muni_sequence_outcome = active`

2. On reply or booked meeting:
   - `tp_muni_contact_status = replied`
   - `tp_muni_sequence_outcome = replied` or `meeting_booked`

3. On bounce or invalid:
   - `tp_muni_contact_status = bounced`
   - `tp_muni_sequence_outcome = bounced`

4. On finished sequence without engagement:
   - `tp_muni_contact_status = no_response`
   - `tp_muni_sequence_outcome = no_response`

The sync script then reopens the municipality and blocks that email from future research.

## Run order

### 1. Import the master list

```bash
python import_municipalities_to_firestore.py --xlsx named_places_population_usa.xlsx
```

### 2. Run the leadgen job

```bash
python municipal_leadgen.py
```

### 3. Sync outcomes from HubSpot

```bash
python sync_muni_contact_states_from_hubspot.py
```

Run the sync job on a schedule after your HubSpot workflows have had time to update outcome properties.

## Notes

- The main script always sets the HubSpot `ai-muni-lead` property before upserting.
- Municipalities flagged with `lead-gen-restrict-sync = yes` stay permanently closed.
- Municipalities that reply stay closed.
- Municipalities with `no_response`, `bounced`, or `invalid` reopen after the cooldown window.
- Reopened municipalities carry `blocked_emails`, so the researcher is told not to return stale emails again.
