# Pipeline Validation Contract
**Version:** 1.1  
**Last updated:** 2026-03-16

This document is the authoritative contract between the JSON schema system and the ingestion pipeline. JSON Schema cannot enforce cross-file referential integrity — all checks in this document must be implemented in the pipeline. Failure of any check must halt ingestion and report the offending field and value.

---

## 1. Cross-file ID existence checks

These checks verify that every ID reference in one file resolves to a valid entry in the target file.

| Source field | Target file | Target key |
|---|---|---|
| `common_metadata.supersedes` | product manifest index | `product_id` |
| `common_metadata.superseded_by` | product manifest index | `product_id` |
| `common_metadata.related_reports[].product_id` | product manifest index | `product_id` |
| `inheritable_audit_metadata.main_audited_entities[].ministry` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.main_audited_entities[].department` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.main_audited_entities[].autonomous_bodies[]` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.main_audited_entities[].other_bodies[]` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.other_audited_entities[].*` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.referenced_entities[]` | `registry_entities.json` | `id` |
| `inheritable_audit_metadata.regions.states_uts[]` | `registry_states_uts.json` | `id` |
| `inheritable_audit_metadata.primary_schemes[]` | `registry_schemes.json` | `id` |
| `inheritable_audit_metadata.other_schemes[]` | `registry_schemes.json` | `id` |
| `inheritable_audit_metadata.audit_type[]` | `taxonomy_audit_type.json` | `entries[].id` |
| `inheritable_audit_metadata.report_sector[]` | `taxonomy_report_sector.json` | `entries[].id` |
| `inheritable_audit_metadata.topics[]` | `taxonomy_topics.json` | `entries[].id` |
| `inheritable_audit_metadata.audit_findings_categories[]` | `taxonomy_audit_findings_audit_report.json` | `entries[].id` |
| `inheritable_audit_metadata.examination_coverage.state_ut_ids[]` | `registry_states_uts.json` | `id` |
| `audit_report_metadata.report_level.state_ut.id` | `registry_states_uts.json` | `id` |
| `audit_report_metadata.report_level.government_context.nodal_ministry` | `registry_entities.json` | `id` |
| `audit_report_metadata.report_level.government_context.nodal_departments[]` | `registry_entities.json` | `id` |
| `audit_report_metadata.report_level.tabling.submitted_to[]` | `registry_entities.json` | `id` |
| `atn.atn_records[].department` | `registry_entities.json` | `id` |
| `atn.atn_records[].rounds[].department` | `registry_entities.json` | `id` |
| `content_block.block_metadata.audit_findings_categories[]` | `taxonomy_audit_findings_audit_report.json` | `entries[].id` |
| `catalog.entries[].audit_findings_categories[]` | `taxonomy_audit_findings_audit_report.json` | `entries[].id` |
| `catalog.entries[].topics[]` | `taxonomy_topics.json` | `entries[].id` |
| `catalog.entries[].report_sector[]` | `taxonomy_report_sector.json` | `entries[].id` |
| `registry_schemes.administering_entity_id` | `registry_entities.json` | `id` |

---

## 2. Taxonomy ID format checks

These fields must contain IDs matching the correct format for the target taxonomy. JSON Schema validates the field type but not the format against the target file's convention.

| Field | Expected ID format | Example |
|---|---|---|
| `audit_type[]` | `ATYPE-*` (SCREAMING-KEBAB) | `ATYPE-COMPLIANCE` |
| `report_sector[]` | `SECT-*` (SCREAMING-KEBAB) | `SECT-CIVIL-HEALTH` |
| `topics[]` | `snake_case` | `rural_employment` |
| `audit_findings_categories[]` | `snake_case` | `wasteful_expenditure` |
| `primary_schemes[]` / `other_schemes[]` | `GOI-SCH-*` or `IN-{ST}-SCH-*` | `GOI-SCH-MGNREGS` |
| `referenced_entities[]` | Prefix from `registry_entities.id_convention` | `GOI-MIN-FINANCE` |
| `regions.states_uts[]` | `^IN-[A-Z]{2,3}$` | `IN-MP` |

---

## 3. Inheritance level restriction checks

These fields are only valid at certain levels of the document hierarchy. The pipeline must reject placements at invalid levels.

| Field | Valid levels | Action on violation |
|---|---|---|
| `audit_findings_categories[]` | `section`, `subsection` only | Reject ingest with error |
| `pac_status` | `chapter` only | Reject ingest with error |
| `main_audited_entities` | `report`, `chapter` (or any level if `metadata_override_mode=replace`) | Reject ingest with error |
| `dpc_act_sections[]` | `report`, `chapter`, `section` | Reject ingest with error |

---

## 4. Pipeline-computed fields

These fields must be set by the pipeline on every ingest. A missing value after ingest indicates pipeline failure and must block publication.

| Field | Source | Computation rule |
|---|---|---|
| `audit_report_metadata.report_level.state_ut.status_at_report_date` | `registry_states_uts.json` `status_history[]` | Find entry where `valid_from <= report_year <= valid_to` (null `valid_to` = current). If report year precedes all history entries, use earliest. |
| `content_block.vector_embedding_id` | External vector store | Set after embedding the block's text content. |
| `content_block.embedding_model` | Pipeline config | Set to the model name used at embedding time. |
| `structure.content_unit.vector_embedding_id` | External vector store | Set after embedding the unit's `executive_summary`. Only set if `executive_summary` is present. |
| `structure.content_unit.embedding_model` | Pipeline config | Set alongside `vector_embedding_id`. |
| `dataset.vector_embedding_id` | External vector store | Set after embedding the dataset's `summary`. Only set if `summary` is present. |
| `dataset.embedding_model` | Pipeline config | Set alongside `vector_embedding_id`. |
| `manifest.total_files` | File system count | Count of all files hashed in `file_checksums` (excluding `manifest.json` itself). |
| `reference.reference_id` | Pipeline ID generator | `REF-{product_id}-{sequential_number}`. |
| `reference.inverse_relationship_type` | Lookup table | Derive from `relationship_type` using the inverse mapping in `reference.schema`. |
| `atn.atn_records[].current_round` | `rounds[]` array length | Always equals `rounds.length`. |
| `manifest.schema_versions` | Schema files | Read `$version` from each schema file at ingest time. |

---

## 5. Taxonomy referential integrity checks (within-file)

These checks verify that parent-child relationships within taxonomy files are internally consistent.

| File | Check |
|---|---|
| `taxonomy_topics.json` | Every ID in `sub_topics[]` must exist in `entries[]`. |
| `taxonomy_audit_findings_audit_report.json` | Every ID in `sub_categories[]` must exist in `entries[]`. |
| `taxonomy_report_sector.json` | Every ID in `sub_sectors[]` must exist in `entries[]`. |
| `registry_entities.json` | Every ID in `predecessor_ids[]` and `successor_ids[]` must exist in `entries[]`. |
| `registry_schemes.json` | Every ID in `predecessor_ids[]`, `successor_ids[]`, and `related_ids[]` must exist in `entries[]`. |
| `registry_states_uts.json` | Every ID in `predecessor_ids[]` and `successor_ids[]` must exist in `entries[]`. |

---

## 6. Schema enum sync check

Run `scripts/validate_schema_enum_sync.py` on every commit. This script compares:

- `taxonomy_product_types.json` `entries[].id` values → `common_metadata.schema` `product_type.enum`
- `taxonomy_audit_type.json` `entries[].id` values → `inheritable_audit_metadata.schema` `audit_type[].items` (documentation only — audit_type[] accepts free strings but pipeline should reject non-registry values)

Fail the CI build if any mismatch is detected.

---

## 7. Status consistency checks

| Check | Rule |
|---|---|
| `atn.current_status` vs `rounds[-1].status_after_round` | Must match. If `rounds` is non-empty, `current_status` must equal `status_after_round` of the last round. |
| `atn.settled_date` presence | Must be present if and only if `current_status = settled`. |
| `audit_report_metadata.tabling.applicable=false` | `submitted_to[]` and `reason_not_tabled` must be present. `lower_house` and `upper_house` must not be present. |
| `audit_report_metadata.tabling.applicable=true` | `legislature` and `lower_house` must be present. |
| `manifest.total_files` | Must equal `len(file_checksums)`. |
| `manifest.file_checksums` values | Must match actual SHA-256 of each referenced file on disk. |
| `annotations[].target` presence | Must be present when `annotation_type = cross_reference`. Must be absent (or null) for all other annotation types. |
| `annotations[].target` format | Validated as one of: bare `unit_id` (^[A-Z0-9][A-Z0-9\-]+$), `product_id/unit_id`, `product_id/block_id`, `product_id/dataset_id`, or a URI (^https?://). |
| `annotations[].target` resolution | When target is a unit_id or product_id/unit_id pointing to an intra-report unit, pipeline validates the unit_id exists in structure.json. Cross-report targets are validated against the catalog index if available, otherwise warned only. |

---

## 8. Product type vs ATN/PAC applicability

Check `taxonomy_product_types.json` `atn_applicable` and `pac_applicable` flags before creating ATN or PAC tracking structures.

| product_type | ATN structures expected | PAC structures expected |
|---|---|---|
| `audit_report` | Yes | Yes |
| `accounts_report` | No | No |
| `state_finance_report` | No | No |
| `study_report` | No | No |
| `audit_impact_report` | No | No |
| `other` | No | No |

Pipeline must reject ATN files associated with `atn_applicable=false` product types and log a warning rather than an error (to handle legacy data).

---

## 9. richbox block type checks  *(added for content_block.schema v1.2)*

These checks apply to blocks with `block_type = richbox`. JSON Schema enforces structural validity; these pipeline checks enforce cross-file referential integrity within the richbox body.

| Check | Rule | Action on violation |
|---|---|---|
| `richbox.body[].table_ref.dataset_ref` resolution | Every `dataset_ref` value in a `table_ref` body item must resolve to a `dataset_id` in a dataset file in the same report's `datasets/` folder. | Reject ingest with error: `richbox body table_ref {dataset_ref} not found in datasets/` |
| `richbox.body[].image.asset_ref` existence | Every `asset_ref` value in an `image` body item must point to a file that exists under `assets/` in the report folder. | Warning only (asset may be added later): `richbox body image asset_ref {path} not found` |
| `richbox.body[].footnote_markers[]` resolution | Every marker value in `footnote_markers[]` on any richbox body item (heading, paragraph, bullets, ordered_list, image, table_ref) must match a `marker` value in the chapter's footnotes file (`footnotes/footnotes_{unit_id}.json`). | Warning only: `richbox body item footnote_marker {marker} has no matching footnote in {footnotes_file}` |
| `richbox.body` non-empty | `body[]` must contain at least one item. Schema enforces `minItems:1` but pipeline must also check after stripping empty strings. | Reject ingest with error |

---

## 10. Deprecated block type warnings  *(added for content_block.schema v1.2)*

These checks produce warnings (not errors) to help authors migrate away from deprecated block types. Legacy files using these types continue to validate and ingest normally.

| Block type | Status | Pipeline action |
|---|---|---|
| `executive_summary_block` | Deprecated alias for `richbox` with `box_type=executive_summary` | Warning: `block {block_id} uses deprecated block_type executive_summary_block — migrate to richbox with box_type=executive_summary` |
| `sidebar` | Soft-deprecated — use `richbox` instead | Warning: `block {block_id} uses soft-deprecated block_type sidebar — consider migrating to richbox with appropriate box_type` |

Warnings are written to the ingest log and the GitHub Actions step summary. They do not block ingest or publication.

---

## 11. content_block.schema version enforcement  *(added for content_block.schema v1.2.1)*

`manifest.schema_versions` records the schema version active at ingest time. The pipeline must enforce minimum version compatibility for richbox and cross_reference features.

| Check | Rule | Action |
|---|---|---|
| `schema_versions.content_block` vs detected features | If any block in the report uses `block_type=richbox`, `schema_versions.content_block` must be `>= 1.2`. | Reject ingest: `richbox blocks require content_block.schema >= 1.2` |
| `schema_versions.content_block` vs annotation features | If any block uses `annotation_type=cross_reference`, `schema_versions.content_block` must be `>= 1.1`. | Reject ingest: `cross_reference annotations require content_block.schema >= 1.1` |
| `schema_versions.content_block` vs footnote_markers on body items | If any richbox body item has `footnote_markers[]`, `schema_versions.content_block` must be `>= 1.2.1`. | Warning only (backward compatible): `footnote_markers on richbox body items require content_block.schema >= 1.2.1` |

In practice all new reports ingested after this update will have `schema_versions.content_block = 1.2.1` automatically — `manifest_builder.html` embeds the current schema versions at download time.


---

## 12. Product ID convention, categories, and jurisdiction rules

### 12.1 Product type prefixes

| product_type | Prefix | Jurisdiction | product_id includes |
|---|---|---|---|
| `audit_report` | `AR` | UNION, STATE, UT, LG | jurisdiction + state_code (if STATE/UT/LG) + lg_code (if LG, optional) |
| `accounts_report` | `AC` | UNION, STATE, UT, LG | jurisdiction + state_code (if STATE/UT/LG) + lg_code (if LG, optional) |
| `finance_report` | `SF` | UNION, STATE, UT, LG | jurisdiction + state_code (if STATE/UT/LG) + lg_code (if LG, optional) |
| `study_report` | `ST` | Not applicable | year only |
| `audit_impact_report` | `AI` | Not applicable | year only |
| `compendium` | `CP` | Not applicable | year only |
| `other` | `OT` | Not applicable | year only |

### 12.2 Product ID patterns

```
Jurisdiction-bound, no LG:    {PREFIX}{NN}-CAG-{YEAR}-{JURISDICTION}-{STATE_CODE}
                               e.g. AR06-CAG-2023-STATE-MP
Jurisdiction-bound, UNION:    {PREFIX}{NN}-CAG-{YEAR}-UNION
                               e.g. AC01-CAG-2023-UNION
LG without specific body:     {PREFIX}{NN}-CAG-{YEAR}-LG-{STATE_CODE}
                               e.g. AC01-CAG-2023-LG-MP
LG with specific body:        {PREFIX}{NN}-CAG-{YEAR}-LG-{STATE_CODE}-{LG_CODE}
                               e.g. AC01-CAG-2023-LG-MP-BBMP
Not jurisdiction-bound:       {PREFIX}{NN}-CAG-{YEAR}
                               e.g. ST01-CAG-2024
```

### 12.3 Accounts Report categories

`accounts_report` has four categories derived from `jurisdiction` + `local_body_type`:

| accounts_category | jurisdiction | local_body_type | accounts_sub_type applies? | portal_section | URL |
|---|---|---|---|---|---|
| `union_accounts` | UNION | — | Yes (finance/appropriation/combined) | `union_accounts` | `/accounts/union` |
| `state_accounts` | STATE or UT | — | Yes (finance/appropriation/combined) | `state_accounts` | `/accounts/state` |
| `ulb_accounts` | LG | ulb | No | `ulb_accounts` | `/accounts/ulb` |
| `pri_accounts` | LG | pri | No | `pri_accounts` | `/accounts/pri` |

`accounts_sub_type` values (union_accounts and state_accounts only):
- `finance_accounts` — Finance Accounts (receipts, expenditure, fund balances)
- `appropriation_accounts` — Appropriation Accounts (actual vs sanctioned grants)
- `combined` — Finance + Appropriation in one volume

ULB and PRI accounts do NOT have a finance/appropriation split.

Product ID examples:
- `AC01-CAG-2023-UNION` — Union Finance Accounts
- `AC02-CAG-2023-UNION` — Union Appropriation Accounts
- `AC01-CAG-2023-STATE-MP` — State Finance Accounts (MP)
- `AC01-CAG-2023-LG-MP` — ULB or PRI Accounts (all LG bodies in MP)
- `AC01-CAG-2023-LG-MP-BBMP` — ULB Accounts (Bruhat Bengaluru Mahanagara Palike)
- `AC02-CAG-2023-LG-MP-ZPJABALPUR` — PRI Accounts (Zila Parishad Jabalpur)

### 12.4 Finance Report categories

`finance_report` has four categories derived from `jurisdiction` + `local_body_type`:

| finance_category | jurisdiction | local_body_type | portal_section | URL |
|---|---|---|---|---|
| `union_finance` | UNION | — | `union_finance` | `/finance/union` |
| `state_finance` | STATE or UT | — | `state_finance` | `/finance/state` |
| `ulb_finance` | LG | ulb | `ulb_finance` | `/finance/ulb` |
| `pri_finance` | LG | pri | `pri_finance` | `/finance/pri` |

Product ID examples:
- `SF01-CAG-2023-UNION` — Union Finance Report
- `SF01-CAG-2023-STATE-MP` — State Finance Report (MP)
- `SF01-CAG-2023-LG-MP` — ULB or PRI Finance Report (all LG in MP)
- `SF01-CAG-2023-LG-MP-BBMP` — ULB Finance Report (specific ULB)
- `SF02-CAG-2023-LG-MP-ZPJABALPUR` — PRI Finance Report (specific Zila Parishad)

### 12.5 LG jurisdiction rules (applies to all LG products)

- `state_code` is ALWAYS required when `jurisdiction=LG` (ULBs and PRIs are under a state)
- `lg_code` is OPTIONAL — omit when the report covers all LG bodies in the state
- `local_body_type` (ulb/pri/combined_lg) is REQUIRED when `jurisdiction=LG`
- `local_body_type` is stored in `accounts_metadata.accounts_sub_type` (for accounts) and `finance_metadata.local_body_type` (for finance) and promoted to top-level `local_body_type` in the catalog entry

### 12.6 Pipeline enforcement rules

- Reject product_id prefix that does not match product_type
- Reject missing state_code when jurisdiction = STATE, UT, or LG
- Reject lg_code without state_code
- Reject missing local_body_type when jurisdiction = LG
- Reject accounts_sub_type on ulb_accounts or pri_accounts
- Set `portal_section` from product_type: audit_report→audit_reports, accounts_report→accounts_reports, finance_report→finance_reports, study_report→study_reports, audit_impact_report→audit_impact, compendium→compendium, other→compendium
- Within accounts_reports and finance_reports, set `accounts_metadata.accounts_category` or `finance_metadata.finance_category` from jurisdiction+local_body_type — these drive the filter sidebar within the section (not separate sections)
- Set `jurisdiction_applicable=false` for study_report, compendium, audit_impact_report, other
- Set `lg_code` in catalog from product_id segment after state_code (if present)

---

## 13. Non-audit product type metadata — deferred  *(placeholder section)*

`accounts_report`, `finance_report`, `study_report`, `audit_impact_report`, `compendium`, and `other` currently use `audit_report_metadata.schema` and `inheritable_audit_metadata.schema` as stubs. Most fields in these schemas do not apply to non-audit products.

**What this means right now:**
- For non-audit reports, leave `inheritable` fields empty or minimally filled (year, jurisdiction, regions only).
- `audit_type[]`, `report_sector[]`, `topics[]`, `audit_findings_categories[]`, `pac_status`, and `dpc_act_sections[]` should be omitted for non-audit products.
- The pipeline will not reject non-audit reports that omit these fields — they are all optional in the schema.
- `portal_section` is derived from `product_type` alone for non-audit reports — no classification fields needed.

**What changes when this is fully built (future work):**
1. `inheritable_accounts_metadata.schema` — classification fields for accounts reports: `finance_year`, `accounts_type` (consolidated_fund/contingency_fund/public_account), `certification_opinion`.
2. `inheritable_finance_metadata.schema` — fiscal indicators: `finance_year`, `fiscal_deficit_pct`, `revenue_deficit`, `debt_gsdp_ratio`.
3. `product.schema` updated with `if/then` conditional `$ref` per product_type for the specific section.
4. Metadata builder Tab 7 built at that point with proper fields per type.
5. This governance section updated to remove the deferred note.

**Pipeline enforcement for now:**
- Do NOT validate `audit_type[]`, `report_sector[]`, `topics[]` presence for non-audit product types.
- Do NOT reject non-audit reports that omit `inheritable` entirely.
- DO validate `product_id` prefix matches `product_type` (Section 12 rules still apply).

---

## 14. Footnote anchor validation  *(added for footnote.schema v1.1)*

New anchor fields added in footnote.schema v1.1: `anchor_char_start`, `anchor_char_end`, `anchor_unit_title`.

**Current status: deferred** — no reports ingested yet. When the first report is built, add these checks to `validate_registry_refs.py`:

| Check | Rule | Action on violation |
|---|---|---|
| `anchor_block_id` resolution | Every `anchor_block_id` in a footnote file must match a `block_id` in the unit's NDJSON blocks file | Error — broken anchor |
| `anchor_dataset_id` resolution | Every `anchor_dataset_id` must match a dataset file in `datasets/` | Error — broken anchor |
| `anchor_char_start` / `anchor_char_end` bounds | If present, `anchor_char_start` ≤ `anchor_char_end` ≤ length of the anchor block's text in the primary language | Warning — offset out of bounds |
| `anchor_unit_title` exclusivity | If `anchor_unit_title: true`, `anchor_block_id` and `anchor_dataset_id` must be absent | Error — conflicting anchors |
| Marker uniqueness within unit | No two footnotes in the same unit file should share the same `marker` value | Warning — duplicate markers |
