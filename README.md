# Data Vault Demo - NT Demo Project

## Overview
This dbt Cloud project demonstrates a **Data Vault 2.0** implementation with zero package dependencies. It addresses three customer requirements:

1. **Ingest file into RAW table NT_table** → `dbt seed`
2. **Add AUDIT_ID and LOAD_DTS** → `stg_nt_table` staging model
3. **Transform emails from @example.com to @ntrs.com** → `curated_firm_details` curated model

## Architecture

```
seeds/SampleFileForDBT.csv
    │
    ▼  (dbt seed → NT_table)
models/staging/stg_nt_table.sql          ← Hash keys, hashdiff, audit columns
    │
    ├──▶ models/raw_vault/hub_firm.sql           ← Unique business keys (insert-only)
    │
    ├──▶ models/raw_vault/sat_firm_details.sql   ← Attribute history (change-detected)
    │
    ▼
models/curated/curated_firm_details.sql  ← Business rules (email transformation)
```

## Running the Demo

```bash
# Step 1: Load seed data into Snowflake
dbt seed

# Step 2: Build all models
dbt run

# Step 3: Run tests to validate Data Vault integrity
dbt test

# Or do it all at once
dbt build
```

## Demo Talking Points

### Staging Layer
- "This is where we add the audit columns your team requested - AUDIT_ID and LOAD_DTS."
- "Hash keys and hashdiffs are generated here for Data Vault loading."
- "In production, the seed reference would be replaced by a source() pointing at your landing zone."

### Raw Vault (Hub + Satellite)
- "The Hub stores unique business keys - it's insert-only, never updated."
- "The Satellite tracks every change in descriptive attributes using hashdiff comparison."
- "No business logic here - this is a pure, auditable record of what the source system sent."

### Curated Layer
- "Business rules live here, completely separated from raw data."
- "The email transformation from @example.com to @ntrs.com happens in this layer."
- "If the rule changes tomorrow, your Raw Vault is untouched - just update this model and rebuild."

### dbt Cloud Value
- **Lineage**: Show the full DAG from seed → staging → hub/sat → curated
- **Explorer/Docs**: All vault metadata is documented and searchable
- **CI/CD**: PR checks validate changes before they hit production
- **Jobs**: Schedule incremental satellite loads on a cadence
- **Tests**: Referential integrity between Hub and Satellite is enforced automatically

## Scaling Up
- "For 50+ entities, you'd use AutomateDV to generate this SQL from metadata."
- "The pattern is identical - AutomateDV just automates the boilerplate."
- "dbt Mesh lets you share vault entities across teams as contracts."
