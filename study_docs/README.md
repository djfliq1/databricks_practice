# Databricks DE Associate — Study Hub

A self-contained study portal for the Databricks Data Engineer Associate certification exam (2026). No server, no install — just open it in your browser.

## Quick Start

1. **Download** this repo (click the green **Code** button > **Download ZIP**), or clone it:
   ```
   git clone https://github.com/djfliq1/databricks_practice.git
   ```
2. Open `study_docs/index.html` in your browser (Chrome recommended)
3. Start studying

That's it. Everything runs locally in your browser.

## What's Inside

28 cheatsheets organized by exam domain:

| Domain | Weight | Docs |
|--------|--------|------|
| Data Processing & Transformations | 31% | 8 |
| Development & Ingestion | 30% | 6 |
| Productionizing Pipelines | 18% | 6 |
| Data Governance & Quality | 11% | 4 |
| Databricks Intelligence Platform | 10% | 2 |
| Cross-Domain Reference | — | 2 |

Topics include: PySpark, Delta Lake, AutoLoader, LakeFlow (DLT, Jobs, Connect), Unity Catalog, SQL Warehouses, CI/CD with DABs, and more.

## Features

- **Progress tracking** — Mark each doc as "In Progress" or "Completed". Your progress is saved in your browser's localStorage (stays between sessions, per-browser).
- **Domain filters** — Filter by exam domain to focus your study.
- **Search** — Search across titles, descriptions, and tags.
- **Copy buttons** — Hover over any code block to copy it.
- **Dark theme** — Easy on the eyes for long study sessions.

## Folder Structure

```
study_docs/
├── index.html                  ← Open this
├── cross_domain/               ← Cross-domain reference sheets
├── domain_1_processing/        ← Data Processing & Transformations
├── domain_2_ingestion/         ← Development & Ingestion
├── domain_3_pipelines/         ← Productionizing Pipelines
├── domain_4_governance/        ← Data Governance & Quality
└── domain_5_platform/          ← Databricks Intelligence Platform
```

Each `.html` file in the domain folders is a standalone cheatsheet — you can also open them individually if you prefer.
