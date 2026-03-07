# Project Agent Guide

## Project Purpose
Build an extensible livestock intelligence platform that supports heterogeneous agricultural, environmental, financial, operational, and image-based data while preserving the existing drill-down usability.

## Architecture Rules
- Preserve drill-down hierarchy: `network -> farm -> herd/group -> animal -> metric`
- Never couple UI directly to raw source payloads
- Route all source data through: connector -> intake -> validation -> normalization -> storage -> analytics -> presentation
- Analytics modules must consume canonical internal records only
- Maintain compatibility with uploaded files and future live API connectors
- Every ingested record must carry provenance (`sourceSystem`, `sourceRecordId` where available, `timestamp`, `qualityFlag`, `metadata`)

## Coding Constraints
- Prefer additive refactors over breaking changes
- Quarantine bad/unmatched records; do not silently coerce invalid source data
- Fail explicitly and log ingestion/run diagnostics
- Keep current app runnable while modularizing

## Current Priorities
Phase 1 first:
1. modular structure
2. canonical schema and DB tables
3. connector interface + registry
4. sensor/weather/prices ingestion through generic pipeline
5. API/service layer for canonical data and run quality
6. data quality visibility

## Handoff Expectations
Update `docs/handoffs/YYYY-MM-DD-phase-ticket-summary.md` for each meaningful chunk including:
- objective completed
- files changed
- schema changes
- endpoints added
- UI modules touched
- tests added/passed
- known issues
- next recommended step
