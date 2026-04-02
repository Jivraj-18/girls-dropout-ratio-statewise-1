# new_repo — reproducible analysis pipeline (no data committed)

This folder contains a deterministic, script-driven pipeline that produces **machine-consumable** analytical outputs.

## Run

```bash
bash run.sh
```

## Inputs

The pipeline expects UDISE+ extracted CSV tables in a local folder.

Supported input modes:
- `UDISE_DATA_PATH=/path/to/udise_csv_data` (use an existing local copy; no downloads)
- default: downloads + extracts from `https://github.com/gsidhu/udise-csv-data` into `new_repo/data/` (gitignored)
- optional: `UDISE_GIT_REPO_URL=https://github.com/<owner>/<repo>` to override the default repo
- optional: `UDISE_CSV_ZIP_URL=https://.../something.zip` to explicitly provide a zip URL (takes precedence)

## Outputs (all reproducible; do not commit)

Written under `out/`:
- `schema.json`
- `summary_stats.json`
- `analysis_results.json`
- `feature_importance.json`
- `data_manifest.json`

## Policy

- Never commit raw/processed data (everything under `data/` is gitignored)
- Never generate dashboards/storytelling artifacts
