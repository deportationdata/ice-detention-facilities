#!/bin/bash
# CI subset of run_all.sh — runs only the scripts whose inputs already live
# in the repo. The other 1-process-* scripts read from local Box / external
# repos that don't exist on the GitHub Actions runner; their outputs (the
# `data/facilities-from-*.parquet` files) are committed to the repo and used
# as-is here.
#
# Run-locally workflow is still `bash code/run_all.sh`.
set -e

# Manual address overrides (depends only on inputs/addresses_manual2.csv)
Rscript code/1-process-manual-edits.R
# 1-process-ice-website.R is run earlier in the workflow against today's scrape

Rscript code/2-stack.R
Rscript code/3-name-state-match.R
Rscript code/4-match-hospitals.R
Rscript code/4-name-code-match.R
Rscript code/5-pick-attributes-latest.R
# Incrementally geocode any new addresses via Google/ArcGIS and update the
# canonical caches at data/facilities-geocoded-cache-{google,arcgis}.rds.
# Requires GOOGLEGEOCODE_API_KEY in the env (Google calls are skipped with a
# warning if it's missing). ArcGIS uses the public no-key endpoint.
Rscript code/6-facilities-geocode.R
Rscript code/7-augment-dataset.R
Rscript code/8-format-dataset.R
