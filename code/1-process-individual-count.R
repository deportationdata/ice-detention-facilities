library(tidyverse)
library(arrow)

# Current ICE daily-population feed — covers active 2025+ facilities but excludes hospitals / medical facilities
facility_daily_pop_latest <-
  arrow::read_parquet(
    "https://github.com/deportationdata/ice/raw/refs/heads/main/data/facilities-daily-population-latest.parquet"
  )

# Older snapshot of the same feed — has the hospitals dropped from the current file.
facility_daily_pop_extra <-
  arrow::read_feather(
    "https://github.com/deportationdata/ice/raw/38ff48e3ca9971e844058a4f6b188f150063a1a5/data/facilities-daily-population-latest.feather"
  )

latest_codes <- unique(facility_daily_pop_latest$detention_facility_code)

facility_daily_pop <-
  bind_rows(
    facility_daily_pop_latest,
    facility_daily_pop_extra |>
      filter(!detention_facility_code %in% latest_codes)
  )

# Stats over the last 365 days of available data
daily_pop_last_date <- max(facility_daily_pop$date, na.rm = TRUE)

counts <-
  facility_daily_pop |>
  filter(
    date > daily_pop_last_date - 365,
    date <= daily_pop_last_date
  ) |>
  group_by(detention_facility_code) |>
  summarise(
    days_with_detentions_daily_last_year = sum(
      n_detained >= 1,
      na.rm = TRUE
    ),
    days_with_detentions_midnight_last_year = sum(
      n_detained_at_midnight >= 1,
      na.rm = TRUE
    ),
    average_daily_population_last_year = mean(n_detained, na.rm = TRUE),
    average_midnight_population_last_year = mean(
      n_detained_at_midnight,
      na.rm = TRUE
    ),
    max_daily_population_last_year = max(n_detained, na.rm = TRUE),
    max_midnight_population_last_year = max(
      n_detained_at_midnight,
      na.rm = TRUE
    ),
    .groups = "drop"
  ) |>
  arrange(detention_facility_code)

write_parquet(counts, "data/facility-individual-counts-2025-2026.parquet")
