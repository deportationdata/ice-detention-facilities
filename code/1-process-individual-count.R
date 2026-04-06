library(tidyverse)
library(arrow)

detentions_latest <- read_parquet(
  "~/github/ice/data/detention-stints-latest.parquet"
)

detentions_39357 <- read_feather(
  "~/github/ice/data/detention-stints-oct25.feather"
)

# Subset latest to 2025-01-01 onward
detentions_latest_2025_26 <-
  detentions_latest |>
  filter(as.Date(book_in_date_time) >= as.Date("2025-01-01")) |>
  transmute(
    detention_facility_code,
    book_in_date_time,
    anonymized_identifier
  )

# Keep only 39357 facilities not in the 2025+ latest subset (hospitals etc.)
codes_in_latest <- unique(detentions_latest_2025_26$detention_facility_code)

detentions_39357_extra <-
  detentions_39357 |>
  filter(
    !detention_facility_code %in% codes_in_latest,
    as.Date(book_in_date_time) >= as.Date("2025-01-01")
  ) |>
  transmute(
    detention_facility_code,
    book_in_date_time,
    anonymized_identifier = unique_identifier
  )

detentions_combined <-
  bind_rows(
    'latest' = detentions_latest_2025_26,
    'extra' = detentions_39357_extra,
    .id = "source"
  ) |>
  mutate(year = year(book_in_date_time))

counts <-
  detentions_combined |>
  group_by(source, detention_facility_code, year) |>
  summarize(n = n_distinct(anonymized_identifier), .groups = "drop") |>
  pivot_wider(names_from = source, values_from = n) |>
  mutate(n = coalesce(latest, extra)) |>
  select(detention_facility_code, year, n) |>
  pivot_wider(names_from = year, values_from = n, names_prefix = "individuals_") |>
  arrange(detention_facility_code)

write_parquet(counts, "data/facility-individual-counts-2025-2026.parquet")
