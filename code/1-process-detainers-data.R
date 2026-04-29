library(tidyverse)
library(tidylog)

detainers_master <- arrow::read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/cpang/data/ice-final/detainers-final-with-flags.feather"
)

detainers_latest <- arrow::read_parquet(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detainers-latest.parquet"
)

detainers_facilities <-
  bind_rows(
    "master" = detainers_master |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        city = facility_city,
        state = facility_state,
        detainer_prepare_date = prepare_date,
        unique_identifier,
        source_file
      ),
    "latest" = detainers_latest |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        city = facility_city,
        state = facility_state,
        detainer_prepare_date,
        unique_identifier,
        source_file = "ERO Detainers_LESA-STU_FINAL Release"
      ),
    .id = "source"
  ) |>
  arrange(detention_facility_code, detainer_prepare_date) |>
  group_by(detention_facility_code) |>
  summarize(
    name = last(name[!is.na(name)]),
    city = last(city[!is.na(city)]),
    state = last(state[!is.na(state)]),
    .groups = "drop"
  ) |>
  filter(!is.na(city), !is.na(state))

arrow::write_parquet(
  detainers_facilities,
  "data/facilities-from-detainers.parquet"
)
