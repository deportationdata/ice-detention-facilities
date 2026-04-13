library(tidyverse)
library(tidylog)

detentions_22955 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata-web-archive/ice/arrests/2022-ICFO-22955/",
    full.names = TRUE
  ) |>
  set_names() |>
  map(~ readxl::read_excel(.x, skip = 4), .id = "file") |>
  map(
    ~ select(
      .x,
      contains("Facility"),
      # `Docket DCO`, # note there is DCO information in here.
      `Apprehension Date And Time`
    )
  ) |>
  bind_rows(.id = "file")

detentions_22955_df <-
  detentions_22955 |>
  janitor::clean_names() |>
  filter(!is.na(detention_facility_code)) |>
  mutate(
    detention_facility_operator = case_when(
      detention_facility_type == "INS Facility" ~ "ICE Facility",
      TRUE ~ detention_facility_type
    ),
    .keep = "unused"
  ) |>
  slice_max(
    n = 1,
    by = detention_facility_code,
    order_by = apprehension_date_and_time,
    with_ties = FALSE
  ) |>
  relocate(detention_facility_code, detention_facility) |>
  select(-apprehension_date_and_time) |>
  select(
    detention_facility_code,
    name = detention_facility,
    address = detention_facility_address,
    city = detention_facility_city,
    state = detention_facility_state,
    operator = detention_facility_operator
  ) |>
  mutate(date = as.Date("2024-06-01"))

arrow::write_parquet(
  detentions_22955_df,
  "data/facilities-foia-22955.parquet"
)
