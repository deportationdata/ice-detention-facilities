library(tidyverse)
library(tidylog)

detentions_41855 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release/2024-ICFO-41855",
    full.names = TRUE
  ) |>
  set_names() |>
  map(~ readxl::read_excel(.x, skip = 4), .id = "file") |>
  map(
    ~ select(
      .x,
      contains("Detention Facility"),
      # `Book in DCO`, # note there is DCO information in here.
      `Book Out Date Time`
    )
  ) |>
  bind_rows(.id = "file")

detentions_41855_df <-
  detentions_41855 |>
  janitor::clean_names() |>
  rename(
    detention_facility_operator = detention_facility_type_1,
    detention_facility_type = detention_facility_type_4
  ) |>
  mutate(
    detention_facility_operator = case_when(
      detention_facility_operator == "INS Facility" ~ "ICE Facility",
      TRUE ~ detention_facility_operator
    )
  ) |>
  slice_max(
    n = 1,
    by = detention_facility_code,
    order_by = book_out_date_time,
    with_ties = FALSE
  ) |>
  relocate(detention_facility_code, detention_facility) |>
  select(-book_out_date_time) |>
  select(
    detention_facility_code,
    name = detention_facility,
    address = detention_facility_address,
    city = detention_facility_city,
    state = detention_facility_state_code,
    zip = detention_facility_zip_code,
    type = detention_facility_type,
    type_detailed = detention_facility_type_detailed,
    ice_funded = detention_facility_ice_funded,
    male_female = detention_facility_male_female,
    over_under_72 = detention_facility_over_under_72,
    operator = detention_facility_operator
  ) |>
  mutate(date = as.Date("2024-01-01"))

arrow::write_feather(
  detentions_41855_df,
  "data/facilities-foia-41855.feather"
)
