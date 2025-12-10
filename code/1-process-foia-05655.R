library(tidyverse)
library(tidylog)

detentions_05655 <-
  list.files("~/Downloads/2024-ICFO-05655 (4)/", full.names = TRUE) |>
  set_names() |>
  map_dfr(~ readxl::read_excel(.x, skip = 4), .id = "file")

detentions_05655_df <-
  detentions_05655 |>
  janitor::clean_names() |>
  rename(
    detention_facility_operator = detention_facility_type_4,
    detention_facility_type = detention_facility_type_17
  ) |>
  select(
    c(contains("detention_facility"), -initial_book_in_detention_facility_code),
    detention_book_out_date_time
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
    order_by = detention_book_out_date_time,
    with_ties = FALSE
  ) |>
  relocate(detention_facility_code, detention_facility) |>
  select(-detention_book_out_date_time) |>
  select(
    detention_facility_code,
    name = detention_facility,
    address = detention_facility_address,
    city = detention_facility_city,
    state = detention_facility_state,
    zip = detention_facility_zip_code,
    type = detention_facility_type,
    type_detailed = detention_facility_type_detailed,
    ice_funded = detention_facility_ice_funded,
    male_female = detention_facility_male_female,
    over_under_72 = detention_facility_over_under_72
  ) |>
  mutate(date = as.Date("2024-02-01"))

arrow::write_feather(
  detentions_05655_df,
  "data/facilities-foia-05655.feather"
)
