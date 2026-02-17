library(tidyverse)
library(tidylog)

detentions_05655 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release (ICE FOIA Library)/2024-ICFO-05655/",
    full.names = TRUE
  ) |>
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
    initial_book_in_date_time, # TODO need to figure out if this is right to use as the start date - probably not b/c likely refers to stay not stint
    detention_book_out_date_time,
    anonymized_identifier,
    gender
  ) |>
  mutate(
    detention_facility_operator = case_when(
      detention_facility_operator == "INS Facility" ~ "ICE Facility",
      TRUE ~ detention_facility_operator
    )
  )

detentions_05655_df_summary <-
  detentions_05655_df |>
  arrange(detention_facility_code, detention_book_out_date_time) |>
  group_by(detention_facility_code, detention_facility) |>
  summarize(
    name = unique(detention_facility),
    address = last(detention_facility_address),
    city = last(detention_facility_city),
    state = last(detention_facility_state),
    zip = last(detention_facility_zip_code),
    type = last(detention_facility_type),
    type_detailed = last(detention_facility_type_detailed),
    ice_funded = last(detention_facility_ice_funded),
    male_female = last(detention_facility_male_female),
    over_under_72 = last(detention_facility_over_under_72),
    operator = last(detention_facility_operator),
    first_book_in = as.Date(min(
      pmin(
        detention_book_out_date_time,
        initial_book_in_date_time,
        na.rm = TRUE
      ),
      na.rm = TRUE
    )),
    last_book_in = as.Date(max(
      pmax(
        detention_book_out_date_time,
        initial_book_in_date_time,
        na.rm = TRUE
      ),
      na.rm = TRUE
    )),
    n_stints = n(),
    n_individuals = n_distinct(anonymized_identifier),
    proportion_male = mean(
      gender == "Male" & gender != "Unknown",
      na.rm = TRUE
    )
  ) |>
  ungroup() |>
  mutate(date = as.Date("2024-02-01"))

arrow::write_feather(
  detentions_05655_df_summary,
  "data/facilities-foia-05655.feather"
)
