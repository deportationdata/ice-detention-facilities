library(tidyverse)
library(tidylog)

detentions_hrw_facilities_2015 <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/2015IceDetentionFacilityListing.xlsx",
    sheet = "Facility List - Main"
  ) |>
  janitor::clean_names() |>
  rename(
    detention_facility_code = history_detention_facility_code,
    operator = facility_operator,
    owner = facility_owner
  ) |>
  mutate(zip = as.character(zip)) |>
  mutate(date = as.Date("2015-12-08"))

arrow::write_feather(
  detentions_hrw_facilities_2015,
  "data/facilities-2015.feather"
)
