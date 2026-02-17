library(tidyverse)
library(tidylog)

detentions_hrw_output <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/output.xlsx"
  ) |>
  janitor::clean_names() |>
  mutate(across(contains("date"), ~ mdy_hm(.x) |> as.Date())) |>
  distinct(
    detention_facility_code,
    name = detention_facility,
    docket = book_in_dco,
    city,
    state
  ) |>
  mutate(date = as.Date("2010-05-25"))

arrow::write_feather(
  detentions_hrw_output,
  "data/facilities-foia-hrw-output.feather"
)
