library(tidyverse)
library(tidylog)

detentions_14_09300 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily",
    pattern = "Excel \\d+ RIF Copy",
    full.names = TRUE
  ) |>
  set_names() |>
  map_dfr(~ readxl::read_excel(.x, skip = 21), .id = "file") |>
  janitor::clean_names() |>
  mutate(across(contains("date"), ~ mdy_hm(.x) |> as.Date())) |>
  distinct(
    detention_facility_code = history_detention_facility_code,
    name = history_detention_facility,
    docket = history_intake_dco,
  ) |>
  mutate(date = as.Date("2014-10-15"))

arrow::write_parquet(
  detentions_14_09300,
  "data/facilities-foia-14-09300.parquet"
)
