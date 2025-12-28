library(tidyverse)

vera <- read_csv(
  "https://github.com/vera-institute/ice-detention-trends/raw/refs/heads/main/metadata/facilities.csv"
) |>
  rename(
    name = detention_facility_name
  ) |>
  mutate(date = as.Date("2025-07-01"))

arrow::write_feather(vera, "data/facilities-vera.feather")
