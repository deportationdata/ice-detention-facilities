library(tidyverse)

marshall <- read_csv(
  # "https://github.com/themarshallproject/dhs_immigration_detention/raw/refs/heads/master/locations.csv"
  "~/Downloads/locations (1).csv"
) |>
  rename_with(.fn = ~ str_to_lower(.x) |> str_replace_all(" ", "_")) |>
  rename(
    detention_facility_code = detloc,
    latitude = lat,
    longitude = lng
  ) |>
  mutate(
    date_of_first_use = dmy(date_of_first_use),
    date_of_last_use = dmy(date_of_last_use),
    zip = as.character(zip)
  ) |>
  mutate(date = as.Date("2019-09-24"))

arrow::write_feather(marshall, "data/facilities-marshall.feather")
