library(googlesheets4)
gs4_deauth()
library(tidyverse)

facilities <- arrow::read_parquet(
  "~/github/ice-detention-facilities/data/facilities-latest.parquet"
) |>
  as_tibble() |>
  select(
    detention_facility_code,
    name,
    address,
    city,
    state,
    type,
    type_detailed,
    male_female,
    # ice_funded,
    over_under_72,
    # operator
  )

geocodes <- read_sheet(
  "https://docs.google.com/spreadsheets/d/1HZu-_ZruXjQatj6JSEjC0cOW78B0lwzQsa8P-8oCMvk/edit?gid=0#gid=0",
  sheet = "geocode"
)

geocodes |>
  group_by(detention_facility_code) |>
  summarize(
    finished = sum(response == TRUE) >= 2 & sum(response == FALSE) == 0,
    hold = first(str_detect(detention_facility_code, "HOLD"))
  ) |>
  count(finished, hold)

geocodes |>
  mutate(
    group1 =
  )
