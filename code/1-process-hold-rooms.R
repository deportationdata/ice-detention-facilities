library(tidyverse)
library(tidylog)

hold_rooms <-
  data.table::fread("inputs/NOCCC_holdroom_research.csv") |>
  as_tibble()

hold_rooms <-
  hold_rooms |>
  transmute(
    detention_facility_code = holdroom_detention_facility_code,
    name = holdroom_detention_facility,
    address,
    city = Address_City,
    state = Address_City,
    zip = as.character(Address_Zip),
    date = as.Date("2026-03-16")
  )

arrow::write_feather(
  hold_rooms,
  "data/noccc-hold-rooms.feather"
)
