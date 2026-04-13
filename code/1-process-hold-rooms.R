library(tidyverse)
library(tidylog)
library(uuid)

hold_rooms <-
  data.table::fread("inputs/noccc/NOCCC_holdroom_research.csv") |>
  as_tibble() |>
  transmute(
    detention_facility_code = holdroom_detention_facility_code,
    name = holdroom_detention_facility,
    address,
    city = Address_City,
    state = Address_State,
    zip = as.character(Address_Zip),
    date = as.Date("2026-03-16")
  )

# Field-office hold rooms: recreate ICE office UUIDs using the coldCounter
# formula — uuid5(namespace, paste(office_name, city, state, sep="_")) — then
# join the holdroom->office crosswalk to pull address/city/state/zip.
# See: https://github.com/CovenAnalytica/coldCounter/blob/main/code/build_coldCounter.py
ice_offices_namespace <- "12345678-1234-5678-1234-567812345678"

ice_offices <-
  readxl::read_excel("inputs/noccc/ice-offices.xlsx") |>
  mutate(
    office_id = mapply(
      function(n, c, s) {
        UUIDfromName(
          ice_offices_namespace,
          paste(n, c, s, sep = "_"),
          type = "sha1"
        )
      },
      office_name,
      city,
      state
    )
  )

field_office_hold_rooms <-
  data.table::fread("inputs/noccc/holdroom_office_mapping.csv") |>
  as_tibble() |>
  left_join(ice_offices, by = "office_id") |>
  transmute(
    detention_facility_code = holdroom_detention_facility_code,
    name = office_name,
    address,
    city,
    state,
    zip,
    date = as.Date("2026-03-16")
  )

hold_rooms <- bind_rows(hold_rooms, field_office_hold_rooms) |> select(-name)

arrow::write_feather(
  hold_rooms,
  "data/noccc-hold-rooms.feather"
)
