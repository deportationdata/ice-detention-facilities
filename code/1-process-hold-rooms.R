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
  # Upstream data correction: the Wenatchee, WA row in ice-offices.xlsx
  # carries Yakima's address (3701 River Road, Yakima WA 98902). The real
  # Wenatchee sub-office is 301 Yakima St, Wenatchee WA 98801 — verified in
  # ICE FOIAs 05655 and 22955. Patch here until the xlsx is corrected upstream.
  mutate(
    address = if_else(
      office_name == "Wenatchee, WA" & city == "Yakima",
      "301 Yakima St",
      address
    ),
    city = if_else(office_name == "Wenatchee, WA", "Wenatchee", city),
    zip = if_else(office_name == "Wenatchee, WA", "98801", zip),
    address_full = if_else(
      office_name == "Wenatchee, WA",
      "301 Yakima St, Wenatchee, WA 98801",
      address_full
    )
  ) |>
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
  ) |>
  # Two OPLA offices have multiple xlsx rows that collapse to the same
  # office_id (office_name + city + state tuple is identical across rows).
  # Without dedupe the left_join below duplicates every hold room mapped
  # to them. Drop the rows we don't want:
  #   - Miami OPLA (523e20ab-…): keep the 18201 SW 12th Street row (Krome
  #     building, where KROHOLD sits) over the 333 S. Miami Avenue OPLA HQ.
  #   - Atlanta OPLA (942bf786-…): no hold room currently maps here; keep
  #     the 180 Ted Turner Drive row deterministically so a future mapping
  #     doesn't silently double-row.
  filter(
    !(office_id == "523e20ab-949b-53df-92ec-db2f783b53aa" &
      address != "18201 SW 12th Street"),
    !(office_id == "942bf786-0940-5e41-8d16-bca845b8cf52" &
      address != "180 Ted Turner Drive, SW, Suite 332")
  )

field_office_hold_rooms <-
  data.table::fread("inputs/noccc/holdroom_office_mapping.csv") |>
  as_tibble() |>
  # Upstream data corrections applied to raw mapping rows:
  #   - CHYHOLD and CSPHOLD have their office_ids swapped in
  #     holdroom_office_mapping.csv: CHYHOLD (Cheyenne Hold Room) points at
  #     Casper's office and vice versa. Swap them back.
  #   - WNTHOLD points at the aa331fed- UUID computed from the uncorrected
  #     "Wenatchee, WA"+"Yakima" row in ice-offices.xlsx. Since we patched
  #     the xlsx row above to city="Wenatchee", the office_id recomputes
  #     to 544157a0-…; point WNTHOLD at that.
  mutate(
    office_id = case_when(
      holdroom_detention_facility_code == "CHYHOLD" ~
        "5a2c92e2-2327-5507-bc26-d4431bc31881", # Cheyenne, WY office
      holdroom_detention_facility_code == "CSPHOLD" ~
        "8652627d-adc5-52c8-8624-2893111359c8", # Casper, WY office
      holdroom_detention_facility_code == "WNTHOLD" ~
        "544157a0-ead9-5055-b607-20c498a84b78", # Wenatchee, WA (corrected)
      TRUE ~ office_id
    )
  ) |>
  # Codes already curated in NOCCC_holdroom_research.csv take precedence —
  # the field-office address can differ from the hold-room address
  # (e.g. BSCHOLD's field office is in El Paso, not Big Spring).
  anti_join(
    hold_rooms,
    by = c("holdroom_detention_facility_code" = "detention_facility_code")
  ) |>
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

arrow::write_parquet(
  hold_rooms,
  "data/noccc-hold-rooms.parquet"
)
