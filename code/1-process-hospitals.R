library(tidyverse)

hospitals_hhs <- read_csv(
  "inputs/Hospital_General_Information.csv"
)

hospitals_dhs <-
  st_read("inputs/datalumos-239108-V1/hospitals-3-shapefile/Hospitals.shp") |>
  st_drop_geometry() |>
  as_tibble() |>
  select(
    dhs_id = ID,
    name = NAME,
    address = ADDRESS,
    city = CITY,
    state = STATE,
    zip = ZIP,
    hospital_type = TYPE
  )

hospitals <-
  hospitals_hhs |>
  transmute(
    name = `Facility Name`,
    address = `Address`,
    city = `City/Town`,
    state = `State`,
    zip = `ZIP Code`,
    medicare_facility_ID = `Facility ID`,
    date = as.Date("2025-10-14") # last modified date
  ) |>
  full_join(
    hospitals_dhs |> select(dhs_id, name, city, state, hospital_type), # TODO handle duplicates on both sides
    by = c("name", "city", "state")
  )

stop()

arrow::write_feather(
  hospitals,
  "data/hospitals.feather"
)
