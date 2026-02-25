library(tidyverse)
library(tidylog)
library(sf)

hospitals_hhs <-
  read_csv(
    "inputs/Hospital_General_Information.csv"
  ) |>
  transmute(
    name = `Facility Name`,
    address = `Address`,
    city = `City/Town`,
    state = `State`,
    zip = `ZIP Code`,
    medicare_facility_ID = `Facility ID`,
    source = "hospitals-hhs",
    date = as.Date("2025-10-14") # last modified date
  ) |>
  distinct(name, city, state, .keep_all = TRUE)

hospitals_dhs <-
  st_read("inputs/datalumos-239108-V1/hospitals-3-shapefile/Hospitals.shp") |>
  st_drop_geometry() |>
  as_tibble() |>
  transmute(
    dhs_id = ID,
    name = NAME,
    address = ADDRESS,
    city = CITY,
    state = STATE,
    zip = ZIP,
    hospital_type = TYPE,
    hospital_dhs_ID = ID,
    source = "hospitals-dhs",
    date = as.Date("2025-08-01") # approximate last date of data collection
  ) |>
  distinct(name, city, state, .keep_all = TRUE)

hospitals <-
  hospitals_hhs |>
  select(name, address, city, state, zip, medicare_facility_ID, date) |>
  full_join(
    hospitals_dhs |> select(dhs_id, address, name, city, state, zip, date),
    by = c("name", "city", "state")
  ) |>
  mutate(
    address = coalesce(address.x, address.y),
    zip = coalesce(zip.x, zip.y),
    date = pmax(date.x, date.y, na.rm = TRUE),
    .keep = "unused"
  ) |>
  relocate(name, address, city, state, zip)

arrow::write_feather(
  hospitals,
  "data/hospitals.feather"
)
