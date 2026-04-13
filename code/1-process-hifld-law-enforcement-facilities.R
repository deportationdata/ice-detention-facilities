library(tidyverse)
library(sf)
library(tidylog)

local_law_enforcement_facilities <- st_read(
  "inputs/local-law-enforcement-locations-shapefile/Local_Law_Enforcement.shp"
)

local_law_enforcement_facilities <-
  local_law_enforcement_facilities |>
  st_drop_geometry() |>
  as_tibble() |>
  transmute(
    hifld_id = as.character(ID),
    name = NAME,
    address = ADDRESS,
    city = CITY,
    state = STATE,
    zip = ZIP,
    type = TYPE,
    status = STATUS,
    latitude = LATITUDE,
    longitude = LONGITUDE,
    date = as.Date("2024-10-07") # approximate date of data release based on file path
  )

arrow::write_parquet(
  local_law_enforcement_facilities,
  "data/hifld-local-law-enforcement-facilities.parquet"
)
