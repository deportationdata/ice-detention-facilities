library(tidyverse)
library(sf)
library(tidylog)

prison_boundaries <- st_read(
  "inputs/prison-boundaries-1-shapefile/Prison_Boundaries.shp"
)

prison_boundaries <-
  prison_boundaries |>
  # calculate centroid
  st_transform(crs = 4326) |>
  st_make_valid() |>
  st_centroid() |>
  mutate(
    longitude = st_coordinates(geometry)[, 1],
    latitude = st_coordinates(geometry)[, 2]
  ) |>
  st_drop_geometry() |>
  as_tibble() |>
  transmute(
    hifld_id = FACILITYID,
    name = NAME,
    address = ADDRESS,
    city = CITY,
    state = ZIP,
    zip = ZIP,
    type = TYPE,
    status = STATUS,
    population = POPULATION,
    latitude,
    longitude,
    date = as.Date("2024-10-07") # approximate date of data release based on file path
  )

arrow::write_feather(
  prison_boundaries,
  "data/hifld-prisons.feather"
)
