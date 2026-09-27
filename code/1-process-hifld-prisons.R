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
    state = STATE,
    zip = ZIP,
    type = TYPE,
    status = STATUS,
    population = POPULATION,
    latitude,
    longitude,
    # when HIFLD read each record's source, and when it last checked the record against imagery
    source_date = as.Date(SOURCEDATE),
    validated_date = as.Date(VAL_DATE),
    source_url = na_if(SOURCE, "NOT AVAILABLE"),
    date = as.Date("2024-10-07") # approximate date of data release based on file path
  )

arrow::write_parquet(
  prison_boundaries,
  "data/hifld-prisons.parquet"
)
