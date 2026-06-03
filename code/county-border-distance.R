library(tidyverse)
library(sf)
library(tigris)

options(tigris_use_cache = TRUE)

county_borders <-
  tigris::counties(year = 2024, class = "sf", progress = FALSE) |>
  st_transform(crs = 4326) |>
  st_cast("MULTILINESTRING") |>
  select(geometry)

facilities <-
  arrow::read_parquet("data/facilities-augmented.parquet") |>
  filter(!is.na(latitude), !is.na(longitude)) |>
  st_as_sf(
    coords = c("longitude", "latitude"),
    crs = 4326,
    remove = FALSE
  )

nearest_idx <- st_nearest_feature(facilities, county_borders)

facilities$distance_to_county_border_m <-
  as.numeric(st_distance(
    facilities,
    county_borders[nearest_idx, ],
    by_element = TRUE
  ))

facility_county_border_distance <-
  facilities |>
  st_drop_geometry() |>
  as_tibble() |>
  select(detention_facility_code, name, county, distance_to_county_border_m)

arrow::write_parquet(
  facility_county_border_distance,
  "data/facilities-county-border-distance.parquet"
)
