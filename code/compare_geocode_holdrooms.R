library(tidyverse)
library(sf)
library(tidygeocoder)

hold_rooms <- arrow::read_parquet("data/noccc-hold-rooms.parquet")

holdrooms_geocoded_arcgis <-
  hold_rooms |>
  mutate(address_full = glue::glue("{address}, {city}, {state} {zip}")) |>
  geocode(
    address_full,
    method = 'arcgis',
    lat = latitude,
    long = longitude,
    limit = 1,
    full_results = TRUE
  )

holdrooms_geocoded_arcgis |>
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) |>
  st_write("~/Downloads/holdrooms.kml")

geocoded_facilities <- arrow::read_parquet(
  "data/facilities-geocoded-exact.parquet"
)

holdrooms_geocoded_arcgis |>
  select(
    detention_facility_code,
    city,
    state,
    address = address_full,
    arcgis_address,
    latitude,
    longitude
  ) |>
  left_join(
    geocoded_facilities |>
      select(
        detention_facility_code,
        address = address_full,
        latitude,
        longitude
      ),
    by = "detention_facility_code",
    suffix = c("_holdroom", "_facility")
  ) |>
  mutate(
    distance_meters = geosphere::distHaversine(
      cbind(longitude_holdroom, latitude_holdroom),
      cbind(longitude_facility, latitude_facility)
    )
  ) |>
  arrange(desc(distance_meters)) |>
  select(
    detention_facility_code,
    address_holdroom,
    address_facility,
    distance_meters
  )
