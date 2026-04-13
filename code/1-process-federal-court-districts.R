library(tidyverse)
library(sf)
library(tigris)
library(tidylog)
library(geoarrow)

# must cite this per license
# http://doi.org/10.3886/E240727V2

# run this to correct corrupt geometry
# ogr2ogr -makevalid -nlt MULTIPOLYGON US_District_Court_Jurisdictions-corrected.shp US_District_Court_Jurisdictions.shp

federal_court_districts <- st_read(
  "inputs/us-district-court-jurisdictions-shapefile/US_District_Court_Jurisdictions-corrected.shp"
) |>
  st_transform(crs = 4326)

federal_court_districts |>
  tibble::as_tibble() |>
  arrow::write_parquet("data/federal-court-districts.parquet")

# run this to correct corrupt geometry
# ogr2ogr -makevalid -nlt MULTIPOLYGON US_CourtOfAppealsCircuits-corrected.shp US_CourtOfAppealsCircuits.shp

federal_court_circuits <- st_read(
  "inputs/us-courts-of-appeals-circuits-shapefile/US_CourtOfAppealsCircuits-corrected.shp"
) |>
  st_transform(crs = 4326)

federal_court_circuits |>
  tibble::as_tibble() |>
  arrow::write_parquet("data/federal-court-circuits.parquet")

# county_sf <-
#   tigris::counties(
#     # cb = TRUE,
#     year = 2024,
#     # resolution = "5m",
#     class = "sf",
#     progress = FALSE
#   ) |>
#   left_join(
#     # NOTE: U.S. minor outlying islands are not included in the TIGER line shapefiles so have no county assigned
#     tigris::fips_codes |>
#       distinct(state_code = state_code, STATE_NAME = state_name),
#     by = c("STATEFP" = "state_code")
#   ) |>
#   st_transform(crs = 4326) |>
#   select(
#     county_name = NAME,
#     state_name = STATE_NAME,
#     county_fips_code = GEOID,
#     state_fips_code = STATEFP,
#     geometry
#   )

# federal_court_districts_df <-
#   read_csv("inputs/federal_court_districts.csv", skip = 1) |>
#   mutate(county = str_remove_all(county, " County"))

# federal_court_districts_entire_states <-
#   federal_court_districts_df |>
#   filter(county == "All Counties") |>
#   select(-county)

# federal_court_districts_partial_states <-
#   federal_court_districts_df |>
#   filter(county != "All Counties")

# federal_court_districts_county_sf_partial_states <-
#   county_sf |>
#   anti_join(
#     federal_court_districts_entire_states,
#     by = c("state_name" = "state")
#   ) |>
#   anti_join(
#     federal_court_districts_partial_states,
#     by = c("county_name" = "county", "state_name" = "state")
#   )

# federal_court_districts_county_sf_entire_states <-
#   county_sf |>
#   inner_join(
#     federal_court_districts_entire_states,
#     by = c("state_name" = "state")
#   )

# federal_court_districts_county_sf <-
#   bind_rows(
#     federal_court_districts_county_sf_partial_states,
#     federal_court_districts_county_sf_entire_states
#   ) |>
#   # collapse into districts
#   summarise(
#     geometry = st_union(geometry),
#     .by = judicial_district
#   )
