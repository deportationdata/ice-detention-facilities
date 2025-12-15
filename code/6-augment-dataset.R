library(tidyverse)
library(sf)
library(tigris)

source("code/functions.R")

county_sf <-
  tigris::counties(
    # cb = TRUE,
    year = 2024,
    # resolution = "5m",
    class = "sf",
    progress = FALSE
  ) |>
  left_join(
    tigris::fips_codes |>
      distinct(state_code = state_code, STATE_NAME = state_name),
    by = c("STATEFP" = "state_code")
  ) |>
  st_transform(crs = 4326) |>
  select(
    county_name = NAME,
    state_name = STATE_NAME,
    county_fips_code = GEOID,
    state_fips_code = STATEFP,
    geometry
  )

# temporarily just use the facilities in the recent data
# facility_list <-
#   arrow::read_feather(
#     "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
#   ) |>
#   as_tibble() |>
#   # mutate(
#   #   cnt = sum(
#   #     year(book_in_date_time) == 2025 | year(book_out_date_time) == 2025
#   #   ),
#   #   .by = detention_facility_code
#   # ) |>
#   # filter(cnt >= 1) |>
#   distinct(detention_facility_code, name = detention_facility)

facility_list <-
  arrow::read_feather(
    "data/facilities-from-detentions.feather"
  ) |>
  as_tibble() |>
  select(
    detention_facility_code,
    name = detention_facility,
    first_book_in,
    latest_book_in = last_book_in
  )

best_values_wide <-
  arrow::read_feather(
    "data/facilities-best-values-wide.feather"
  )

# stats_from_detention_stints <- arrow::read_feather(
#   "data/facilities-from-detentions.feather"
# )

facility_final <-
  facility_list |>
  left_join(
    best_values_wide,
    by = "detention_facility_code"
  ) |>
  # left_join(
  #   stats_from_detention_stints |>
  #     select(
  #       detention_facility_code,
  #       first_book_in,
  #       latest_book_in = last_book_in
  #     ),
  #   by = "detention_facility_code"
  # ) |>
  relocate(
    detention_facility_code,
    name,
    address,
    city,
    state,
    zip,
    aor,
    type,
    type_detailed
  ) |>
  as_tibble()

# bring in geocoding

vera_df <- arrow::read_feather("data/facilities-vera.feather")

not_in_vera_geocoded_df <- arrow::read_feather(
  "data/facilities-not-in-vera-geocoded.feather"
)

geocode_df <-
  bind_rows(
    vera_df |> select(detention_facility_code, latitude, longitude),
    not_in_vera_geocoded_df |>
      select(detention_facility_code, latitude, longitude)
  )

# add lat lon from Vera data
facility_final <-
  facility_final |>
  left_join(
    geocode_df |> distinct(detention_facility_code, latitude, longitude),
    by = c("detention_facility_code")
  )

# facility_final |>
#   filter(is.na(address)) |>
#   select(1:2) |>
#   clipr::write_clip()

# bring in court data

# from https://www.uscourts.gov/file/18039/download map
circuits <- tribble(
  ~code , ~circuit ,
  # states
  "AL"  ,       11 ,
  "AK"  ,        9 ,
  "AZ"  ,        9 ,
  "AR"  ,        8 ,
  "CA"  ,        9 ,
  "CO"  ,       10 ,
  "CT"  ,        2 ,
  "DE"  ,        3 ,
  "FL"  ,       11 ,
  "GA"  ,       11 ,
  "HI"  ,        9 ,
  "ID"  ,        9 ,
  "IL"  ,        7 ,
  "IN"  ,        7 ,
  "IA"  ,        8 ,
  "KS"  ,       10 ,
  "KY"  ,        6 ,
  "LA"  ,        5 ,
  "ME"  ,        1 ,
  "MD"  ,        4 ,
  "MA"  ,        1 ,
  "MI"  ,        6 ,
  "MN"  ,        8 ,
  "MS"  ,        5 ,
  "MO"  ,        8 ,
  "MT"  ,        9 ,
  "NE"  ,        8 ,
  "NV"  ,        9 ,
  "NH"  ,        1 ,
  "NJ"  ,        3 ,
  "NM"  ,       10 ,
  "NY"  ,        2 ,
  "NC"  ,        4 ,
  "ND"  ,        8 ,
  "OH"  ,        6 ,
  "OK"  ,       10 ,
  "OR"  ,        9 ,
  "PA"  ,        3 ,
  "RI"  ,        1 ,
  "SC"  ,        4 ,
  "SD"  ,        8 ,
  "TN"  ,        6 ,
  "TX"  ,        5 ,
  "UT"  ,       10 ,
  "VT"  ,        2 ,
  "VA"  ,        4 ,
  "WA"  ,        9 ,
  "WV"  ,        4 ,
  "WI"  ,        7 ,
  "WY"  ,       10 ,
  # Non-states
  "PR"  ,        1 ,
  "VI"  ,        3 ,
  "GU"  ,        9 ,
  "MP"  ,        9
)

federal_court_districts_df <-
  read_csv("inputs/federal_court_districts.csv", skip = 1)

federal_court_districts_entire_states <-
  federal_court_districts_df |>
  filter(county == "All Counties") |>
  select(-county)

federal_court_districts_partial_states <-
  federal_court_districts_df |>
  filter(county != "All Counties")

federal_court_districts_county_sf_partial_states <-
  county_sf |>
  anti_join(
    federal_court_districts_entire_states,
    by = c("state_name" = "state")
  ) |>
  anti_join(
    federal_court_districts_partial_states,
    by = c("county_name" = "county", "state_name" = "state")
  )

federal_court_districts_county_sf_entire_states <-
  county_sf |>
  inner_join(
    federal_court_districts_entire_states,
    by = c("state_name" = "state")
  )

federal_court_districts_county_sf <-
  bind_rows(
    federal_court_districts_county_sf_partial_states,
    federal_court_districts_county_sf_entire_states
  ) |>
  # collapse into districts
  summarise(
    geometry = st_union(geometry),
    .by = judicial_district
  )

# bring in ICE field office

ice_field_offices <- sfarrow::st_read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-aor-shp.feather"
) |>
  st_transform(crs = 4326)

facility_final <-
  facility_final |>
  st_as_sf(
    coords = c("longitude", "latitude"),
    na.fail = FALSE,
    crs = 4326,
    remove = FALSE
  ) |>
  select(-circuit) |>
  left_join(
    circuits,
    by = c("state" = "code")
  ) |>
  st_join(
    federal_court_districts_county_sf, #|> mutate(in_districts = 1),
    join = st_within
  ) |>
  st_join(
    ice_field_offices |>
      filter(area_of_responsibility_name != "HQ") |>
      transmute(field_office = office_name), #|> mutate(in_aor = 1),
    join = st_within
  ) |>
  st_join(
    county_sf,
    join = st_within
  ) |>
  st_drop_geometry() |>
  as_tibble()

arrow::write_feather(
  facility_final,
  "data/facilities-augmented.feather"
)
