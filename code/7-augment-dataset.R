library(tidyverse)
library(sf)
library(tigris)

options(tigris_use_cache = TRUE)

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
  filter(last_book_in >= as.Date("2025-01-01")) |> # only keep facilities with book ins in 2025 or later
  select(detention_facility_code, first_book_in, last_book_in)

# best_values_wide <-
#   arrow::read_feather(
#     "data/facilities-best-values-wide.feather"
#   )

facility_latest_values <-
  arrow::read_feather(
    "data/facilities-latest-values-long.feather"
  )

facility_latest_values_wide <-
  facility_latest_values |>
  pivot_wider(
    id_cols = c(detention_facility_code, detention_facility_code_alt),
    names_from = variable,
    values_from = value
  )


# stats_from_detention_stints <- arrow::read_feather(
#   "data/facilities-from-detentions.feather"
# )

# bring in geocoding

# vera_df <- arrow::read_feather("data/facilities-vera.feather")

# not_in_vera_geocoded_df <- arrow::read_feather(
#   "data/facilities-not-in-vera-geocoded.feather"
# )

facilities_geocoded_df <- arrow::read_feather(
  "data/facilities-geocoded-exact.feather"
)

# facility_list |>
#   inner_join(facilities_geocoded_df, by = "detention_facility_code") |>
#   left_join(
#     vera_df |>
#       select(
#         detention_facility_code,
#         latitude_vera = latitude,
#         longitude_vera = longitude
#       ),
#     by = "detention_facility_code"
#   ) |>
#   mutate(
#     geometry_vera = map2(
#       longitude_vera,
#       latitude_vera,
#       ~ st_point(c(.x, .y))
#     ) |>
#       st_sfc(crs = 4326),
#     geometry_google = map2(longitude, latitude, ~ st_point(c(.x, .y))) |>
#       st_sfc(crs = 4326),
#     distance = st_distance(geometry_vera, geometry_google, by_element = TRUE) |>
#       as.numeric()
#   )

# geocode_df <-
# bind_rows(
#   vera_df |>
#     select(detention_facility_code, latitude, longitude) |>
#     anti_join(not_in_vera_geocoded_df, by = "detention_facility_code"),
#   not_in_vera_geocoded_df |>
#     select(detention_facility_code, latitude, longitude)
# )

# bring in court data

federal_circuit_courts_sf <-
  sfarrow::st_read_feather("data/federal-court-circuits.feather")

federal_district_courts_sf <-
  sfarrow::st_read_feather("data/federal-court-districts.feather")

# bring in ICE field office

ice_field_offices <- sfarrow::st_read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-aor-shp.feather"
) |>
  st_transform(crs = 4326)

name_code_match <-
  arrow::read_feather(
    "data/facilities-name-code-match.feather"
  )

facility_final <-
  facility_list |>
  as_tibble() |>
  left_join(
    facility_latest_values_wide,
    by = "detention_facility_code"
  ) |>
  select(-state) |>
  left_join(
    name_code_match |>
      distinct(
        detention_facility_code,
        state
      ),
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
  # left_join(
  #   geocode_df |> distinct(detention_facility_code, latitude, longitude),
  #   by = c("detention_facility_code")
  # ) |>
  left_join(
    facilities_geocoded_df |>
      select(detention_facility_code, latitude, longitude),
    by = "detention_facility_code"
  ) |>
  select(-aor) |>
  st_as_sf(
    coords = c("longitude", "latitude"),
    na.fail = FALSE,
    crs = 4326,
    remove = FALSE
  ) |>
  st_join(
    federal_circuit_courts_sf |>
      select(federal_court_circuit_habeas = NAME),
    join = st_within
  ) |>
  st_join(
    federal_district_courts_sf |> select(federal_court_district_habeas = NAME),
    join = st_within
  ) |>
  st_join(
    ice_field_offices |>
      filter(area_of_responsibility_name != "HQ") |>
      transmute(field_office = office_name), #|> mutate(in_aor = 1),
    join = st_within
  ) |>
  st_join(
    county_sf |> select(-state_name) |> rename(county = county_name),
    join = st_within
  ) |>
  mutate(
    federal_court_circuit_habeas = case_when(
      # 48 USC 1613(a) specifies that the Virgin Islands are in the 3st Circuit
      state == "VI" ~ "THIRD CIRCUIT",
      # Rasul v Bush specifies that the Guantanamo Bay detention facility is in the District of Columbia Circuit
      detention_facility_code %in%
        c("GTMOBCU", "GTMODCU", "GTMOACU") ~ "DISTRICT OF COLUMBIA CIRCUIT",
      TRUE ~ federal_court_circuit_habeas
    ),
    federal_court_district_habeas = case_when(
      # 48 USC 1611(b) specifies that the Virgin Islands are served by the Virgin Islands District Court
      state == "VI" ~ "Virgin Islands District Court",
      # Rasul v Bush specifies that the Guantanamo Bay detention facility is in the jurisdiction of the District of District of Columbia
      detention_facility_code %in%
        c("GTMOBCU", "GTMODCU", "GTMOACU") ~ "District of District of Columbia",
      TRUE ~ federal_court_district_habeas
    ),
    # convert to integer
    federal_court_circuit_habeas = federal_court_circuit_habeas |>
      str_remove(" CIRCUIT") |>
      recode_values(
        "FIRST" ~ "1",
        "SECOND" ~ "2",
        "THIRD" ~ "3",
        "FOURTH" ~ "4",
        "FIFTH" ~ "5",
        "SIXTH" ~ "6",
        "SEVENTH" ~ "7",
        "EIGHTH" ~ "8",
        "NINTH" ~ "9",
        "TENTH" ~ "10",
        "ELEVENTH" ~ "11",
        "DISTRICT OF COLUMBIA" ~ "DC"
      ) |>
      as.integer(),
    # Guantanamo Bay detention facility is served by the Miami field office see:
    # https://www.ice.gov/detain/detention-facilities/naval-station-guantanamo-bay
    field_office = case_when(
      detention_facility_code %in% c("GTMOBCU", "GTMODCU", "GTMOACU") ~ "Miami",
      TRUE ~ field_office
    )
  ) |>
  st_drop_geometry() |>
  as_tibble() |>
  relocate(county, county_fips_code, .before = state) |>
  relocate(state_fips_code, .after = state) |>
  relocate(detention_facility_code_alt, .after = detention_facility_code)

arrow::write_feather(
  facility_final,
  "data/facilities-augmented.feather"
)
