library(tidyverse)
library(geoarrow)
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
#   arrow::read_parquet(
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

# bring in all facilities in detention file plus in any ICE source (DTM, web site, etc.)
facility_list <-
  arrow::read_parquet(
    "data/facilities-attributes-cleaned-with-codes.parquet"
  ) |>
  filter(
    source %in%
      c(
        "05655",
        "2015",
        "2017",
        "22955",
        "41855",
        "51185",
        "dedicated",
        "detention_management",
        "detentions",
        # "eoir",
        "hrw",
        "website"
      ),
    date >= as.Date("2025-01-01")
  ) |>
  distinct(detention_facility_code)

facility_latest_values <-
  arrow::read_parquet(
    "data/facilities-latest-values-long.parquet"
  )

facility_latest_values_wide <-
  facility_latest_values |>
  pivot_wider(
    id_cols = c(detention_facility_code, detention_facility_code_alt),
    names_from = variable,
    values_from = value
  )

# facilities_open_dates <-
#   arrow::read_parquet(
#     "data/facilities-from-detentions.parquet"
#   ) |>
#   as_tibble() |>
#   select(detention_facility_code, first_book_in, last_book_in)

facilities_geocoded_df <- arrow::read_parquet(
  "data/facilities-geocoded-exact.parquet"
)

# bring in court data

federal_circuit_courts_sf <-
  arrow::read_parquet("data/federal-court-circuits.parquet") |>
  sf::st_as_sf()

federal_district_courts_sf <-
  arrow::read_parquet("data/federal-court-districts.parquet") |>
  sf::st_as_sf()

# bring in ICE field office — remote feather (sfarrow-written); download + read
ice_field_offices <- local({
  tf <- tempfile(fileext = ".feather")
  download.file(
    "https://github.com/deportationdata/ice-offices/raw/refs/heads/main/data/ice-aor-shp.feather",
    tf,
    mode = "wb",
    quiet = TRUE
  )
  arrow::read_feather(tf) |> sf::st_as_sf()
}) |>
  st_transform(crs = 4326)

name_code_match <-
  arrow::read_parquet(
    "data/facilities-name-code-match.parquet"
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
  # left_join(facilities_open_dates, by = "detention_facility_code") |>
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
    # simplify circuit names
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
      ),
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

duplicate_facilities_to_remove <-
  facility_final |>
  filter(!is.na(detention_facility_code_alt)) |>
  select(detention_facility_code = detention_facility_code_alt, name) |>
  distinct()

# remove the extra rows for facilities that have two codes
facility_final <-
  facility_final |>
  anti_join(duplicate_facilities_to_remove, by = "detention_facility_code")

individual_counts <-
  arrow::read_parquet("data/facility-individual-counts-2025-2026.parquet")

facility_final <-
  facility_final |>
  left_join(individual_counts, by = "detention_facility_code")

arrow::write_parquet(
  facility_final,
  "data/facilities-augmented.parquet"
)
