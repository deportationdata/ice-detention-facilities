library(tidyverse)
library(sfarrow)
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

cbsa_sf <-
  tigris::core_based_statistical_areas(
    year = 2024,
    class = "sf",
    progress = FALSE
  ) |>
  st_transform(crs = 4326) |>
  select(
    core_based_statistical_area = NAME,
    core_based_statistical_area_code = GEOID,
    core_based_statistical_area_type = LSAD,
    geometry
  ) |>
  mutate(
    core_based_statistical_area_type = recode_values(
      core_based_statistical_area_type,
      "M1" ~ "Metro",
      "M2" ~ "Micro"
    )
  )

csa_sf <-
  tigris::combined_statistical_areas(
    year = 2024,
    class = "sf",
    progress = FALSE
  ) |>
  st_transform(crs = 4326) |>
  select(
    combined_statistical_area = NAME,
    combined_statistical_area_code = GEOID,
    geometry
  )

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
  ) |>
  left_join(
    facility_latest_values |>
      filter(variable %in% c("type", "type_detailed")) |>
      mutate(variable = str_c(variable, "_all")) |>
      pivot_wider(
        id_cols = detention_facility_code,
        names_from = variable,
        values_from = value_all
      ),
    by = "detention_facility_code"
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

facilities_manual_latlon <-
  arrow::read_parquet("data/facilities-manual.parquet") |>
  filter(!is.na(latitude), !is.na(longitude)) |>
  select(
    detention_facility_code,
    latitude_manual = latitude,
    longitude_manual = longitude
  )

# bring in court data

federal_circuit_courts_sf <-
  sfarrow::st_read_parquet("data/federal-court-circuits.parquet")

federal_district_courts_sf <-
  sfarrow::st_read_parquet("data/federal-court-districts.parquet")

# bring in ICE field office — remote parquet (sfarrow-written)
ice_field_offices <-
  sfarrow::st_read_parquet(
    "https://github.com/deportationdata/ice-offices/raw/refs/heads/main/data/ice-aor-shp.parquet"
  ) |>
  sf::st_transform(crs = 4326)

# rolling 365-day window stats computed from detention stints (covers
# hospitals and other facilities missing from the ICE daily-population feed).
# See code/1-process-individual-count.R for the computation.
facility_window_stats <-
  arrow::read_parquet(
    "data/facility-individual-counts-2025-2026.parquet"
  )

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
  left_join(
    facilities_geocoded_df |>
      select(detention_facility_code, latitude, longitude),
    by = "detention_facility_code"
  ) |>
  left_join(facilities_manual_latlon, by = "detention_facility_code") |>
  mutate(
    latitude = coalesce(latitude_manual, latitude),
    longitude = coalesce(longitude_manual, longitude)
  ) |>
  select(-latitude_manual, -longitude_manual) |>
  select(-aor) |>
  # Hold rooms whose currently-picked address comes from NOCCC research that
  # doesn't hold up on review (see audit notes). Blank address/city/zip/lat/lon
  # so downstream spatial joins (county, CBSA, CSA, field office, courts) also
  # come back NA; keep state so the record still sits in the right jurisdiction.
  mutate(
    across(
      c(address_full, address, city, zip, latitude, longitude),
      ~ if_else(
        str_ends(detention_facility_code, "HOLD") &
          # don't blank out hand-verified hold facilities
          !detention_facility_code %in%
            c(
              "SNDHOLD",
              "SNJHOLD",
              "VENHOLD",
              "ALMHOLD",
              "DENHOLD",
              "DURHOLD",
              "GSCHOLD",
              "ORLHOLD",
              "ATLHOLD",
              "BOSHOLD",
              "BALHOLD",
              "SPMHOLD",
              "EUGHOLD",
              "POOHOLD",
              "YRKHOLD",
              "PROHOLD",
              "OGUHOLD",
              "STAHOLD",
              "OKCHOLD",
              "LOSHOLD",
              "BSAHOLD",
              "RMKHOLD",
              "PRLHOLD",
              "SEAHOLD",
              "HOUHOLD",
              "JAXHOLD",
              "WASHOLD",
              "OMAHOLD",
              "NORHOLD",
              "IMPHOLD",
              "NYCHOLD",
              "YAKHOLD",
              "MEDHOLD",
              "HHWHOLD",
              "ANCHOLD",
              "IWAHOLD",
              "SAJHOLD",
              "RCMHOLD",
              "CBPHOLD"
            ),
        NA,
        .x
      )
    )
  ) |>
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
  st_join(cbsa_sf, join = st_within) |>
  st_join(csa_sf, join = st_within) |>
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
  relocate(
    county,
    county_fips_code,
    core_based_statistical_area,
    core_based_statistical_area_code,
    core_based_statistical_area_type,
    combined_statistical_area,
    combined_statistical_area_code,
    .before = state
  ) |>
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
  anti_join(duplicate_facilities_to_remove, by = "detention_facility_code") |>
  left_join(facility_window_stats, by = "detention_facility_code")

arrow::write_parquet(
  facility_final,
  "data/facilities-augmented.parquet"
)
