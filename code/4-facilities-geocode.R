library(tidyverse)
library(sf)
library(tidygeocoder)

# temporarily just use the facilities in the recent data
facility_list <-
  arrow::read_feather(
    "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
  ) |>
  as_tibble() |>
  # mutate(
  #   cnt = sum(
  #     year(book_in_date_time) == 2025 | year(book_out_date_time) == 2025
  #   ),
  #   .by = detention_facility_code
  # ) |>
  # filter(cnt >= 1) |>
  distinct(detention_facility_code, name = detention_facility)

best_values_wide <-
  arrow::read_feather(
    "data/facilities-best-values-wide.feather"
  )

facility_final <-
  facility_list |>
  left_join(
    best_values_wide,
    by = "detention_facility_code"
  ) |>
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


vera_df <- arrow::read_feather("data/facilities-vera.feather")

# add lat lon from Vera data
facility_without_geocode <-
  facility_final |>
  anti_join(
    vera_df |> select(detention_facility_code, latitude, longitude),
    by = c("detention_facility_code")
  ) |>
  filter(!is.na(address))

facilities_geocoded <-
  facility_without_geocode |>
  select(detention_facility_code, address, city, state, zip) |>
  distinct() |>
  mutate(full_address = glue::glue("{address}, {city}, {state} {zip}")) |>
  geocode(
    full_address,
    method = 'google',
    lat = latitude,
    long = longitude,
    limit = 1,
    full_results = TRUE
  )

facilities_geocoded_df <-
  facilities_geocoded |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    formatted_address,
    address_components
  ) |>
  unnest(address_components) |>
  unnest(types) |>
  group_by(detention_facility_code) |>
  mutate(
    is_exact = any(types %in% c("street_number", "premise"))
  ) |>
  ungroup() |>
  filter(
    types %in%
      c(
        "locality",
        "administrative_area_level_1",
        "administrative_area_level_2",
        "premise"
      )
  ) |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    formatted_address,
    types,
    short_name,
    is_exact
  ) |>
  pivot_wider(
    names_from = types,
    values_from = short_name
  ) |>
  mutate(
    formatted_address = if ("premise" %in% names(cur_data())) {
      str_remove(formatted_address, glue::glue("{premise}, "))
    } else {
      formatted_address
    }
  ) |>
  filter(is_exact == TRUE, !is.na(latitude), !is.na(longitude)) |>
  rename(
    state = administrative_area_level_1,
    county = administrative_area_level_2,
    city = locality
  ) |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    formatted_address,
    city,
    state,
    county
  )

arrow::write_feather(
  facilities_geocoded_df,
  "data/facilities-not-in-vera-geocoded.feather"
)
