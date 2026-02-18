library(tidyverse)
library(sf)
library(tidygeocoder)

# temporarily just use the facilities in the recent data
# facility_list <-
#   arrow::read_feather(
#     # "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
#     "~/github/ice/data/detention-stints-latest.feather"
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

best_values_wide <-
  arrow::read_feather(
    "data/facilities-best-values-wide.feather"
  )

facilities_to_geocode <-
  best_values_wide |>
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
  as_tibble() |>
  filter(!is.na(address)) |>
  select(detention_facility_code, address, city, state, zip) |>
  distinct() |>
  mutate(
    full_address = glue::glue("{address}, {city}, {state} {zip}") |>
      str_to_lower()
  )

# facilities_geocoded_google <-
#   facilities_to_geocode |>
#   geocode(
#     full_address,
#     method = 'google',
#     lat = latitude,
#     long = longitude,
#     limit = 1,
#     full_results = TRUE
#   )

# write_rds(
#   facilities_geocoded_google,
#   file = "data/facilities_geocoded_google_16feb26.rds"
# )

facilities_geocoded_google <- read_rds(
  "data/facilities_geocoded_google_16feb26.rds"
)

# facilities_geocoded_census <-
#   facilities_to_geocode |>
#   geocode(
#     full_address,
#     method = 'census',
#     lat = latitude,
#     long = longitude,
#     limit = 1,
#     full_results = TRUE
#   )

# write_rds(
#   facilities_geocoded_census,
#   file = "data/facilities_geocoded_census_16feb26.rds"
# )

facilities_geocoded_census <- read_rds(
  "data/facilities_geocoded_census_16feb26.rds"
)

# facilities_geocoded_arcgis <-
#   facilities_to_geocode |>
#   geocode(
#     full_address,
#     method = 'arcgis',
#     lat = latitude,
#     long = longitude,
#     limit = 1,
#     full_results = TRUE
#   )

# write_rds(
#   facilities_geocoded_arcgis,
#   file = "data/facilities_geocoded_arcgis_16feb26.rds"
# )

facilities_geocoded_arcgis <- read_rds(
  "data/facilities_geocoded_arcgis_16feb26.rds"
)


facilities_geocoded_df <-
  bind_rows(
    "google" = facilities_geocoded_google,
    # "census" = facilities_geocoded_census |>
    #   rename(formatted_address = matched_address),
    "arcgis" = facilities_geocoded_arcgis |>
      rename(formatted_address = arcgis_address),
    .id = "geocoder"
  ) |>
  arrange(detention_facility_code, geocoder) |>
  # mutate(
  #   is_geocoded = !is.na(latitude) & !is.na(longitude)
  # ) |>
  mutate(
    any_geocoded = any(!is.na(latitude), !is.na(longitude)),
    is_exact_google = map_lgl(
      address_components,
      \(ac_df) {
        any(
          unlist(ac_df$types, use.names = FALSE) %in%
            c("street_number", "premise")
        )
      }
    ),
    is_exact_arcgis = geocoder == "arcgis" &
      attributes.Addr_type %in%
        c(
          "PointAddress",
          "StreetAddress",
          "Subaddress",
          "StreetAddress",
          "StreetInt"
        ),
    is_exact = case_when(
      geocoder == "google" ~ is_exact_google,
      geocoder == "arcgis" ~ is_exact_arcgis,
      TRUE ~ NA
    ),
    # geocoded_google = any(geocoder == "google" & is_geocoded),
    # geocoded_census = any(geocoder == "census" & is_geocoded),
    # geocoded_arcgis = any(geocoder == "arcgis" & is_geocoded),
    .by = detention_facility_code
  ) |>
  filter(any_geocoded == TRUE, any(is_exact_arcgis)) |>
  mutate(
    is_exact_google_any = any(is_exact_google),
    is_exact_arcgis_any = any(is_exact_arcgis),
    .by = detention_facility_code
  ) |>
  filter(
    is_exact_google_any == TRUE | is_exact_arcgis_any == TRUE
  ) |>
  filter(
    (is_exact_google_any == TRUE &
      is_exact_arcgis_any == FALSE &
      geocoder == "google") |
      (is_exact_google_any == FALSE &
        is_exact_arcgis_any == TRUE &
        geocoder == "arcgis") |
      (is_exact_google_any == TRUE &
        is_exact_arcgis_any == TRUE &
        geocoder == "arcgis")
  ) |>
  select(
    detention_facility_code,
    geocoder,
    latitude,
    longitude,
    full_address,
    formatted_address
  )

arrow::write_feather(
  facilities_geocoded_df,
  "data/facilities-geocoded-exact.feather"
)
# st_as_sf(
#   coords = c("longitude", "latitude"),
#   crs = 4326,
#   remove = FALSE,
#   na.fail = FALSE
# ) |>
# mutate(
#   dist_to_google = st_distance(
#     geometry,
#     geometry[geocoder == "google"]
#   ),
#   dist_to_census = st_distance(
#     geometry,
#     geometry[geocoder == "census"]
#   ),
#   .by = detention_facility_code
# )
# select(
#   detention_facility_code,
#   geocoder,
#   full_address,
#   formatted_address,
#   dist_to_google,
#   dist_to_argis
# ) |>

# filter(
#   !all(
#     dist_to_google < units::set_units(500, "m") |
#       dist_to_argis < units::set_units(500, "m")
#   ),
#   .by = detention_facility_code
# )

geocoded_firm <-
  facilities_geocoded_df |>
  filter(
    geocoder == "arcgis",
    as.logical(dist_to_google < units::set_units(500, "m")) |
      as.logical(dist_to_census < units::set_units(500, "m"))
  ) |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    full_address,
    formatted_address
  )


geocoded_uncertain <-
  facilities_geocoded_df |>
  filter(
    geocoder == "arcgis",
    as.logical(dist_to_google > units::set_units(500, "m")) &
      as.logical(dist_to_census > units::set_units(500, "m"))
  ) |>
  select(
    detention_facility_code,
    latitude,
    longitude,
    full_address,
    formatted_address
  )

pivot_wider(
  id_cols = detention_facility_code,
  names_from = geocoder,
  values_from = is_geocoded
)

filter(any_error, geocoder == "census") |>
  select(1:2, full_address, formatted_address, latitude, longitude) |>
  print(n = 500)

facilities_geocoded_df <-
  facilities_geocoded_google |>
  select(
    detention_facility_code,
    address,
    city,
    full_address,
    latitude,
    longitude,
    formatted_address,
    address_components
  ) |>
  unnest(address_components) |>
  unnest(types) |>
  mutate(
    is_exact = any(types %in% c("street_number", "premise")),
    .by = detention_facility_code
  ) |>
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
    address,
    city,
    full_address,
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
    formatted_address = if ("premise" %in% names(.data)) {
      str_remove(formatted_address, glue::glue("{premise}, "))
    } else {
      formatted_address
    }
  ) |>
  filter(is_exact == FALSE) |>
  View()


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
    address,
    formatted_address,
    city,
    state,
    county
  )

arrow::write_feather(
  facilities_geocoded_df,
  "data/facilities-not-in-vera-geocoded.feather"
)
