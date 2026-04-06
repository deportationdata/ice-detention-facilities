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

facility_latest_values <-
  arrow::read_feather(
    "data/facilities-latest-values-long.feather"
  )

facility_list <-
  arrow::read_feather(
    "data/facilities-attributes-cleaned-with-codes.feather"
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

# facility_latest_values |>
#   select(detention_facility_code, variable, value, source, date) |>
#   filter(detention_facility_code == "ELPCOTX")

facilities_to_geocode <-
  facility_latest_values |>
  inner_join(
    facility_list,
    by = "detention_facility_code"
  ) |>
  filter(variable == "address_full") |>
  select(detention_facility_code, address_full = value)

# facilities_to_geocode |>
#   filter(detention_facility_code == "ELPCOTX") |>
#   geocode(address_full, method = "arcgis") |>
#   as.data.frame()

facilities_geocoded_google <-
  facilities_to_geocode |>
  geocode(
    address_full,
    method = 'google',
    lat = latitude,
    long = longitude,
    limit = 1,
    full_results = TRUE
  )

# write_rds(
#   facilities_geocoded_google,
#   file = "data/facilities_geocoded_google_21feb26.rds"
# )

facilities_geocoded_google <- read_rds(
  "data/facilities_geocoded_google_21feb26.rds"
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

# facilities_geocoded_census <- read_rds(
#   "data/facilities_geocoded_census_16feb26.rds"
# )

# facilities_geocoded_arcgis <-
#   facilities_to_geocode |>
#   geocode(
#     address_full,
#     method = 'arcgis',
#     lat = latitude,
#     long = longitude,
#     limit = 1,
#     full_results = TRUE
#   )

# write_rds(
#   facilities_geocoded_arcgis,
#   file = "data/facilities_geocoded_arcgis_21feb26.rds"
# )

facilities_geocoded_arcgis <- read_rds(
  "data/facilities_geocoded_arcgis_21feb26.rds"
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
    address_full,
    formatted_address
  )

arrow::write_feather(
  facilities_geocoded_df,
  "data/facilities-geocoded-exact.feather"
)

vera <- arrow::read_feather("data/facilities-vera.feather")

marshall <- arrow::read_feather("data/facilities-marshall.feather")

facilities_geocoded_df |>
  left_join(
    facility_latest_values |>
      filter(variable == "name") |>
      rename(name = value) |>
      distinct(detention_facility_code, name),
    by = "detention_facility_code"
  ) |>
  # left_join(
  #   vera |>
  #     select(
  #       detention_facility_code,
  #       latitude_vera = latitude,
  #       longitude_vera = longitude,
  #       address_vera = address,
  #       city_vera = city
  #     ),
  #   by = "detention_facility_code"
  # ) |>
  # left_join(
  #   marshall |>
  #     select(
  #       detention_facility_code,
  #       latitude_marshall = latitude,
  #       longitude_marshall = longitude,
  #       address_marshall = address,
  #       city_marshall = city
  #     ),
  #   by = "detention_facility_code"
  # ) |>
  # mutate(
  #   latitude_jan26 = latitude,
  #   longitude_jan26 = longitude
  # ) |>
  # filter(
  #   !is.na(latitude_vera),
  #   !is.na(longitude_vera),
  #   !is.na(latitude_marshall),
  #   !is.na(longitude_marshall)
  # ) |>
  # mutate(
  #   distance_vera = sf::st_distance(
  #     sf::st_as_sf(
  #       cur_data(),
  #       coords = c("longitude_vera", "latitude_vera"),
  #       crs = 4326,
  #       remove = FALSE
  #     ),
  #     sf::st_as_sf(
  #       cur_data(),
  #       coords = c("longitude_jan26", "latitude_jan26"),
  #       crs = 4326,
  #       remove = FALSE
  #     ),
  #     by_element = TRUE
  #   ),
  #   distance_marshall = sf::st_distance(
  #     sf::st_as_sf(
  #       cur_data(),
  #       coords = c("longitude_marshall", "latitude_marshall"),
  #       crs = 4326,
  #       remove = FALSE
  #     ),
  #     sf::st_as_sf(
  #       cur_data(),
  #       coords = c("longitude_jan26", "latitude_jan26"),
  #       crs = 4326,
  #       remove = FALSE
  #     ),
  #     by_element = TRUE
  #   )
  # ) |>
  # filter(
  #   distance_vera > units::set_units(5000, "meters") |
  #     distance_marshall > units::set_units(5000, "meters")
  # ) |>
  # filter(
  #   !detention_facility_code %in%
  #     c(
  #       "BOPCNV",
  #       "BOPTCN",
  #       "EHDLGTX",
  #       "GREENMO",
  #       "GSCHOLD",
  #       "HARHOLD",
  #       "HENRIVA",
  #       "JMHOSFL",
  #       "KENTOKY",
  #       "KERCOCA",
  #       "KRHUBFL",
  #       "KRO",
  #       "KROHOLD",
  #       "LARELKY",
  #       "MADISMS",
  #       "MCCLETX",
  #       "MONTGAL",
  #       "NYCHOLD",
  #       "PLATTMO",
  #       "RAPPSVA",
  #       "SLRDCAZ",
  #       "VPREGVA"
  #     )
  # ) |>
  select(
    -geocoder,
    # -contains("latitude"),
    # -contains("longitude"),
    -formatted_address
  ) |>
  # select(contains("latitude"), contains("longitude")) |>
  as.data.frame()

# # st_as_sf(
# #   coords = c("longitude", "latitude"),
# #   crs = 4326,
# #   remove = FALSE,
# #   na.fail = FALSE
# # ) |>
# # mutate(
# #   dist_to_google = st_distance(
# #     geometry,
# #     geometry[geocoder == "google"]
# #   ),
# #   dist_to_census = st_distance(
# #     geometry,
# #     geometry[geocoder == "census"]
# #   ),
# #   .by = detention_facility_code
# # )
# # select(
# #   detention_facility_code,
# #   geocoder,
# #   full_address,
# #   formatted_address,
# #   dist_to_google,
# #   dist_to_argis
# # ) |>

# # filter(
# #   !all(
# #     dist_to_google < units::set_units(500, "m") |
# #       dist_to_argis < units::set_units(500, "m")
# #   ),
# #   .by = detention_facility_code
# # )

# geocoded_firm <-
#   facilities_geocoded_df |>
#   filter(
#     geocoder == "arcgis",
#     as.logical(dist_to_google < units::set_units(500, "m")) |
#       as.logical(dist_to_census < units::set_units(500, "m"))
#   ) |>
#   select(
#     detention_facility_code,
#     latitude,
#     longitude,
#     full_address,
#     formatted_address
#   )

# geocoded_uncertain <-
#   facilities_geocoded_df |>
#   filter(
#     geocoder == "arcgis",
#     as.logical(dist_to_google > units::set_units(500, "m")) &
#       as.logical(dist_to_census > units::set_units(500, "m"))
#   ) |>
#   select(
#     detention_facility_code,
#     latitude,
#     longitude,
#     full_address,
#     formatted_address
#   )

# pivot_wider(
#   id_cols = detention_facility_code,
#   names_from = geocoder,
#   values_from = is_geocoded
# )

# filter(any_error, geocoder == "census") |>
#   select(1:2, full_address, formatted_address, latitude, longitude) |>
#   print(n = 500)

# facilities_geocoded_df <-
#   facilities_geocoded_google |>
#   select(
#     detention_facility_code,
#     address,
#     city,
#     full_address,
#     latitude,
#     longitude,
#     formatted_address,
#     address_components
#   ) |>
#   unnest(address_components) |>
#   unnest(types) |>
#   mutate(
#     is_exact = any(types %in% c("street_number", "premise")),
#     .by = detention_facility_code
#   ) |>
#   filter(
#     types %in%
#       c(
#         "locality",
#         "administrative_area_level_1",
#         "administrative_area_level_2",
#         "premise"
#       )
#   ) |>
#   select(
#     detention_facility_code,
#     latitude,
#     longitude,
#     address,
#     city,
#     full_address,
#     formatted_address,
#     types,
#     short_name,
#     is_exact
#   ) |>
#   pivot_wider(
#     names_from = types,
#     values_from = short_name
#   ) |>
#   mutate(
#     formatted_address = if ("premise" %in% names(.data)) {
#       str_remove(formatted_address, glue::glue("{premise}, "))
#     } else {
#       formatted_address
#     }
#   ) |>
#   filter(is_exact == FALSE) |>
#   View()

# filter(is_exact == TRUE, !is.na(latitude), !is.na(longitude)) |>
#   rename(
#     state = administrative_area_level_1,
#     county = administrative_area_level_2,
#     city = locality
#   ) |>
#   select(
#     detention_facility_code,
#     latitude,
#     longitude,
#     address,
#     formatted_address,
#     city,
#     state,
#     county
#   )

# arrow::write_feather(
#   facilities_geocoded_df,
#   "data/facilities-not-in-vera-geocoded.feather"
# )
