library(tidyverse)
library(tidygeocoder)

facility_latest_values <-
  arrow::read_parquet(
    "data/facilities-latest-values-long.parquet"
  )

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
        "hrw",
        "website"
      ),
    date >= as.Date("2025-01-01")
  ) |>
  distinct(detention_facility_code)

facilities_to_geocode <-
  facility_latest_values |>
  inner_join(facility_list, by = "detention_facility_code") |>
  filter(variable == "address_full", !is.na(value)) |>
  select(detention_facility_code, address_full = value)

# Caches are keyed by address_full (not detention_facility_code), so renaming a
# code doesn't invalidate cached coords. Each run appends just the new addresses.
# any_of() lets the script tolerate older snapshots that still carry a stale
# detention_facility_code column.
google_cache <- read_rds("data/facilities-geocoded-google.rds") |>
  select(-any_of("detention_facility_code"))
new_for_google <- setdiff(
  facilities_to_geocode$address_full, google_cache$address_full
)
if (length(new_for_google) > 0 && nzchar(Sys.getenv("GOOGLEGEOCODE_API_KEY"))) {
  google_cache <-
    bind_rows(
      google_cache,
      tibble(address_full = new_for_google) |>
        geocode(
          address_full,
          method = "google",
          lat = latitude,
          long = longitude,
          limit = 1,
          full_results = TRUE
        )
    ) |>
    distinct(address_full, .keep_all = TRUE)
  write_rds(google_cache, "data/facilities-geocoded-google.rds")
}

arcgis_cache <- read_rds("data/facilities-geocoded-arcgis.rds") |>
  select(-any_of("detention_facility_code"))
new_for_arcgis <- setdiff(
  facilities_to_geocode$address_full, arcgis_cache$address_full
)
if (length(new_for_arcgis) > 0) {
  arcgis_cache <-
    bind_rows(
      arcgis_cache,
      tibble(address_full = new_for_arcgis) |>
        geocode(
          address_full,
          method = "arcgis",
          lat = latitude,
          long = longitude,
          limit = 1,
          full_results = TRUE
        )
    ) |>
    distinct(address_full, .keep_all = TRUE)
  write_rds(arcgis_cache, "data/facilities-geocoded-arcgis.rds")
}

facilities_geocoded_google <-
  facilities_to_geocode |>
  inner_join(google_cache, by = "address_full")

facilities_geocoded_arcgis <-
  facilities_to_geocode |>
  inner_join(arcgis_cache, by = "address_full")

facilities_geocoded_df <-
  bind_rows(
    "google" = facilities_geocoded_google,
    "arcgis" = facilities_geocoded_arcgis |>
      rename(formatted_address = arcgis_address),
    .id = "geocoder"
  ) |>
  arrange(detention_facility_code, geocoder) |>
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
          "StreetInt"
        ),
    .by = detention_facility_code
  ) |>
  filter(any_geocoded == TRUE, any(is_exact_arcgis)) |>
  mutate(
    is_exact_google_any = any(is_exact_google),
    is_exact_arcgis_any = any(is_exact_arcgis),
    .by = detention_facility_code
  ) |>
  filter(is_exact_google_any | is_exact_arcgis_any) |>
  filter(
    (is_exact_google_any & !is_exact_arcgis_any & geocoder == "google") |
      (!is_exact_google_any & is_exact_arcgis_any & geocoder == "arcgis") |
      (is_exact_google_any & is_exact_arcgis_any & geocoder == "arcgis")
  ) |>
  select(
    detention_facility_code,
    geocoder,
    latitude,
    longitude,
    address_full,
    formatted_address
  )

arrow::write_parquet(
  facilities_geocoded_df,
  "data/facilities-geocoded-exact.parquet"
)
