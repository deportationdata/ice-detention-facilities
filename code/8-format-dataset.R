library(tidyverse)
library(sf)
library(geoarrow)

source("code/functions.R")

facility_augmented <-
  arrow::read_parquet(
    "data/facilities-augmented.parquet"
  ) |>
  as_tibble()

# format data

make_abbr_caps <- function(x, abbr) {
  pattern <- paste0("\\b(", paste(abbr, collapse = "|"), ")\\b")
  str_replace_all(
    x,
    regex(pattern, ignore_case = TRUE),
    ~ str_to_upper(.x)
  )
}

facility_formatted <-
  facility_augmented |>
  # TODO: need to remove things from dtm that have 0 people detained
  filter(detention_facility_code != "XXWICHI") |>
  # left_join(hospitals, by = c("name", "state")) |>
  mutate(
    name = name |>
      str_replace_all("\\.", " ") |>
      str_to_lower() |>
      str_replace_all(c(
        abbrev_expansions
      )) |>
      str_replace_all("[‘’`]", "’") |>
      str_squish() |>
      # collapse space-separated single letters into abbreviations (e.g. "u s" -> "US")
      str_replace_all("(?:^|(?<= ))([a-z]( [a-z])+)(?= |$)", function(m) {
        str_to_upper(str_replace_all(m, " ", ""))
      }) |>
      tools::toTitleCase() |>
      # capitalize letter after apostrophe (O'hare -> O'Hare)
      str_replace_all("'([a-z])", function(m) {
        str_c("'", str_to_upper(str_sub(m, 2, 2)))
      }) |>
      # capitalize letter after "Mc" prefix (Mccreary -> McCreary)
      str_replace_all("\\bMc([a-z])", function(m) {
        str_c("Mc", str_to_upper(str_sub(m, 3, 3)))
      }) |>
      # uppercase standalone single letters (initials like "g" in "Dale g Haile")
      str_replace_all("\\b([a-z])\\b", str_to_upper) |>
      # fix possessive 's (the single-letter step above capitalizes it)
      str_replace_all("'S\\b", "'s") |>
      make_abbr_caps(
        abbr = c(
          "CCA",
          "HCA",
          "NYC",
          "F",
          "PD",
          "YMCA",
          "HSI",
          "FDC",
          "APSO",
          "LBJ",
          "US",
          "SSM",
          "CF",
          "C",
          "WD",
          "NE",
          "NW",
          "SE",
          "SW",
          "ICE",
          "USBP",
          "BHC",
          "JFK",
          "EGP",
          "CPC",
          "INS",
          "FMC",
          "CSP",
          "EOR",
          "SMC",
          "CBP",
          "MDC",
          state.abb
        )
      ) |>
      # normalize asymmetric spaces around hyphens: " -x" -> " - x", "x- " -> "x - "
      str_replace_all(" -(?=\\S)", " - ") |>
      str_replace_all("(?<=\\S)- ", " - ") |>
      # ensure there is a space after commas
      str_replace_all(",([^ ])", ", \\1") |>
      # ensure there is a space before an open parenthesis
      str_replace_all("([^ ])\\(", "\\1 (") |>
      # and after close parenthesis
      str_replace_all("\\)([^ ])", ") \\1") |>
      # drop , at end of name
      str_replace_all(",\\s*$", "") |>
      # drop space before comma
      str_replace_all("\\s+,", ",") |>
      str_squish(),
    address = address |> str_to_title(),
    address_full = address_full |> str_to_title(),
    city = city |> clean_city() |> str_to_title(),
    state = str_to_upper(state),
    type = str_to_upper(type) |> str_replace_all("OTHER", "Other"),
    type_detailed = replace_na(type_detailed, ""),
    type_detailed = snakecase::to_title_case(
      type_detailed,
      abbreviations = c(
        "IGSA",
        "IGA",
        "USMS",
        "DIGSA",
        "CDF",
        "BOP",
        "USBP",
        "MOC",
        "SPC",
        "DOD",
        "CBP"
      )
    ) |>
      na_if(""),
    # docket = str_to_upper(docket),
    male_female = male_female |>
      str_to_lower() |>
      recode_values(
        "male" ~ "M",
        "female" ~ "F",
        "female/male" ~ "M/F"
      ),
    over_under_72 = str_to_title(over_under_72)
  ) |>
  arrange(name)

arrow::write_parquet(
  facility_formatted,
  "data/facilities-latest.parquet"
)

facility_formatted |>
  st_as_sf(
    coords = c("longitude", "latitude"),
    na.fail = FALSE,
    crs = 4326,
    remove = FALSE
  ) |>
  tibble::as_tibble() |>
  arrow::write_parquet("data/facilities-latest-sf.parquet")

# # metadata for diagnostics
# best_values_metadata <-
#   best_values |>
#   select(
#     detention_facility_code,
#     variable,
#     n_changes,
#     n_unique,
#     has_reversion,
#     is_aba,
#     review_flag
#   ) |>
#   pivot_wider(
#     names_from = variable,
#     values_from = c(n_changes, n_unique, has_reversion, is_aba, review_flag),
#     names_glue = "{variable}_{.value}"
#   )

# facility_final_metadata <-
#   facility_final |>
#   left_join(best_values_metadata, by = "detention_facility_code")
