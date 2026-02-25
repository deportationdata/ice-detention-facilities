library(tidyverse)
library(sf)

source("code/functions.R")

facility_augmented <-
  arrow::read_feather(
    "data/facilities-augmented.feather"
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
  # left_join(hospitals, by = c("name", "state")) |>
  mutate(
    name = name |>
      str_replace_all("\\.", " ") |>
      str_to_lower() |>
      str_replace_all(c(
        abbrev_expansions
      )) |>
      str_replace_all("[’‘`]", "'") |>
      str_to_title() |>
      make_abbr_caps(
        abbr = c(
          "CCA",
          "HCA",
          "NYC",
          "F",
          "PD",
          "YMCA",
          "US",
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
          state.abb
        )
      ) |>
      str_replace_all(regex("'S\\b", ignore_case = TRUE), "'s") |> # fix 'S or 's
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
      str_squish() |>
      # make And Or lower case
      str_replace_all("\\bAnd\\b", "and") |>
      str_replace_all("\\bOr\\b", "or") |>
      str_replace_all("\\bOf\\b", "of"),
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

arrow::write_feather(
  facility_formatted,
  "data/facilities-latest.feather"
)

sfarrow::st_write_feather(
  facility_formatted |>
    st_as_sf(
      coords = c("longitude", "latitude"),
      na.fail = FALSE,
      crs = 4326,
      remove = FALSE
    ),
  "data/facilities-latest-sf.feather"
)

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
