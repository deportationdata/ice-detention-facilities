library(tidyverse)

source("code/functions.R")

# temporarily just use the facilities in the recent data
facility_list <-
  arrow::read_feather(
    "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
  ) |>
  as_tibble() |>
  mutate(
    cnt = sum(year(book_in_date_time) == 2025),
    .by = detention_facility_code
  ) |>
  filter(cnt >= 1) |>
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

# bring in codes for changed facility codes

# format data

facility_formatted <-
  facility_final |>
  # left_join(hospitals, by = c("name", "state")) |>
  mutate(
    name = name |>
      str_replace_all("\\.", "") |>
      clean_facility_name() |>
      snakecase::to_title_case(
        abbreviations = c(
          state.abb,
          "NYC",
          "F",
          "PD",
          "YMCA",
          "US",
          "CF",
          "C",
          "WD",
          "N",
          "S",
          "E",
          "W",
          "NE",
          "NW",
          "SE",
          "SW",
          "ICE",
          "USBP"
        )
      ) |>
      str_replace_all("Holdroom", "Hold Room"),
    address = address |> str_to_title(),
    city = city |> clean_city() |> str_to_title(),
    state = str_to_upper(state),
    aor = str_to_upper(aor),
    type = str_to_upper(type) |> str_replace_all("OTHER", "Other"),
    type_detailed = replace_na(type_detailed, ""),
    type_detailed = snakecase::to_title_case(
      type_detailed,
      abbreviations = c("IGSA", "IGA", "USMS", "DIGSA", "CDF", "BOP")
    ) |>
      na_if(""),
    docket = str_to_upper(docket),
    male_female = case_match(
      male_female |> str_to_lower(),
      "male" ~ "M",
      "female" ~ "F",
      "female male" ~ "M/F"
    ),
    over_under_72 = str_to_title(over_under_72)
  ) |>
  arrange(name)

arrow::write_feather(
  facility_formatted,
  "data/facilities-latest.feather"
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
