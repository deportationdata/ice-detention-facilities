library(tidyverse)
library(tidylog)

source("code/functions.R")

# analysis on all fields
all_fields <- c(
  "name",
  "address",
  "city",
  "state",
  "zip",
  "aor",
  "type",
  "type_detailed",
  "male_female",
  "circuit",
  "docket",
  "ice_funded",
  "over_under_72",
  "operator",
  "eoir_base_city",
  "eoir_detention_facility_code"
)

facility_attributes <-
  arrow::read_feather(
    "data/facilities-attributes-raw.feather"
  ) |>
  mutate(circuit = as.character(circuit))

# first, for those w/o codes, get codes from name-city-state match

name_code_match <-
  arrow::read_feather(
    "data/facilities-name-code-match.feather"
  )

facilities_with_multiple_codes <-
  name_code_match |>
  mutate(name_join = clean_text(name)) |>
  distinct(name_join, state, detention_facility_code, .keep_all = TRUE) |>
  filter(n() > 1, .by = c("name_join", "state")) |>
  arrange(state, name_join) |>
  mutate(ID = factor(str_c(name_join, state)) |> as.numeric()) |>
  mutate(n = row_number(), .by = "ID") |>
  pivot_wider(
    id_cols = c(name_join, state),
    names_from = n,
    values_from = detention_facility_code,
    names_glue = "detention_facility_code_{.name}"
  )

facility_attributes_nocodes <-
  facility_attributes |>
  filter(
    is.na(detention_facility_code)
  ) |>
  select(-detention_facility_code) |>
  mutate(
    name_join = clean_text(name)
  ) |>
  left_join(
    name_code_match |>
      mutate(name_join = clean_text(name)) |>
      group_by(state, name_join) |>
      slice_max(date_facility_code, n = 1, with_ties = FALSE) |>
      ungroup() |>
      select(-name),
    by = c("state", "name_join")
  ) |>
  select(-name_join) |>
  distinct() # check this

facility_attributes_unmatched <-
  facility_attributes_nocodes |>
  filter(is.na(detention_facility_code)) |>
  anti_join(
    name_code_match,
    by = c("name", "state")
  ) |>
  # keep only ICE sources (NOTE: I selected the ICE ones based on currently the only ones that match)
  filter(source %in% c("51185", "detention_management", "website"))

# need to keep in the final data those with multiple codes -- multiple rows -- because those actually exist in the detentions data
# then in the list we'll provide a column with multiple codes as a list for merging but there will be one row when we display it

facility_attributes <-
  bind_rows(
    facility_attributes,
    facility_attributes_nocodes
  ) |>
  filter(!is.na(detention_facility_code)) |>
  left_join(
    facilities_with_multiple_codes,
    by = c("detention_facility_code" = "detention_facility_code_2", "state")
  ) |>
  mutate(
    detention_facility_code = case_when(
      !is.na(detention_facility_code_1) ~ detention_facility_code_1,
      TRUE ~ detention_facility_code
    )
  ) |>
  select(-detention_facility_code_1)

arrow::write_feather(
  facility_attributes,
  "data/facilities-attributes-cleaned-with-codes.feather"
)

is_po_box <- function(values) {
  str_detect(
    str_to_upper(values),
    "\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b"
  )
}

facility_pivot <-
  facility_attributes |>
  mutate(
    address_full = case_when(
      !is.na(address) ~ glue::glue("{address}, {city}, {state} {zip}"),
      TRUE ~ NA_character_
    )
  ) |>
  select(
    detention_facility_code,
    date,
    source,
    all_of(all_fields),
    address_full
  ) |>
  relocate(address_full, .after = name) |>
  pivot_longer(
    cols = all_of(c(all_fields, "address_full")),
    names_to = "variable",
    values_to = "value"
  ) |>
  filter(!is.na(value) & value != "")

arrow::write_feather(
  facility_pivot,
  "data/facilities-values-long.feather"
)

facility_latest_values <-
  facility_pivot |>
  mutate(
    source_hierarchy = case_when(
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
          "eoir",
          "hrw",
          "website"
        ) ~ 1,
      source == "manual" ~ 2,
      TRUE ~ 0
    )
  ) |>
  filter(
    !source %in% c("vera", "marshall")
  ) |>
  arrange(
    detention_facility_code,
    variable,
    source_hierarchy,
    !is_po_box(value),
    date
  ) |>
  group_by(detention_facility_code, variable) |>
  summarize(
    value = last(value[!is.na(value)]),
    source = last(source[!is.na(value)]),
    date = last(date[!is.na(value)]),
    .groups = "drop"
  ) |>
  left_join(
    facilities_with_multiple_codes |>
      select(detention_facility_code_1, detention_facility_code_2),
    by = c("detention_facility_code" = "detention_facility_code_1")
  ) |>
  rename(detention_facility_code_alt = detention_facility_code_2) |>
  relocate(detention_facility_code_alt, .after = detention_facility_code)

arrow::write_feather(
  facility_latest_values,
  "data/facilities-latest-values-long.feather"
)
