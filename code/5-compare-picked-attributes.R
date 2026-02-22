library(tidyverse)
library(arrow)

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

# string normalizer for comparisons
norm <- function(x) {
  x |>
    as.character() |>
    stringr::str_squish() |>
    stringr::str_to_lower()
}

# convert wide to long for comparison
old_long <-
  arrow::read_feather("data/facilities-best-values-wide.feather") |>
  select(detention_facility_code, any_of(all_fields)) |>
  pivot_longer(
    cols = any_of(all_fields),
    names_to = "variable",
    values_to = "value_old"
  ) |>
  mutate(value_old_norm = norm(value_old))

new_long <-
  arrow::read_feather("data/facilities-latest-values-long.feather") |>
  filter(variable %in% all_fields) |>
  transmute(
    detention_facility_code,
    variable,
    value_new = value,
    source_new = source,
    date_new = date,
    value_new_norm = norm(value_new)
  )

comparison <-
  full_join(
    old_long,
    new_long,
    by = c("detention_facility_code", "variable")
  ) |>
  mutate(
    # mismatch rules:
    # if both missing -> not mismatch
    # if one missing -> mismatch
    # else compare normalized strings
    mismatch = case_when(
      is.na(value_old) & is.na(value_new) ~ FALSE,
      is.na(value_old) & !is.na(value_new) ~ TRUE,
      !is.na(value_old) & is.na(value_new) ~ TRUE,
      TRUE ~ value_old_norm != value_new_norm
    )
  )

overall_summary <-
comparison |>
 summarize(
  n_pairs = n(),
  n_mismatch = sum(mismatch, na.rm = TRUE),
  pct_mismatch = 100 * mean(mismatch, na.rm = TRUE)
)

by_variable <-
  comparison |>
  group_by(variable) |>
  summarize(
    n_pairs = n(),
    n_mismatch = sum(mismatch, na.rm = TRUE),
    pct_mismatch = 100 * mean(mismatch, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(desc(pct_mismatch), desc(n_mismatch))

by_facility <-
  comparison |>
  group_by(detention_facility_code) |>
  summarize(
    n_fields_compared = n(),
    n_mismatch = sum(mismatch, na.rm = TRUE),
    pct_mismatch = 100 * mean(mismatch, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(desc(n_mismatch), desc(pct_mismatch))

mismatch_examples <-
  comparison |>
  filter(mismatch) |>
  select(
    detention_facility_code,
    variable,
    value_old,
    value_new,
    date_new,
    source_new
  ) |>
  arrange(variable, detention_facility_code)