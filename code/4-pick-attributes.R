library(tidyverse)
library(tidylog)

source("code/functions.R")

# analysis on all fields
all_fields <- c(
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
  "operator"
)

facility_attributes <-
  arrow::read_feather(
    "data/facilities-attributes-raw.feather"
  ) |>
  mutate(circuit = as.character(circuit))

# facility_attributes_clean <-
#   facility_attributes |>
#   mutate(
#     name,

#     across(
#       c(
#         name,
#         address,
#         city,
#         state,
#         aor,
#         type,
#         type_detailed,
#         male_female,
#         circuit,
#         docket,
#         ice_funded,
#         over_under_72,
#         field_office
#       ),
#       ~ if_else(is.na(.x), NA_character_, clean_text(.x))
#     ),

#     address = if_else(
#       is.na(address),
#       NA_character_,
#       clean_street_address(address)
#     ),

#     across(
#       c(zip, zip_4),
#       ~ if_else(is.na(.x), NA_character_, str_remove_all(.x, "[^0-9]"))
#     ),

#     zip = case_when(
#       is.na(zip) ~ NA_character_,
#       nchar(zip) == 5 ~ zip,
#       nchar(zip) > 5 ~ str_sub(zip, 1, 5),
#       TRUE ~ str_pad(zip, 5, pad = "0")
#     ),

#     zip_4 = case_when(
#       is.na(zip_4) ~ NA_character_,
#       nchar(zip_4) == 4 ~ zip_4,
#       nchar(zip_4) > 4 ~ str_sub(zip_4, 1, 4),
#       TRUE ~ str_pad(zip_4, 4, pad = "0")
#     ),

#     circuit = as.character(circuit)
#   )

# first, for those w/o codes, get codes from name-city-state match

name_city_state_match <-
  arrow::read_feather(
    "data/facilities-name-code-match.feather"
  ) |>
  filter(
    !is.na(state),
    !detention_facility_code %in% c("USMS2TX", "USMS3TX") # these are wrongly matched
  )

facilities_with_multiple_codes <-
  name_city_state_match |>
  filter(n() > 1, .by = c("name", "state")) |>
  arrange(name) |>
  mutate(ID = factor(str_c(name, state)) |> as.numeric()) |>
  mutate(n = row_number(), .by = "ID") |>
  pivot_wider(
    names_from = n,
    values_from = detention_facility_code,
    names_glue = "detention_facility_code_{.name}"
  ) |>
  select(-ID)

facility_attributes_nocodes <-
  facility_attributes |>
  filter(
    is.na(detention_facility_code)
  ) |>
  select(-detention_facility_code) |>
  left_join(
    name_city_state_match,
    by = c("name", "state")
  )

facility_attributes_unmatched <-
  facility_attributes_nocodes |>
  filter(is.na(detention_facility_code)) |>
  anti_join(
    name_city_state_match |> mutate(state = str_to_lower(state)),
    by = c("name", "state")
  )

# need to keep in the final data those with multiple codes -- multiple rows -- because those actually exist in the detentions data
# then in the list we'll provide a column with multiple codes as a list for merging but there will be one row when we display it

facility_attributes <-
  facility_attributes |>
  filter(!is.na(detention_facility_code)) |>
  bind_rows(facility_attributes_nocodes) |>
  filter(!is.na(detention_facility_code)) |>
  left_join(
    facilities_with_multiple_codes |> select(-state, -name),
    by = c("detention_facility_code" = "detention_facility_code_2")
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

facility_pivot <-
  facility_attributes |>
  select(detention_facility_code, date, source, all_of(all_fields)) |>
  pivot_longer(
    cols = all_of(all_fields),
    names_to = "variable",
    values_to = "value"
  ) |>
  filter(!is.na(value) & value != "")

pivot_changes_flagged <-
  facility_pivot |>
  group_by(variable, detention_facility_code) |>
  mutate(
    prev_value = lag(value),
    changed = if_else(
      is.na(prev_value),
      FALSE,
      str_to_lower(str_squish(value)) != str_to_lower(str_squish(prev_value))
    )
  ) |>
  ungroup()

change_summary_all <-
  pivot_changes_flagged |>
  group_by(variable, detention_facility_code) |>
  summarize(
    n_obs = n(),
    n_changes = sum(changed, na.rm = TRUE),
    ever_changed = any(changed),
    .groups = "drop"
  )

variable_change_stats <-
  change_summary_all |>
  group_by(variable) |>
  summarize(
    total_facilities = n(),
    facilities_changed = sum(ever_changed),
    pct_changed = mean(ever_changed) * 100,
    .groups = "drop"
  ) |>
  arrange(desc(pct_changed))

facilities_with_changes_all <-
  change_summary_all |>
  filter(n_changes > 0)

pattern_results <-
  pivot_changes_flagged |>
  semi_join(
    facilities_with_changes_all,
    by = c("variable", "detention_facility_code")
  ) |>
  group_by(variable, detention_facility_code) |>
  summarize(
    pattern = paste(
      unique(paste0(value, " (", date, ")")),
      collapse = " → "
    ),
    values_over_time = paste(
      paste0(value, " (", date, ")"),
      collapse = " → "
    ),
    .groups = "drop"
  )

# helper functions for implementing best values
has_reversion <- function(values) {
  values <- values[!is.na(values)]
  if (length(values) < 3) {
    return(FALSE)
  }
  values[1] %in% values[-1] # original value reappears
}

# count number of reversions, specifically A → B → A
is_aba <- function(values) {
  values <- values[!is.na(values)]
  if (length(values) < 3) {
    return(FALSE)
  }
  values[1] == values[length(values)] && length(unique(values)) == 2
}

get_modes <- function(v) {
  v <- v[!is.na(v)]
  tb <- table(v)
  names(tb)[tb == max(tb)]
}

value_histories <-
  pivot_changes_flagged |>
  group_by(variable, detention_facility_code) |>
  arrange(date, .by_group = TRUE) |>
  summarize(
    history = list(paste0(value, " (", date, ", ", source, ")")),
    values = list(as.character(value)),
    dates = list(date),
    sources = list(source),
    n_changes = sum(changed, na.rm = TRUE),
    .groups = "drop"
  )

# implementing best values
best_values <-
  value_histories |>
  rowwise() |>
  mutate(
    unique_values = list(unique(values)),
    n_unique = length(unique_values),
    modes = list(get_modes(values)),
    n_modes = length(modes),
    ends_with_original = values[length(values)] == values[1],
    has_reversion = has_reversion(values),
    is_aba = is_aba(values),

    best_value = case_when(
      # rule 1: never changes
      n_changes == 0 ~ values[1],

      # rules 2 and 3: changes but does not revert (keep final)
      !has_reversion ~ values[length(values)],

      # rule 4: A → B → A (use modal value)
      has_reversion & n_modes == 1 ~ modes[[1]],

      # rule 5: multiple modes or more than 5 changes
      TRUE ~ NA_character_
    ),

    # manual review flag
    review_flag = case_when(
      n_unique > 2 & n_modes > 1 ~ "multiple_modes",
      n_changes > 5 ~ "many_changes",
      has_reversion & n_modes > 1 ~ "reversion_multiple_modes",
      TRUE ~ NA_character_
    )
  ) |>
  ungroup()

# counts of A → B → A pattern
reversion_counts <-
  value_histories |>
  rowwise() |>
  mutate(
    is_aba = is_aba(values)
  ) |>
  ungroup() |>
  filter(is_aba) |>
  select(variable, detention_facility_code, values, dates, sources)

reversion_counts_summary <-
  reversion_counts |>
  group_by(variable) |>
  summarize(
    n_reversions = n(),
    .groups = "drop"
  )

# merging best_values with facilities data
best_values_wide <-
  best_values |>
  select(detention_facility_code, variable, best_value) |>
  pivot_wider(
    names_from = variable,
    values_from = best_value
  ) |>
  left_join(
    facilities_with_multiple_codes |>
      transmute(
        detention_facility_code = detention_facility_code_1,
        detention_facility_code_alt = detention_facility_code_2
      ),
    by = "detention_facility_code"
  )

arrow::write_feather(
  best_values_wide,
  "data/facilities-best-values-wide.feather"
)
