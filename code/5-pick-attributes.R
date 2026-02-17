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
  "operator",
  "eoir_base_city",
  "eoir_detention_facility_code"
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
    has_gov_source = any(!is.na(source) & source != "vera"),
    most_recent_source = source[which.max(date)],
    .groups = "drop"
  )

is_po_box <- function(values) {
  str_detect(
    str_to_upper(values),
    "\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b"
  )
}

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
    has_non_po = length(values[!is_po_box(values)]) > 0,

    best_value = case_when(
      most_recent_source == "vera" & !has_gov_source ~ values[length(values)],

      variable == "address" & has_non_po ~ {
        v <- values[!is_po_box(values)]
        if (length(v) == 0) {
          NA_character_
        } else if (n_changes == 0) {
          v[1]
        } else if (!has_reversion) {
          v[length(v)]
        } else if (has_reversion & length(get_modes(v)) == 1) {
          get_modes(v)[1]
        } else {
          v[length(v)]
        }
      },

      TRUE ~ case_when(
        # rule 1: never changes
        n_changes == 0 ~ values[1],

        # rule 2: changes but does not revert (keep final)
        !has_reversion ~ values[length(values)],

        # rule 3: A → B → A (use modal value)
        has_reversion & n_modes == 1 ~ modes[[1]],

        # rule 4: multiple modes or more than 5 changes
        TRUE ~ values[length(values)]
      )
    ),

    best_address_any = if_else(
      variable == "address",
      case_when(
        n_changes == 0 ~ values[1],
        !has_reversion ~ values[length(values)],
        has_reversion & n_modes == 1 ~ modes[[1]],
        TRUE ~ NA_character_
      ),
      NA_character_
    ),

    # manual review flag
    review_flag = case_when(
      n_unique > 2 & n_modes > 1 ~ "multiple_modes",
      n_changes > 5 ~ "many_changes",
      has_reversion & n_modes > 1 ~ "reversion_multiple_modes",
      TRUE ~ NA_character_
    ),

    has_problem = !is.na(review_flag)
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
    id_cols = detention_facility_code,
    names_from = variable,
    values_from = best_value
  ) |>
  left_join(
    best_values |>
      filter(variable == "address") |>
      transmute(detention_facility_code, address_any = best_address_any),
    by = "detention_facility_code"
  ) |>
  arrange(detention_facility_code)

problem_flags_wide <-
  best_values |>
  mutate(problem_var = paste0("problem_", variable)) |>
  select(detention_facility_code, problem_var, has_problem) |>
  pivot_wider(
    names_from = problem_var,
    values_from = has_problem,
    values_fill = FALSE
  )

best_values_wide <-
  best_values_wide |>
  left_join(problem_flags_wide, by = "detention_facility_code") |>
  left_join(
    facilities_with_multiple_codes,
    by = c("detention_facility_code" = "detention_facility_code_1", "state")
  ) |>
  rename(detention_facility_code_alt = detention_facility_code_2) |>
  relocate(detention_facility_code_alt, .after = detention_facility_code)

arrow::write_feather(
  best_values_wide,
  "data/facilities-best-values-wide.feather"
)
