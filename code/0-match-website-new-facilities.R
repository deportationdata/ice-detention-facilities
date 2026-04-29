library(tidyverse)
library(arrow)
library(fuzzylink)
source("code/functions.R")

OUTPUT <- "data/facilities-website-matches.parquet"
SUMMARY <- "/tmp/website-match-summary.md"
# Floor on fuzzylink's `match_probability`. fuzzylink's classifier is poorly
# calibrated when there are few candidates, so the gpt-5.2 `match == "Yes"`
# verdict (filtered separately) is the load-bearing signal; this floor just
# blocks pairs where the classifier had ~zero confidence.
THRESHOLD <- 0.3

# (name, state) pairs newly added in today's scrape, per the diff snapshots in
# /tmp. Empty in local dev; populated by the workflow.
diff_key <- function(d) paste(d$name, d$state, sep = " | ")
old_path <- "/tmp/old-facilities-from-ice-website.parquet"
new_path <- "/tmp/new-facilities-from-ice-website.parquet"
if (!file.exists(old_path) || !file.exists(new_path)) {
  # Local-dev fallback: diff the two most-recent dated parquets in data/.
  dated <- sort(list.files(
    "data",
    pattern = "^facilities-from-ice-website-\\d{4}-\\d{2}-\\d{2}\\.parquet$",
    full.names = TRUE
  ), decreasing = TRUE)
  if (length(dated) >= 2) {
    new_path <- dated[1]
    old_path <- dated[2]
  }
}
added_keys <- if (file.exists(old_path) && file.exists(new_path)) {
  setdiff(diff_key(read_parquet(new_path)), diff_key(read_parquet(old_path)))
} else {
  character(0)
}

# Lookup includes both the basic catalog (3-name-state-match) and the augmented
# code map (4-name-code-match) — that way any manual entries in either tribble
# propagate here without my script re-running the whole pipeline.
lookup <- bind_rows(
  read_parquet("data/facilities-name-state-match.parquet") |>
    select(detention_facility_code, name, state),
  read_parquet("data/facilities-name-code-match.parquet") |>
    select(detention_facility_code, name, state)
) |>
  filter(!is.na(detention_facility_code), !is.na(state), !is.na(name)) |>
  distinct(detention_facility_code, name, state) |>
  mutate(name_join = clean_facility_name(name))

# Newly-added website rows that don't already exact-match on cleaned name and
# haven't been resolved on a previous run.
prior <- if (file.exists(OUTPUT)) read_parquet(OUTPUT) else NULL
candidates <- read_parquet("data/facilities-from-ice-website.parquet") |>
  filter(
    !is.na(name),
    !is.na(state),
    paste(name, state, sep = " | ") %in% added_keys
  ) |>
  mutate(name_join = clean_facility_name(name)) |>
  anti_join(lookup, by = c("state", "name_join")) |>
  anti_join(
    prior %||% tibble(name = character(), state = character()),
    by = c("name", "state")
  )

matches <- tibble(
  name = character(),
  state = character(),
  detention_facility_code = character(),
  matched_against_name = character(),
  match_score = double()
)

if (nrow(candidates) > 0 && nzchar(Sys.getenv("OPENAI_API_KEY"))) {
  raw <- fuzzylink(
    candidates |> select(name, state, name_join),
    lookup |> select(name, state, name_join, detention_facility_code),
    record_type = "name of detention facility",
    instructions = "compare facility names to find possible matches; some are hospitals or medical centers and some are jails, prisons, or detention facilities",
    by = "name_join",
    blocking.variables = "state", # within-state only -> no cross-state hits
    learner = "ranger",
    max_labels = 50000
  )

  # `match == "Yes"` is fuzzylink's gpt-5.2 verdict (authoritative); the
  # `match_probability` column is from its trained classifier, which is poorly
  # calibrated when there are few candidates — keep a low floor as a sanity
  # check rather than the 0.85 you'd expect from a well-trained model.
  matches <- raw |>
    as_tibble() |>
    filter(match == "Yes", match_probability >= THRESHOLD) |>
    group_by(name = name.x, state) |>
    slice_max(match_probability, n = 1, with_ties = FALSE) |>
    ungroup() |>
    transmute(
      name,
      state,
      detention_facility_code,
      matched_against_name = name.y,
      match_score = match_probability
    )
} else if (nrow(candidates) > 0) {
  warning(
    "OPENAI_API_KEY not set; skipping ",
    nrow(candidates),
    " candidate(s)."
  )
}

# Generate XX-prefix synthetic codes for candidates fuzzylink couldn't match,
# so they still flow through 5-pick-attributes-latest.R into facilities-latest.
# Format mirrors existing manual XX codes (XXBESTW, XXSANYD, etc.): "XX" plus
# the first 5 alphanumeric chars of the cleaned name. Append a digit on
# collision against any code already in use.
make_synth_code <- function(nm, taken) {
  base <- nm |>
    clean_facility_name() |>
    str_remove_all("[^a-z0-9]") |>
    str_sub(1, 5) |>
    str_to_upper() |>
    str_pad(5, side = "right", pad = "X")
  out <- str_c("XX", base)
  i <- 2L
  while (out %in% taken) {
    out <- str_c("XX", base, i)
    i <- i + 1L
  }
  out
}

unmatched <- candidates |>
  anti_join(matches, by = c("name", "state")) |>
  select(name, state)

if (nrow(unmatched) > 0) {
  taken <- c(lookup$detention_facility_code,
             prior$detention_facility_code %||% character(0),
             matches$detention_facility_code)
  synth <- vector("list", nrow(unmatched))
  for (i in seq_len(nrow(unmatched))) {
    code <- make_synth_code(unmatched$name[i], taken)
    taken <- c(taken, code)
    synth[[i]] <- tibble(
      name = unmatched$name[i],
      state = unmatched$state[i],
      detention_facility_code = code,
      matched_against_name = NA_character_,
      match_score = NA_real_
    )
  }
  matches <- bind_rows(matches, list_rbind(synth))
}

# Drop prior entries whose (name, state) now matches an authoritative lookup
# entry — keeps the matches parquet from carrying stale synthetic codes for
# facilities that have since been manually coded in 3-name-state-match.R or
# 4-name-code-match.R.
prior_clean <- if (is.null(prior)) {
  NULL
} else {
  prior |>
    mutate(name_join = clean_facility_name(name)) |>
    anti_join(lookup, by = c("state", "name_join")) |>
    select(-name_join)
}

bind_rows(prior_clean, matches) |>
  distinct(name, state, .keep_all = TRUE) |>
  write_parquet(OUTPUT)

real <- matches |> filter(!is.na(matched_against_name))
synth <- matches |> filter(is.na(matched_against_name))
writeLines(
  c(
    sprintf(
      "**New ICE-website facility codes:** %d real (matched to existing) + %d synthetic XX-prefix placeholders.",
      nrow(real), nrow(synth)
    ),
    if (nrow(real) > 0) {
      c("", "Real matches:", sprintf(
        "- `%s | %s` -> `%s` (matched `%s`, p=%.2f)",
        real$name, real$state, real$detention_facility_code,
        real$matched_against_name, real$match_score))
    },
    if (nrow(synth) > 0) {
      c("", "Synthetic placeholders (review and replace with real codes when known):",
        sprintf("- `%s | %s` -> `%s`",
                synth$name, synth$state, synth$detention_facility_code))
    }
  ),
  SUMMARY
)
