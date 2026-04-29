#!/usr/bin/env Rscript
# Diff two facilities-latest-sf.parquet files (main vs PR) and emit a markdown
# summary suitable for a PR comment. Geometry column is dropped — meaningful
# location changes show up as latitude/longitude/address differences anyway.
#
# Usage: Rscript code/diff-facilities-pr.R <main.parquet> <pr.parquet>
#   stdout: markdown comment body

suppressMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(stringr)
})

args <- commandArgs(trailingOnly = TRUE)
main_path <- args[1]
pr_path   <- args[2]
MARKER    <- "<!-- pr-diff-bot -->"
MAX_ROWS  <- 100  # cap each table; comment-size guard

cat(MARKER, "\n", sep = "")
cat("## `facilities-latest-sf.parquet` diff vs `main`\n\n")

if (!file.exists(main_path) || file.size(main_path) == 0) {
  cat("_No `facilities-latest-sf.parquet` on `main` — skipping diff._\n")
  quit(status = 0)
}
if (!file.exists(pr_path) || file.size(pr_path) == 0) {
  cat("_No `facilities-latest-sf.parquet` on this branch — nothing to diff._\n")
  quit(status = 0)
}

read_clean <- function(p) {
  read_parquet(p) |>
    as_tibble() |>
    select(-any_of("geometry"))
}
main_df <- read_clean(main_path)
pr_df   <- read_clean(pr_path)

# Stable key — code, falling back to name|state when code is blank.
make_key <- function(df) {
  df |> mutate(.key = if_else(
    is.na(detention_facility_code) | detention_facility_code == "",
    paste0("noCode|", coalesce(name, ""), "|", coalesce(state, "")),
    detention_facility_code
  ))
}
main_df <- make_key(main_df)
pr_df   <- make_key(pr_df)

added   <- pr_df   |> anti_join(main_df, by = ".key")
removed <- main_df |> anti_join(pr_df,   by = ".key")
common  <- intersect(main_df$.key, pr_df$.key)

# Cell-level diffs on rows present in both. Cast everything to character so the
# pivot can compare different types uniformly.
data_cols <- setdiff(intersect(names(main_df), names(pr_df)), c(".key"))
to_char_long <- function(df) {
  df |>
    filter(.key %in% common) |>
    select(.key, all_of(data_cols)) |>
    mutate(across(-.key, ~ as.character(.x))) |>
    pivot_longer(-.key, names_to = "column", values_to = "value")
}

changes <- inner_join(
    to_char_long(main_df) |> rename(main = value),
    to_char_long(pr_df)   |> rename(pr   = value),
    by = c(".key", "column")
  ) |>
  filter(
    (is.na(main) != is.na(pr)) |
      (!is.na(main) & !is.na(pr) & main != pr)
  ) |>
  left_join(
    pr_df |> distinct(.key, name, detention_facility_code),
    by = ".key"
  )

# Markdown helpers -----------------------------------------------------------
md_escape <- function(x) {
  x <- replace_na(as.character(x), "")
  x <- str_replace_all(x, "\\|", "\\\\|")  # escape pipes in cell values
  str_replace_all(x, "\\n", " ")
}

md_table <- function(df, cols, max_rows = MAX_ROWS) {
  if (nrow(df) == 0) return("_(none)_\n")
  df_show <- df |> slice_head(n = max_rows) |> select(all_of(cols))
  hdr  <- paste0("| ", paste(cols, collapse = " | "), " |")
  sep  <- paste0("| ", paste(rep("---", length(cols)), collapse = " | "), " |")
  rows <- vapply(seq_len(nrow(df_show)), function(i) {
    paste0("| ", paste(md_escape(unlist(df_show[i, ])), collapse = " | "), " |")
  }, character(1))
  out <- paste(c(hdr, sep, rows), collapse = "\n")
  if (nrow(df) > max_rows) {
    out <- paste0(out, "\n\n_Showing first ", max_rows, " of ", nrow(df), " rows._")
  }
  paste0(out, "\n")
}

# Output sections -----------------------------------------------------------
cat(sprintf("- **Added:** %d facility(ies)\n", nrow(added)))
cat(sprintf("- **Removed:** %d facility(ies)\n", nrow(removed)))
cat(sprintf("- **Modified:** %d cell change(s) across %d facility(ies)\n\n",
            nrow(changes), n_distinct(changes$.key)))

cat("### Added\n\n")
cat(md_table(added,
             intersect(c("detention_facility_code", "name", "state",
                         "address_full", "field_office"), names(added))))
cat("\n### Removed\n\n")
cat(md_table(removed,
             intersect(c("detention_facility_code", "name", "state",
                         "address_full", "field_office"), names(removed))))
cat("\n### Modified\n\n")
cat(md_table(
  changes |> arrange(detention_facility_code, name, column),
  c("detention_facility_code", "name", "column", "main", "pr")
))
