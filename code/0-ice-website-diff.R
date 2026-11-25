suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

# Paths can be overridden so the scrape workflow can also diff today's scrape
# against main's snapshot (the cumulative change in a long-open PR).
old_path <- Sys.getenv("FACILITIES_DIFF_OLD", "/tmp/old-facilities-from-ice-website.parquet")
new_path <- Sys.getenv("FACILITIES_DIFF_NEW", "/tmp/new-facilities-from-ice-website.parquet")
out_path <- Sys.getenv("FACILITIES_DIFF_OUT", "/tmp/facilities-changes.txt")

old <- read_parquet(old_path)
new <- read_parquet(new_path)

key <- function(d) paste(d$name, d$state, sep = " | ")

added   <- setdiff(key(new), key(old))
removed <- setdiff(key(old), key(new))

common <- intersect(key(new), key(old))
key_cols <- setdiff(names(new), c("name", "state", "date"))

modified_lines <- unlist(lapply(common, function(k) {
  o <- old |> filter(key(old) == k) |> select(any_of(key_cols)) |> slice(1)
  n <- new |> filter(key(new) == k) |> select(any_of(key_cols)) |> slice(1)
  if (identical(o, n)) return(NULL)
  diffs <- unlist(lapply(key_cols, function(col) {
    ov <- as.character(o[[col]])
    nv <- as.character(n[[col]])
    if (!identical(ov, nv)) paste0("  - ", col, ": `", ov, "` -> `", nv, "`")
  }))
  c(paste0("- **", k, "**"), diffs)
}))

lines <- character(0)
if (length(added) > 0) {
  lines <- c(lines, paste0("**", length(added), " facility(ies) added:**"))
  lines <- c(lines, paste0("- ", added))
  lines <- c(lines, "")
}
if (length(removed) > 0) {
  lines <- c(lines, paste0("**", length(removed), " facility(ies) removed:**"))
  lines <- c(lines, paste0("- ", removed))
  lines <- c(lines, "")
}
if (length(modified_lines) > 0) {
  n_modified <- sum(startsWith(modified_lines, "- **"))
  lines <- c(lines, paste0("**", n_modified, " facility(ies) with modified details:**"))
  lines <- c(lines, modified_lines)
  lines <- c(lines, "")
}
if (length(lines) == 0) {
  writeLines("NO_REAL_CHANGES", out_path)
} else {
  writeLines(lines, out_path)
}
