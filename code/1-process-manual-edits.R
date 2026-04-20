library(tidyverse)
library(tidylog)

manual_values <- read_delim("inputs/addresses_manual2.csv")

manual_values <-
  manual_values |>
  # remove empty columns
  select(-where(~ all(is.na(.)))) |>
  select(-name) |>
  mutate(
    source = "ddp-manual",
    date = as.Date("2999-01-01"),
    zip = if_else(
      !is.na(zip) & str_detect(as.character(zip), "^\\d+$") & nchar(as.character(zip)) < 5,
      str_pad(as.character(zip), 5, "left", "0"),
      as.character(zip)
    )
  )

arrow::write_parquet(
  manual_values,
  "data/facilities-manual.parquet"
)
