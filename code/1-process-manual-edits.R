library(tidyverse)
library(tidylog)

manual_values <- read_delim("inputs/addresses_manual2.csv")

manual_values <-
  manual_values |>
  # remove empty columns
  select(-where(~ all(is.na(.)))) |>
  mutate(
    source = "ddp-manual",
    date = as.Date("2999-01-01"),
    zip = as.character(zip)
  )

arrow::write_parquet(
  manual_values,
  "data/facilities-manual.parquet"
)
