library(tidyverse)
library(tidylog)

manual_values <- read_delim("inputs/manual-values.csv")

manual_values <-
  manual_values |>
  mutate(
    source = "ddp-manual",
    date = as.Date("2999-01-01"),
    zip = as.character(zip)
  )

arrow::write_parquet(
  manual_values,
  "data/facilities-manual.parquet"
)
