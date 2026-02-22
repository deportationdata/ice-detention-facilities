library(tidyverse)
library(tidylog)

files <- list.files(
  "data",
  pattern = "facilities-from-ice-website-.*\\.feather",
  full.names = TRUE
)

arrow::read_feather(files[1])

facilities_from_website <-
  files |>
  set_names(basename(files)) |>
  map_dfr(arrow::read_feather, .id = "file") |>
  mutate(date = str_extract(file, "\\d{4}-\\d{2}-\\d{2}") |> as.Date()) |>
  group_by(name, field_office, address, city, state, zip) |>
  summarize(
    name = last(name),
    field_office = last(field_office),
    address = last(address),
    city = last(city),
    state = last(state),
    zip = last(zip),
    date = last(date),
    .groups = "drop"
  )

arrow::write_feather(
  facilities_from_website,
  "data/facilities-from-ice-website.feather"
)
