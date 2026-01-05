library(tidyverse)

# Digitized from table in https://www.globaldetentionproject.org/wp-content/uploads/2021/01/US_Department_of_Homeland_Security_2007_1_1.pdf

# Download to temp file and read in 2017 data
facilities_51185 <-
  read_csv(
    "inputs/facilities-foia-51185.csv",
  ) |>
  janitor::clean_names() |>
  mutate(row_original = row_number(), .before = 0)

facilities_51185 <-
  facilities_51185 |>
  rename(state = st, adp_fy07 = fy07, adp_max_fy07 = max_fy07) |>
  mutate(zip = as.character(zip)) |> # for appending to other data more easily
  mutate(date = as.Date("2007-11-07")) # date in file

arrow::write_feather(
  facilities_51185,
  "data/facilities-foia-51185.feather"
)
