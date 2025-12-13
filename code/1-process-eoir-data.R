library(tidyverse)
library(tidylog)

base_city <- read_delim("inputs/tblLookupBaseCity.csv")

# inmate_housing <- read_delim("inputs/tblInmateHousing.csv")

lookup_inmate <- data.table::fread("inputs/tblLookupInmate.csv") |> as_tibble()

ice_facilities_eoir_court <-
  lookup_inmate |>
  select(
    eoir_base_city = INMATE_BASE,
    eoir_detention_facility_code = INMATE_CODE,
    name = INMATE_NAME,
    address = INMATE_ADDRESS,
    city = INMATE_CITY,
    state = INMATE_ST,
    zip = INMATE_ZIP
  ) |>
  mutate(
    date = as.Date("2025-12-01")
  )

arrow::write_feather(
  ice_facilities_eoir_court,
  "data/facilities-eoir.feather"
)
