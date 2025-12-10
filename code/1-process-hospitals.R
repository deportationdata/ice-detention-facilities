library(tidyverse)

hospitals <- read_csv(
  "inputs/Hospital_General_Information.csv"
)

hospitals <-
  hospitals |>
  transmute(
    name = `Facility Name`,
    address = `Address`,
    city = `City/Town`,
    state = `State`,
    zip = `ZIP Code`,
    medicare_facility_ID = `Facility ID`,
    date = as.Date("2025-10-14") # last modified date
  )

arrow::write_feather(
  hospitals,
  "data/hospitals.feather"
)

jails_prisons <- haven::read_dta("inputs/ICPSR-38323-0001-Data.dta")

jails_prisons <-
  jails_prisons |>
  transmute(
    bjs_facility_ID = JURISID,
    name = FACNAME,
    address = FACADDRESS,
    city = FACCITY,
    state = FACSTATE,
    zip = FACZIP,
    hold_72 = case_when(
      HOLD72PLUS == 1 ~ "Over 72",
      HOLD72PLUS == 0 ~ "Under 72",
      TRUE ~ NA_character_
    ),
    date = as.Date("2020-01-01") # approximate date of data release
  )

arrow::write_feather(
  jails_prisons,
  "data/facilities/jails_prisons.feather"
)
