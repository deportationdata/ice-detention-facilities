library(tidyverse)

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

arrow::write_parquet(
  jails_prisons,
  "data/jails_prisons.parquet"
)
