library(tidyverse)
library(tidylog)

source("code/functions.R")

# analysis on all fields
all_fields <- c(
  "detention_facility_code",
  "name",
  "address",
  "city",
  "state",
  "zip",
  "aor",
  "type",
  "type_detailed",
  "male_female",
  "circuit",
  "docket",
  "ice_funded",
  "over_under_72",
  "operator"
)

facilities_2017 <-
  arrow::read_feather("data/facilities-2017.feather")

facilities_dedicated_nondedicated <-
  arrow::read_feather("data/facilities-dedicated-nondedicated.feather")

detentions_05655 <-
  arrow::read_feather(
    "data/facilities-foia-05655.feather"
  )

facility_addresses_from_ice_website <-
  arrow::read_feather(
    "data/facilities-from-ice-website.feather"
  ) |>
  mutate(date = as.Date("2025-10-01")) #|> # approximate date of website scrape

facilities_detention_management <-
  arrow::read_feather(
    "data/facilities-detention-management.feather"
  )

detentions_41855 <-
  arrow::read_feather(
    "data/facilities-foia-41855.feather"
  )

detentions_22955 <-
  arrow::read_feather(
    "data/facilities-foia-22955.feather"
  )

facilities_51185 <-
  arrow::read_feather(
    "data/facilities-foia-51185.feather"
  )

facilities_eoir <-
  arrow::read_feather(
    "data/facilities-eoir.feather"
  )

facilities_vera <-
  arrow::read_feather(
    "data/facilities-vera.feather"
  )

facilities_marshall <-
  arrow::read_feather(
    "data/facilities-marshall.feather"
  )

hospitals <-
  arrow::read_feather(
    "data/hospitals.feather"
  )

jails_prisons <-
  arrow::read_feather(
    "data/jails_prisons.feather"
  )

hfild_local_law_enforcement_facilities <-
  arrow::read_feather(
    "data/hifld-local-law-enforcement-facilities.feather"
  )

hfild_prisons <-
  arrow::read_feather(
    "data/hifld-prisons.feather"
  )

detentions_current <- arrow::read_feather(
  "~/github/ice/data/detention-stints-latest.feather"
)

detentions_2012_2023 <- arrow::read_feather(
  "~/github/ice/data/ice-detentions-2012-2023.feather"
)

facilities_from_detentions <-
  bind_rows(
    "2023-11-15" = detentions_2012_2023 |>
      select(detention_facility_code, detention_facility),
    "2025-10-15" = detentions_current |>
      select(detention_facility_code, detention_facility),
    .id = "date"
  ) |>
  transmute(
    detention_facility_code,
    name = detention_facility,
    date = as.Date(date)
  ) |>
  distinct()

rm(detentions_2012_2023)

# combine for each field one-by-one
facility_attributes <-
  bind_rows(
    "dedicated" = facilities_dedicated_nondedicated |>
      select(any_of(all_fields), date),
    "2017" = facilities_2017 |>
      select(any_of(all_fields), date),
    "05655" = detentions_05655 |>
      select(any_of(all_fields), date),
    "51185" = facilities_51185 |>
      select(any_of(all_fields), date),
    "website" = facility_addresses_from_ice_website |>
      select(any_of(all_fields), date),
    "detention_management" = facilities_detention_management |>
      select(any_of(all_fields), date),
    "41855" = detentions_41855 |>
      select(any_of(all_fields), date),
    "22955" = detentions_22955 |>
      select(any_of(all_fields), date),
    "detentions" = facilities_from_detentions |>
      select(any_of(all_fields), date),
    "eoir" = facilities_eoir |>
      select(
        eoir_detention_facility_code,
        eoir_base_city,
        any_of(all_fields),
        date
      ),
    "vera" = facilities_vera |>
      select(any_of(all_fields), date),
    "marshall" = facilities_marshall |>
      select(any_of(all_fields), date),
    "hospitals" = hospitals |>
      select(medicare_facility_ID, any_of(all_fields), date),
    "jails_prisons" = jails_prisons |>
      select(bjs_facility_ID, any_of(all_fields), date),
    "hifld_local_law_enforcement" = hfild_local_law_enforcement_facilities |>
      select(hifld_id, any_of(all_fields), date),
    "hifld_prisons" = hfild_prisons |>
      select(hifld_id, any_of(all_fields), date),
    .id = "source"
  ) |>
  relocate(detention_facility_code, all_of(all_fields), source, date)

arrow::write_feather(
  facility_attributes,
  "data/facilities-attributes-raw.feather"
)
