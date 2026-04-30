library(tidyverse)
library(tidylog)

source("code/functions.R")

# analysis on all fields
all_fields <- c(
  "name",
  "address",
  "city",
  "state",
  "zip",
  "aor",
  "type",
  "type_detailed",
  "male_female",
  # "circuit",
  # "docket",
  # "ice_funded",
  "over_under_72"
  # "operator"
  # "eoir_base_city",
  # "eoir_detention_facility_code"
)

facility_attributes <-
  arrow::read_parquet(
    "data/facilities-attributes-raw.parquet"
  )

# first, for those w/o codes, get codes from name-city-state match

name_code_match <-
  arrow::read_parquet(
    "data/facilities-name-code-match.parquet"
  )

hospitals_name_match <-
  arrow::read_parquet(
    "data/hospitals-name-match.parquet"
  )

last_detentions_date_per_code <-
  facility_attributes |>
  filter(source == "detentions") |>
  group_by(detention_facility_code) |>
  summarize(last_detentions_date = max(date, na.rm = TRUE), .groups = "drop")

# these are different facilities with the same name or ICE treats them separately,
# so do not collapse them
do_not_collapse_codes <- tribble(
  ~code_a   , ~code_b   ,
  "OAK"     , "BOPOAD"  ,
  "GREENMA" , "MAFRKHC" ,
  "USMS2TX" , "USMS3TX" ,
) |>
  unlist()

facilities_with_multiple_codes <-
  name_code_match |>
  mutate(name_join = clean_text(name)) |>
  filter(!is.na(name_join), name_join != "") |>
  filter(!detention_facility_code %in% do_not_collapse_codes) |>
  distinct(name_join, state, detention_facility_code, .keep_all = TRUE) |>
  filter(n() > 1, .by = c("name_join", "state")) |>
  left_join(last_detentions_date_per_code, by = "detention_facility_code") |>
  arrange(
    state,
    name_join,
    desc(last_detentions_date),
    desc(date_facility_code)
  ) |>
  mutate(ID = factor(str_c(name_join, state)) |> as.numeric()) |>
  mutate(n = row_number(), .by = "ID") |>
  pivot_wider(
    id_cols = c(name_join, state),
    names_from = n,
    values_from = detention_facility_code,
    names_glue = "detention_facility_code_{.name}"
  )

facility_attributes_nocodes <-
  facility_attributes |>
  filter(
    is.na(detention_facility_code)
  ) |>
  select(-detention_facility_code) |>
  mutate(
    name_join = clean_text(name)
  ) |>
  left_join(
    name_code_match |>
      mutate(name_join = clean_text(name)) |>
      group_by(state, name_join) |>
      slice_max(date_facility_code, n = 1, with_ties = FALSE) |>
      ungroup() |>
      select(-name) |>
      rename(detention_facility_code_nc = detention_facility_code),
    by = c("state", "name_join")
  ) |>
  left_join(
    hospitals_name_match |>
      mutate(name_join = clean_text(name)) |>
      distinct(state, name_join, detention_facility_code) |>
      rename(detention_facility_code_hm = detention_facility_code),
    by = c("state", "name_join")
  ) |>
  mutate(
    detention_facility_code = coalesce(
      detention_facility_code_nc,
      detention_facility_code_hm
    ),
    .keep = "unused"
  ) |>
  select(-name_join) |>
  distinct() # check this

# Auto-matched (name, state) -> code from code/0-match-website-new-facilities.R.
# This file is regenerated daily by the scrape workflow. The hand-curated
# tribble below wins ties on (name, state).
website_matches <-
  arrow::read_parquet("data/facilities-website-matches.parquet") |>
  filter(!is.na(detention_facility_code)) |>
  distinct(name, state, detention_facility_code)

facility_attributes_unmatched_manual <-
  tribble(
    ~name                                                                       , ~state        , ~detention_facility_code ,
    "JUVENILE FACILITY"                                                         , "IL"          , ""                       ,
    "GUAYNABO ADC (SAN JUAN)"                                                   , "PR"          , "BOPGUA"                 ,
    "CCA CHER-TAZ DET.CTR."                                                     , "AZ"          , ""                       ,
    "AIRPORT DDP"                                                               , "PR"          , ""                       ,
    "U.S IMMIGRATION"                                                           , "MI"          , ""                       ,
    "JOHNSON COUNTY DETENTION CENTER"                                           , "NC"          , ""                       ,
    "CENTE"                                                                     , "WA"          , ""                       ,
    "MARSHFIELD E. CENTER"                                                      , "TX"          , ""                       ,
    "Broome County Correctional Facility"                                       , "NY"          , "BROMMNY"                ,
    "Burleigh County Detention Center"                                          , "ND"          , "BURLEND"                ,
    "DOW Detention Facility at Fort Bliss"                                      , "TX"          , "EROFCB"                 ,
    "Diamondback Correctional Facility"                                         , "OK"          , "OKDBACK"                ,
    "FCI Lewisburg"                                                             , "PA"          , "BOPLEW"                 ,
    "Lincoln County Detention Center"                                           , "NE"          , "LINCONE"                ,
    "McCook Detention Center"                                                   , "NE"          , "NEMCCOI"                ,
    "Naval Station Guantanamo Bay (JTF Camp Six and Migrant Ops Center Main A)" , NA_character_ , "GTMODCU"                ,
    "Sarpy County Department of Corrections"                                    , "NE"          , "SARPYNE"                ,
    "Uinta County Detention Center"                                             , "WY"          , "UINTAWY"                ,
    "CBP SAN YSIDRO POE"                                                        , "CA"          , "XXSANYD"                ,
    "CTR FAM SVS JUNTOS PRF"                                                    , "NJ"          , "XXSVSJU"                ,
    "BEST WESTERN PLUS EL PASO AIRPORT HOTEL & CONFEREN"                        , "TX"          , "XXBESTW"                ,
    "SUPER  BY WYNDHAM"                                                         , "TX"          , "XXSUPER"                ,
    "PHARR POLICE DEPT"                                                         , "TX"          , "XXPHARR"                ,
    "OMDC ENV USBP OFO TRNSPT"                                                  , "CA"          , "XXOMDCE"                ,
    "TIMBER RIDGE SCHOOL"                                                       , "VA"          , "XXTIMBR"                ,
    "JTF CAMP SIX"                                                              , "FL"          , "GTMODCU"                ,
    "MIGRANT OPS CENTER MAIN A"                                                 , "FL"          , "GTMODCU"                ,
    "WICHITA COUNTY JAIL"                                                       , "TX"          , "XXWICHI"                ,
    "DOD DETENTION FACILITY AT FORT BLISS"                                      , "TX"          , "EROFCB"
  )

facility_attributes_unmatched_manual <-
  bind_rows(facility_attributes_unmatched_manual, website_matches) |>
  distinct(name, state, .keep_all = TRUE)

facility_attributes_unmatched <-
  facility_attributes_nocodes |>
  filter(is.na(detention_facility_code)) |>
  anti_join(
    name_code_match,
    by = c("name", "state")
  ) |>
  # keep only ICE sources (NOTE: I selected the ICE ones based on currently the only ones that match)
  filter(
    source %in% c("51185", "detention_management", "website"),
    date >= as.Date("2025-01-01")
  ) |>
  select(-detention_facility_code) |>
  left_join(
    facility_attributes_unmatched_manual |>
      filter(!is.na(state)),
    by = c("name", "state")
  ) |>
  left_join(
    facility_attributes_unmatched_manual |>
      filter(is.na(state)) |>
      select(-state),
    by = "name"
  ) |>
  mutate(
    detention_facility_code = coalesce(
      detention_facility_code.x,
      detention_facility_code.y
    ),
    .keep = "unused"
  )

facility_attributes <-
  bind_rows(
    facility_attributes,
    facility_attributes_nocodes,
    facility_attributes_unmatched
  ) |>
  filter(!is.na(detention_facility_code)) |>
  left_join(
    facilities_with_multiple_codes |> select(-state),
    by = c("detention_facility_code" = "detention_facility_code_2")
  ) |>
  mutate(
    # detention_facility_code_orig = detention_facility_code,
    detention_facility_code = case_when(
      !is.na(detention_facility_code_1) ~ detention_facility_code_1,
      TRUE ~ detention_facility_code
    )
  ) |>
  select(-detention_facility_code_1)

arrow::write_parquet(
  facility_attributes,
  "data/facilities-attributes-cleaned-with-codes.parquet"
)

is_likely_street_address <- function(values) {
  precomma <- stringr::str_split_i(values, ",", 1)
  !(str_detect(str_to_upper(values), "\\bP\\s*\\.?\\s*O\\s*\\.?\\s*BOX\\b") |
    !str_detect(precomma, "\\d"))
}

facility_pivot <-
  facility_attributes |>
  mutate(
    address_full = case_when(
      !is.na(address) ~ glue::glue(
        "{address}, {city}, {state}{if_else(!is.na(zip), str_c(' ', zip), '')}"
      ),
      TRUE ~ NA_character_
    )
  ) |>
  select(
    detention_facility_code,
    date,
    source,
    all_of(all_fields),
    address_full
  ) |>
  relocate(address_full, .after = name) |>
  pivot_longer(
    cols = all_of(c(all_fields, "address_full")),
    names_to = "variable",
    values_to = "value"
  ) |>
  filter(!is.na(value) & value != "")

arrow::write_parquet(
  facility_pivot,
  "data/facilities-values-long.parquet"
)

ice_sources <- c(
  "2015",
  "2017",
  "05655",
  "10-2554-527",
  "14-09300",
  "22955",
  "41855",
  "51185",
  "dedicated",
  "detention_management",
  "detentions",
  "website"
)

cells_with_errors <-
  tribble(
    ~detention_facility_code , ~source   , ~date                 , ~variable      , ~notes                                                                          ,
    "CBENDTX"                , "website" , as.Date("2026-02-21") , "address_full" , "address is wrong; should be 4909 FM (Farm to Market) 2826, Robstown, TX 78380"
  )

facility_latest_values <-
  facility_pivot |>
  anti_join(
    cells_with_errors,
    by = c("detention_facility_code", "date", "source", "variable")
  ) |>
  mutate(
    source_hierarchy = case_when(
      source %in%
        c(
          "05655",
          "2015",
          "2017",
          "22955",
          "41855",
          "51185",
          "dedicated",
          "detention_management",
          "detentions",
          "eoir",
          "hrw",
          "website"
        ) ~
        1,
      source %in% c("manual", "noccc_hold_rooms") ~ 2,
      TRUE ~ 0
    )
  ) |>
  filter(
    variable == "address_full" |
      !source %in% c("vera", "marshall", 'noccc_hold_rooms')
  ) |>
  arrange(
    detention_facility_code,
    variable,
    is_likely_street_address(value) | source == "manual",
    source_hierarchy,
    variable == "name" & source == "website",
    date
  ) |>
  group_by(detention_facility_code, variable) |>
  summarize(
    value_all = str_c(
      sort(unique(str_squish(value[
        !is.na(value) & value != "" & source %in% ice_sources
      ]))),
      collapse = "; "
    ),
    value = last(value[!is.na(value)]),
    source = last(source[!is.na(value)]),
    date = last(date[!is.na(value)]),
    .groups = "drop"
  ) |>
  left_join(
    facilities_with_multiple_codes |>
      select(detention_facility_code_1, detention_facility_code_2),
    by = c("detention_facility_code" = "detention_facility_code_1")
  ) |>
  rename(detention_facility_code_alt = detention_facility_code_2) |>
  relocate(detention_facility_code_alt, .after = detention_facility_code) |>
  mutate(value = str_squish(value))

arrow::write_parquet(
  facility_latest_values,
  "data/facilities-latest-values-long.parquet"
)
