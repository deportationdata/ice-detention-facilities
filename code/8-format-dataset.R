library(tidyverse)
library(sf)
library(sfarrow)

source("code/functions.R")

facility_augmented <-
  arrow::read_parquet(
    "data/facilities-augmented.parquet"
  ) |>
  as_tibble()

# format data

make_abbr_caps <- function(x, abbr) {
  pattern <- paste0("\\b(", paste(abbr, collapse = "|"), ")\\b")
  str_replace_all(
    x,
    regex(pattern, ignore_case = TRUE),
    ~ str_to_upper(.x)
  )
}

facility_formatted <-
  facility_augmented |>
  # TODO: need to remove things from dtm that have 0 people detained
  # XXWICHI is kept through the pipeline as a manual placeholder for Wichita
  # County Jail (no ICE code yet); the code is blanked just before write.
  mutate(
    state = if_else(detention_facility_code == "XXWICHI", "TX", state)
  ) |>
  # left_join(hospitals, by = c("name", "state")) |>
  mutate(
    name = name |>
      # one-off typo correction: "Crt" (seen in "Chavez Det Crt") -> "Ctr" so
      # the existing ctr -> center abbrev expansion handles it.
      str_replace_all("\\b[Cc][Rr][Tt]\\b", "Ctr") |>
      str_replace_all(regex("\\bmanhat\\b", ignore_case = TRUE), "Manhattan") |>
      # insert a space when a period sits between two letters (e.g. "St.hospital"
      # -> "St. hospital", "FED.CORR." -> "FED. CORR.") so title-case and word
      # boundaries work properly downstream.
      str_replace_all("(?<=[A-Za-z])\\.(?=[A-Za-z])", ". ") |>
      # strip periods only from single-letter acronyms (U.S., C.F., L.E.C.) so
      # the single-letter-collapse step can re-join them. Preserve periods in
      # word abbreviations (St., Ste., Mt., Jr., Inc.).
      str_replace_all("(?<=\\b[A-Za-z])\\.", " ") |>
      str_to_lower() |>
      str_replace_all(c(
        abbrev_expansions
      )) |>
      # strip periods that now trail expanded abbreviations (e.g. "medical."
      # after "med." was expanded). 4+ lowercase letters is long enough to
      # exclude Saint/month/honorific abbreviations like St., Ste., Jr., Mt.
      str_replace_all("(?<=[a-z]{4})\\.", "") |>
      str_replace_all("[‘’`]", "’") |>
      str_squish() |>
      # collapse space-separated single letters into abbreviations (e.g. "u s" -> "US")
      str_replace_all("(?:^|(?<= ))([a-z]( [a-z])+)(?= |$)", function(m) {
        str_to_upper(str_replace_all(m, " ", ""))
      }) |>
      tools::toTitleCase() |>
      # capitalize letter after apostrophe (O'hare -> O'Hare)
      str_replace_all("'([a-z])", function(m) {
        str_c("'", str_to_upper(str_sub(m, 2, 2)))
      }) |>
      # capitalize letter after "Mc" prefix (Mccreary -> McCreary)
      str_replace_all("\\bMc([a-z])", function(m) {
        str_c("Mc", str_to_upper(str_sub(m, 3, 3)))
      }) |>
      # uppercase standalone single letters (initials like "g" in "Dale g Haile")
      str_replace_all("\\b([a-z])\\b", str_to_upper) |>
      # fix possessive 's (the single-letter step above capitalizes it)
      str_replace_all("'S\\b", "'s") |>
      make_abbr_caps(
        abbr = c(
          "BI",
          "CCA",
          "CCNO",
          "CNMI",
          "DSMC",
          "HCA",
          "IAH",
          "LTAC",
          "NYC",
          "THOP",
          "F",
          "PD",
          "YMCA",
          "APSO",
          "LBJ",
          "US",
          "SSM",
          "CF",
          "C",
          "WD",
          "NE",
          "NW",
          "SE",
          "SW",
          "ICE",
          "USBP",
          "BHC",
          "JFK",
          "EGP",
          "CPC",
          "INS",
          "FMC",
          "CSP",
          "EOR",
          "SMC",
          "CBP",
          "MDC",
          state.abb
        )
      ) |>
      # normalize asymmetric spaces around hyphens: " -x" -> " - x", "x- " -> "x - "
      str_replace_all(" -(?=\\S)", " - ") |>
      str_replace_all("(?<=\\S)- ", " - ") |>
      # ensure there is a space after commas
      str_replace_all(",([^ ])", ", \\1") |>
      # ensure there is a space before an open parenthesis
      str_replace_all("([^ ])\\(", "\\1 (") |>
      # and after close parenthesis
      str_replace_all("\\)([^ ])", ") \\1") |>
      # drop , at end of name
      str_replace_all(",\\s*$", "") |>
      # drop space before comma
      str_replace_all("\\s+,", ",") |>
      # strip trailing period from state abbrev at end of name (e.g. "MS.")
      str_replace_all(
        paste0("\\b(", paste(state.abb, collapse = "|"), ")\\.\\s*$"),
        "\\1"
      ) |>
      # expand SO -> Sheriff's Office when preceded by Parish/County
      str_replace_all(
        "\\b(Parish|County)\\s+[Ss][Oo]\\b",
        "\\1 Sheriff's Office"
      ) |>
      # uppercase any remaining standalone SO (facility abbrev, either direction)
      str_replace_all("\\b[Ss][Oo]\\b", "SO") |>
      # add period to Saint abbreviations (St Luke's -> St. Luke's)
      str_replace_all("\\bSt\\b(?=\\s+[A-Z])", "St.") |>
      str_squish(),
    address = address |>
      str_to_title() |>
      # uppercase directional abbreviations NE/NW/SE/SW
      str_replace_all("\\b(Ne|Nw|Se|Sw)\\b", toupper) |>
      # uppercase two-letter state abbrev before a zip
      str_replace_all("(?<=, )[A-Za-z]{2}(?=(?: \\d{5}|$))", toupper),
    address_full = address_full |>
      str_to_title() |>
      str_replace_all("\\b(Ne|Nw|Se|Sw)\\b", toupper) |>
      str_replace_all("(?<=, )[A-Za-z]{2}(?=(?: \\d{5}|$))", toupper),
    city = city |> clean_city() |> str_to_title(),
    state = str_to_upper(state),
    type = type |> str_to_title() |> replace_na(""),
    type_detailed = type_detailed |> replace_na(""),
    type = snakecase::to_title_case(
      type,
      abbreviations = c(
        "IGSA",
        "IGA",
        "USMS",
        "DIGSA",
        "CDF",
        "BOP",
        "USBP",
        "MOC",
        "SPC",
        "DOD",
        "CBP"
      )
    ),
    type_detailed = snakecase::to_title_case(
      type_detailed,
      abbreviations = c(
        "IGSA",
        "IGA",
        "USMS",
        "DIGSA",
        "CDF",
        "BOP",
        "USBP",
        "MOC",
        "SPC",
        "DOD",
        "CBP"
      )
    ) |>
      na_if(""),
    # medical, hold/staging, family, juvenile, dod
    # type_detailed = ERROR,

    # medical, hold, staging, dedicated govt contract, nondedicated govt contract, ice cdf, ice spc, cbp
    type_ddp = case_when(
      detention_facility_code %in%
        c("CBPORIL", "JFKTSNY", "URSLATX") |
        str_detect(type_detailed, "CBP|USBP") |
        str_detect(type, "CBP|USBP") ~ "CBP holding",

      str_detect(type, "Staging") |
        str_detect(type_detailed, "Staging") ~ "ICE staging",

      str_detect(detention_facility_code, "HOLD") |
        str_detect(type_detailed, "Hold") |
        str_detect(type, "Hold") ~ "ICE holding",

      detention_facility_code %in%
        c("CSLLFTX", "THPSCTX") |
        str_detect(type_detailed, "Hospital") |
        str_detect(type, "Hospital") ~ "Medical",

      detention_facility_code == "BIINCCO" |
        str_detect(type_detailed, "CDF") |
        str_detect(type, "CDF") ~ "CDF",

      str_detect(type_detailed, "SPC") | str_detect(type, "SPC") ~ "SPC",

      detention_facility_code %in%
        c("TASTDTX", "DILLSAF", "HARRIMS", "FLDSSFS") |
        str_starts(detention_facility_code, "GTMO") |
        str_starts(detention_facility_code, "BOP") |
        str_starts(detention_facility_code_alt, "BOP") |
        str_detect(
          type_detailed,
          "IGSA|IGA|DIGSA|County|Police|State|BOP|DOD|MOC"
        ) |
        str_detect(
          type,
          "IGSA|IGA|DIGSA|County|Police|State|BOP|DOD|MOC"
        ) ~ "Government contract",

      str_detect(
        name,
        "Hospital|Medical|Health|Memorial|Rehab|\\bMc\\b"
      ) ~ "Medical",

      str_detect(name, "County|PD|BRRJ|Correctional|Parish") |
        detention_facility_code %in% "FLBAKCI" ~ "Government contract",

      TRUE ~ type
    ),
    male_female = male_female |>
      str_to_lower() |>
      recode_values(
        "male" ~ "M",
        "female" ~ "F",
        "female/male" ~ "M/F"
      ),
    type_population = case_when(
      str_detect(type_detailed, "Fam") |
        str_detect(type, "Fam") ~ "Family",
      str_detect(type_detailed, "Juv") | str_detect(type, "Juv") ~ "Juvenile",
      male_female == "M/F" ~ "Adult (male/female)",
      male_female == "M" ~ "Adult (male)",
      male_female == "F" ~ "Adult (female)",
      TRUE ~ "Adult (unknown gender)"
    ),

    # docket = str_to_upper(docket),
    over_under_72 = str_to_title(over_under_72)
  ) |>
  arrange(name) |>
  # remove the cbp movement coordination area, not an actual facility
  filter(!detention_facility_code == "UCBPMCA")

facility_formatted <-
  facility_formatted |>
  select(
    detention_facility_code,
    # detention_facility_code_alt, # not needed for the 2025 forward period
    name,
    address,
    city,
    county,
    county_fips_code,
    core_based_statistical_area,
    core_based_statistical_area_code,
    core_based_statistical_area_type,
    combined_statistical_area,
    combined_statistical_area_code,
    state,
    state_fips_code,
    zip,
    address_full,
    latitude,
    longitude,
    field_office,
    days_with_detentions_daily_last_year,
    days_with_detentions_midnight_last_year,
    average_daily_population_last_year,
    average_midnight_population_last_year,
    max_daily_population_last_year,
    max_midnight_population_last_year
  )

# XXWICHI is a manual placeholder — clear the code in the final output so
# downstream users see a blank code rather than the internal XX marker.
facility_formatted <-
  facility_formatted |>
  mutate(
    detention_facility_code = if_else(
      detention_facility_code == "XXWICHI",
      NA_character_,
      detention_facility_code
    )
  )

arrow::write_parquet(
  facility_formatted,
  "data/facilities-latest.parquet"
)

facility_formatted |>
  st_as_sf(
    coords = c("longitude", "latitude"),
    na.fail = FALSE,
    crs = 4326,
    remove = FALSE
  ) |>
  sfarrow::st_write_parquet("data/facilities-latest-sf.parquet")

# # metadata for diagnostics
# best_values_metadata <-
#   best_values |>
#   select(
#     detention_facility_code,
#     variable,
#     n_changes,
#     n_unique,
#     has_reversion,
#     is_aba,
#     review_flag
#   ) |>
#   pivot_wider(
#     names_from = variable,
#     values_from = c(n_changes, n_unique, has_reversion, is_aba, review_flag),
#     names_glue = "{variable}_{.value}"
#   )

# facility_final_metadata <-
#   facility_final |>
#   left_join(best_values_metadata, by = "detention_facility_code")
