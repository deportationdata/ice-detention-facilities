# cleaning helper functions
clean_text <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all(
      "(?<=\\b)([A-Za-z]\\.)+(?=\\b)",
      ~ str_remove_all(.x, "\\.")
    ) |>
    str_replace_all("[[:punct:]]", " ") |>
    iconv(from = "UTF-8", to = "ASCII//TRANSLIT") |>
    str_squish()
}

abbrev_expansions <- c(
  "\\bcnt\\b" = "center",
  "\\bctr\\b" = "center",
  "\\bcn\\b" = "center",
  "\\bcen\\b" = "center",
  "\\bcent\\b" = "center",
  "\\bcor\\b" = "correctional",
  "\\bcorr\\b" = "correctional",
  "\\bcorrec\\b" = "correctional",
  "\\bcty\\b" = "county",
  "\\bco\\b" = "county",
  "\\bcplx\\b" = "complex",
  "\\bbldg\\b" = "building",
  "\\ext\\b" = "extension",
  "\\bcolo\\b" = "colorado",
  "\\bdiv\\b" = "division",
  "\\bcsp\\b" = "california state prison",
  "\\bcdf\\b" = "contract detention facility",
  "\\bci\\b" = "correctional institution",
  "\\bcorrects\\b" = "corrections",
  "\\btrm\\b" = "terminal",
  "\\bofc\\b" = "office",
  "\\bfo\\b" = "field office",
  "\\bfmc\\b" = "federal medical center",
  "\\bdept\\b" = "department",
  "\\bdept of corr\\b" = "department of corrections",
  "\\bcr\\b" = "creek",
  "\\bdet\\b" = "detention",
  "\\bdf\\b" = "detention facility",
  "\\bfed\\b" = "federal",
  "\\bmet\\b" = "metropolitan",
  "\\bfac\\b" = "facility",
  "\\bfam\\b" = "family",
  "\\bfclty\\b" = "facility",
  "\\bfacilty\\b" = "facility",
  "\\bfo\\b" = "field office",
  "\\bhosp\\b" = "hospital",
  "\\bhsp\\b" = "hospital",
  "\\bhlth\\b" = "health",
  "\\bjuv\\b" = "juvenile",
  "\\bpen\\b" = "penitentiary",
  "\\bpub\\b" = "public",
  "\\bproc\\b" = "processing",
  "\\breg\\b" = "regional",
  "\\brm\\b" = "room",
  "\\brms\\b" = "rooms",
  "\\bsfty\\b" = "safety",
  "\\bspc\\b" = "service processing center",
  "\\busp\\b" = "us penitentiary",
  "\\bholdroom\\b" = "hold room",
  "\\bcf\\b" = "correctional facility",
  "\\bmed\\b" = "medical",
  "\\bfci\\b" = "federal correctional institution",
  "\\buspen\\b" = "us penitentiary",
  "\\bcc\\b" = "correctional institution",
  "\\bero\\b" = "enforcement and removal operations",
  "\\bdro\\b" = "detention and removal operations",
  "\\bdist\\b" = "district",
  "\\bdetn\\b" = "detention",
  "\\bdpt\\b" = "department",
  "\\bairp\\b" = "airport",
  "\\bcntr\\b" = "center",
  "\\bdetfac\\b" = "detention facility",
  "\\bste\\b(?!\\.?\\s+(genevieve|marie))" = "suite",
  "\\bstes\\b(?!\\.?\\s+(genevieve|marie))" = "suites",
  "\\badc\\b" = "adult detention center",
  "\\bfaci\\b" = "facility",
  "\\bjdc\\b" = "juvenile detention center",
  "\\bipc\\b" = "immigration processing center",
  "\\bft\\b" = "fort",
  "\\bc i\\b" = "correctional institution",
  "\\bci\\b" = "correctional institution",
  "\\bd f\\b" = "detention facility",
  "\\bemerg\\b" = "emergency",
  "\\bserv\\b" = "service",
  "\\bsvcs\\b" = "services",
  "\\bfacil\\b" = "facility",
  "\\bd o c\\b" = "department of corrections",
  "\\brem\\b" = "removal",
  "\\bops\\b" = "operations",
  "\\bop\\b" = "operation"
)

clean_facility_name <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all("[[:punct:]]", " ") |>
    str_replace_all(c(
      abbrev_expansions
    )) |>
    str_squish()
}

clean_city <- function(str) {
  str |>
    str_to_lower() |>
    str_replace_all(c(
      "\\bmt\\b" = "mount",
      "\\bft\\b" = "fort",
      "\\bste\\b" = "saint",
      "\\bst\\b" = "saint",
      "\\bspgs\\b" = "springs",
      "\\bspg\\b" = "springs",
      "\\bn\\b" = "north"
    ))
}

clean_street_address <- function(str) {
  str |>
    str_trim() |>
    str_to_lower() |>
    str_replace_all(c(
      "\\bstreet\\b" = "st",
      "\\bavenue\\b" = "ave",
      "\\bboulevard\\b" = "blvd",
      "\\bdrive\\b" = "dr",
      "\\broad\\b" = "rd",
      "\\bhighway\\b" = "hwy",
      "\\bplace\\b" = "pl",
      "\\blane\\b" = "ln",
      "\\bsquare\\b" = "sq",
      "\\bterrace\\b" = "ter",
      "\\bparkway\\b" = "pkwy",
      "\\bcourt\\b" = "ct",
      "\\bnorth\\b" = "n",
      "\\bsouth\\b" = "s",
      "\\beast\\b" = "e",
      "\\bwest\\b" = "w",
      "\\bfort\\b" = "ft"
    ))
}

# -----------------------Helpers
# -----------------------
abbr_dict <- c(
  "-" = " ",
  "&" = " AND ",

  "\\bst\\.?\\b" = " st ",
  "\\bsaint\\b" = " st ",
  "\\bctr\\b" = " center ",
  "\\bctrs\\b" = " centers ",
  "\\bmed\\b" = " medical ",
  "\\bhosp\\b" = " hospital ",
  "\\bmc\\b" = " MEDICAL CENTER ",
  "\\buniv\\b" = " UNIVERSITY ",
  "\\bmt\\b" = "mount",
  "\\bft\\b" = "fort",
  "\\bste\\b" = "saint",
  "\\bst\\b" = "saint",
  "\\bspgs\\b" = "springs",
  "\\bspg\\b" = "springs",
  "\\bn\\b" = "north",
  "\\bs\\b" = "south",
  "\\be\\b" = "east",
  "\\bw\\b" = "west",
  "\\bcnt\\b" = "center",
  "\\bctr\\b" = "center",
  "\\bcor\\b" = "correctional",
  "\\bcorr\\b" = "correctional",
  "\\bcorrec\\b" = "correctional",
  "\\bcty\\b" = "county",
  "\\bco\\b" = "county",
  "\\bcplx\\b" = "complex",
  "\\bcdf\\b" = "contract detention facility",
  "\\bdept\\b" = "department",
  "\\bdept of corr\\b" = "department of corrections",
  "\\bfci\\b" = "federal correctional institution",
  "\\bdet\\b" = "detention",
  "\\bdf\\b" = "detention facility",
  "\\bfed\\b" = "federal",
  "\\bmet\\b" = "metropolitan",
  "\\bfac\\b" = "facility",
  "\\bfam\\b" = "family",
  "\\bfclty\\b" = "facility",
  "\\bfacilty\\b" = "facility",
  "\\bfo\\b" = "field office",
  "\\bhosp\\b" = "hospital",
  "\\bhsp\\b" = "hospital",
  "\\bhlth\\b" = "health",
  "\\bjuv\\b" = "juvenile",
  "\\bpen\\b" = "penitentiary",
  "\\bpub\\b" = "public",
  "\\bproc\\b" = "processing",
  "\\breg\\b" = "regional",
  "\\brm\\b" = "room",
  "\\brms\\b" = "rooms",
  "\\bsfty\\b" = "safety",
  "\\bspc\\b" = "service processing center",
  "\\busp\\b" = "us penitentiary",
  "\\breg\\b" = "regional",
  "\\bmem\\b" = "memorial",
  "\\bhlth\\b" = "health",
  "\\binst\\b" = "institution"
)

normalize_name <- function(x) {
  x |>
    str_to_upper() |>
    str_replace_all("[[:punct:]]", " ") |>
    str_replace_all(abbr_dict) |>
    str_squish()
}

generic_facility_words <- c(
  "HOSPITAL",
  "HOSPITALS",
  "HOSP",
  "MEDICAL",
  "CENTER",
  "CENTERS",
  "HEALTH",
  "REGIONAL",
  "MEMORIAL",
  "SYSTEM",
  "CLINIC"
)
