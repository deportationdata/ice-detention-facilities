library(tidyverse)
library(tidylog)

source("code/functions.R")

facility_attributes <- arrow::read_feather(
  "data/facilities-attributes-raw.feather"
)

name_state_match <-
  arrow::read_feather(
    "data/facilities-name-state-match.feather"
  )

# goal here is for the data where there is no code to fuzzy match by name within states to get a code
# then later we can just merge on code

facility_code <-
  facility_attributes |>
  filter(
    !is.na(detention_facility_code)
  ) |>
  transmute(
    name,
    state,
    detention_facility_code,
    date,
    source
  )

facility_no_code <-
  facility_attributes |>
  filter(
    is.na(detention_facility_code)
  ) |>
  filter(
    !source %in%
      c(
        "hospitals",
        "hifld_prisons",
        "hifld_local_law_enforcement",
        "jails_prisons",
        "eoir",
        "vera",
        "marshall"
      )
  ) |>
  transmute(name, state, date, source) |>
  mutate(
    # missing state for GTMO
    state = case_when(
      name ==
        "Naval Station Guantanamo Bay (JTF Camp Six and Migrant Ops Center Main A)" ~ "GU",
      TRUE ~ state
    )
  )

exact_matches <-
  facility_no_code |>
  mutate(name_join = clean_text(name)) |>
  inner_join(
    name_state_match |>
      mutate(name_join = clean_text(name)) |>
      rename(source_state = source, date_state = date),
    by = c("state", "name_join")
  ) |>
  group_by(detention_facility_code, name = name.x, state) |>
  slice_max(date, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(
    detention_facility_code,
    name,
    state,
    date,
    source,
    date_state,
    source_state
  )

name_state_match <-
  bind_rows(
    name_state_match,
    exact_matches
  ) |>
  arrange(detention_facility_code, name, state, source, date) |>
  group_by(detention_facility_code, name, state) |>
  slice_max(date, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(-date_state, -source_state)
# distinct(detention_facility_code, name, state, .keep_all = TRUE)

exact_non_matches <-
  facility_no_code |>
  mutate(name_join = clean_text(name)) |>
  anti_join(
    name_state_match |>
      mutate(name_join = clean_text(name)),
    by = c("state", "name_join")
  ) |>
  distinct(name, state, .keep_all = TRUE) |>
  filter(!is.na(name_join), name_join != "")

# library(fuzzylink)

# name_state_match |>
#   mutate(name_join = clean_text(name)) |>
#   select(1:3) |>
#   # find any row with any missing value
#   filter(if_any(everything(), ~ is.na(.x) | .x == ""))

# matches_cleaned <-
#   fuzzylink::fuzzylink(
#     exact_non_matches |> select(name, state, name_join),
#     name_state_match |>
#       mutate(name_join = clean_text(name)) |>
#       select(name, state, name_join, detention_facility_code),
#     record_type = "name of detention facility",
#     learner = "ranger",
#     instructions = "compare facility names to find possible matches; some are hospitals or medical centers and some are jails, prisons, or detention facilities",
#     by = "name_join",
#     blocking.variables = "state",
#     max_labels = 50000,
#     openai_api_key = "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"
#   )

# write_rds(matches_cleaned, "~/downloads/attributes-matches-cleaned-feb1626.rds")

# matches_cleaned <- read_rds(
#   "~/downloads/attributes-matches-cleaned-feb1626.rds"
# )

# matches_cleaned |>
#   filter(match == "Yes") |>
#   select(
#     name.x,
#     state,
#     detention_facility_code,
#     name_matched = name.y,
#     match,
#     match_probability
#   ) |>
#   group_by(name.x, detention_facility_code, state) |>
#   slice_max(order_by = match_probability, n = 1, with_ties = FALSE) |>
#   ungroup() |>
#   filter(match_probability >= 0.1) |>
#   mutate(
#     match_probability = round(match_probability * 100)
#   ) |>
#   select(-match) |>
#   relocate(
#     name.x,
#     name_matched,
#     state,
#     detention_facility_code,
#     match_probability
#   ) |>
#   clipr::write_clip()

# look into bighorn, is this matching right?
fuzzy_matches_manual <- tribble(
  ~name.x                                                      , ~name_matched                                 , ~state , ~detention_facility_code , ~match_probability ,
  "ACI"                                                        , "ACI (CRANSTON, RHODE ISLAND)"                , "RI"   , "RICRANS"                , "53"               ,
  "ALEXANDRIA STAGING FACILI"                                  , "ALEXANDRIA STAGING FACILITY"                 , "LA"   , "JENATLA"                , "90"               ,
  "ASHLAND DEF.CORR.INST."                                     , "ASHLAND FEDERAL CORRECTIONAL INSTITUTE"      , "KY"   , "BOPASH"                 , "81"               ,
  "AVOYELLES WOMEN CORR CN"                                    , "AVOYELLES WOMEN'S CORR CN"                   , "LA"   , "AVWOMLA"                , "94"               ,
  "Abyon | Farmville Detention Center"                         , "FARMVILLE DETENTION CENTER"                  , "VA"   , "FRMVLVA"                , "75"               ,
  "Alamance County Detention Center"                           , "ALAMANCE COUNTY DETENTION FACILITY"          , "NC"   , "ALAMCNC"                , "98"               ,
  "BELLEVILLE PD"                                              , "BELLEVILLE PD, BELVIL, MI"                   , "MI"   , "BELPDMI"                , "98"               ,
  "BERKS COUNTY JAIL"                                          , "BERKS COUNTY JAIL, PA"                       , "PA"   , "BERKSPA"                , "100"              ,
  "BERKS COUNTY RESIDENTIAL CENTER3"                           , "BERKS COUNTY RESIDENTIAL CENTER"             , "PA"   , "BCORCPA"                , "98"               ,
  "BERLIN FED. CORR. INST."                                    , "FCI BERLIN"                                  , "NH"   , "BOPBER"                 , "40"               ,
  "BETHANY C.S."                                               , "BETHANY C.S.,GRAND RAPIDS"                   , "MI"   , "BETGRMI"                , "91"               ,
  "BIG HORN COUNTY JAIL"                                       , "BIG HORN COUNTY JAIL, MT"                    , "MT"   , "BIGHOMT"                , "100"              ,
  "BIG HORN COUNTY JAIL"                                       , "BIG HORN COUNTY JAIL, WY"                    , "WY"   , "BIGHOWY"                , "100"              ,
  "BUCKS CNTY JAIL"                                            , "BUCKS CNTY JAIL, PA"                         , "PA"   , "BUCKSPA"                , "100"              ,
  "BUFFALO SERVICE PROCESSING CENTER"                          , "BUFFALO (BATAVIA) SERVICE PROCESSING CENTER" , "NY"   , "BTV"                    , "88"               ,
  "BURLEIGH CO. JAIL"                                          , "BURLEIGH CO. JAIL, ND"                       , "ND"   , "BURLEND"                , "99"               ,
  "BUTLER COUNTY JAIL"                                         , "BUTLER COUNTY JAIL, PA"                      , "PA"   , "BUTLEPA"                , "99"               ,
  "Butler County Sheriff’s Office"                             , "BUTLER COUNTY JAIL"                          , "OH"   , "BUTLEOH"                , "83"               ,
  "CALHOUN CO."                                                , "CALHOUN CO., BATTLE CR,MI"                   , "MI"   , "CALHOMI"                , "94"               ,
  "CANADIAN COUNTY"                                            , "CANADIAN COUNTY, EL RENO,"                   , "OK"   , "CANADOK"                , "81"               ,
  "CARTER COUNTY"                                              , "CARTER COUNTY (ARDMORE, OKLAHOMA)"           , "OK"   , "CARTEOK"                , "80"               ,
  "CASA GRANDE REC. MED. CENTER"                               , "CASA GRANDE REG. MED. CENTER"                , "AZ"   , "CGRMCAZ"                , "93"               ,
  "CASCADE COUNTY JAIL"                                        , "CASCADE COUNTY JAIL, MT"                     , "MT"   , "CASCAMT"                , "100"              ,
  # "CCA CHER-TAZ DET.CTR."                                      , "CCA CENTRAL ARIZONA DETENTION CENTER"          , "AZ"   , "CCADCAZ"                , "42"               ,
  "CENTRAL LOUISIANA ICE PROCESSING CENTER (CLIPC)"            , "CENTRAL LOUISIANA ICE PROC CTR"              , "LA"   , "JENADLA"                , "95"               ,
  "CHESTER CNTY JAIL"                                          , "CHESTER CNTY JAIL, PA"                       , "PA"   , "CHESTPA"                , "100"              ,
  "CHIPPEWA CO."                                               , "CHIPPEWA CO., SSM"                           , "MI"   , "CHIPPMI"                , "66"               ,
  "CIMMARRON CORR FACILITY"                                    , "CIMARRON CORRECTIONAL FACILITY"              , "OK"   , "OKCIMMC"                , "97"               ,
  "CLARK COUNTY JAIL (ID)"                                     , "CLARK COUNTY JAIL"                           , "ID"   , "CLARKID"                , "100"              ,
  "CLARK COUNTY JAIL (IN)"                                     , "CLARK COUNTY JAIL"                           , "IN"   , "CLARKIN"                , "99"               ,
  "CLEARFIELD CO. JAIL"                                        , "CLEARFIELD CO. JAIL, PA"                     , "PA"   , "CLEARPA"                , "100"              ,
  "CNMI Department of Corrections"                             , "SAIPAN DEPARTMENT OF CORRECTIONS"            , "MP"   , "MPSIPAN"                , "86"               ,
  "CODINGTON CO. JAIL"                                         , "CODINGTON CO. JAIL, SD"                      , "SD"   , "CODINSD"                , "100"              ,
  "COMFORT INN & SUITES - EL PASO"                             , "COMFORT INN STE ELP AIRP"                    , "TX"   , "CISAETX"                , "71"               ,
  "COOK INLET PRETRIAL"                                        , "COOK INLET PRETRIAL, ANCH"                   , "AK"   , "AKCOOKI"                , "99"               ,
  "CORR. CTR OF NORTHWEST OHIO"                                , "CORRECTIONS CENTER OF NW OHIO"               , "OH"   , "OHNORWE"                , "67"               ,
  "CRAWFORD CO"                                                , "CRAWFORD CO, GRAYLING,MI."                   , "MI"   , "CRAWFMI"                , "92"               ,
  "CSC SO (FU) MUNIC JAIL"                                     , "CSC SO.FULTON MUNIC.JAIL"                    , "GA"   , "CSCSFGA"                , "77"               ,
  "California City Detention Facility"                         , "CALIFORNIA CITY CORRECTIONAL CENTER"         , "CA"   , "CACTYCA"                , "92"               ,
  "Campbell County Detention Center"                           , "CAMPBELL CO DET CTR"                         , "KY"   , "CPBDCKY"                , "81"               ,
  "Central Arizona Florence Correctional Center"               , "CCA FLORENCE CORRECTIONAL CENTER"            , "AZ"   , "CCAFLAZ"                , "61"               ,
  "Central Louisiana ICE Processing Center"                    , "CENTRAL LOUISIANA ICE PROC CTR"              , "LA"   , "JENADLA"                , "99"               ,
  "Central Louisiana ICE Processing Center (CLIPC)"            , "CENTRAL LOUISIANA ICE PROC CTR"              , "LA"   , "JENADLA"                , "95"               ,
  "Central Louisiana Ice Processing Center (CLIPC)"            , "CENTRAL LOUISIANA ICE PROC CTR"              , "LA"   , "JENADLA"                , "95"               ,
  "Central Louisiana Ice Processing Center (clipc)"            , "CENTRAL LOUISIANA ICE PROC CTR"              , "LA"   , "JENADLA"                , "95"               ,
  "Chippewa County Correctional Facility"                      , "CHIPPEWA CO., SSM"                           , "MI"   , "CHIPPMI"                , "66"               ,
  "ClINTON COUNTY CORRECTIONAL"                                , "CLINTON COUNTY CORRECTIONAL FACILITY"        , "PA"   , "CLINTPA"                , "100"              ,
  "Clinton County Sheriff's Office"                            , "CLINTON COUNTY JAIL"                         , "IN"   , "CLINTIN"                , "89"               ,
  "Coastal Bend Detention Center"                              , "COASTAL BEND DETENTION FACILITY"             , "TX"   , "CBENDTX"                , "93"               ,
  "CoreCivic Laredo Processing Center"                         , "LAREDO PROCESSING CENTER"                    , "TX"   , "LRDICDF"                , "88"               ,
  "CoreCivic Webb County Detention Center"                     , "WEBB COUNTY DETENTION CENTER (CCA)"          , "TX"   , "WEBDCTX"                , "86"               ,
  "Corrections Center of Northwest Ohio (CCNO)"                , "CORRECTIONS CENTER OF NW OHIO"               , "OH"   , "OHNORWE"                , "97"               ,
  "DAVISON COUNTY"                                             , "DAVISON COUNTY, SD"                          , "SD"   , "DAVISSD"                , "99"               ,
  "DENVER CONTRACT DETENTION FACILITY (CDF) II"                , "Denver CDF II"                               , "CO"   , "DENIICO"                , "70"               ,
  "DEPTARTMENT OF CORRECTIONS HAGATNA"                         , "DEPT OF CORRECTIONS-HAGATNA"                 , "GU"   , "GUDOCHG"                , "90"               ,
  "DEVEREAUX ARIAONA"                                          , "DEVEREAUX ARIZONA"                           , "AZ"   , "DEVRXAZ"                , "83"               ,
  "Denver Contract Detention Facility (Aurora)"                , "DENVER CONTRACT DETENTION FACILITY"          , "CO"   , "DENICDF"                , "100"              ,
  "Dodge Detention Facility"                                   , "DODGE COUNTY JAIL"                           , "WI"   , "DODGEWI"                , "55"               ,
  "EL PASO SOFT SIDED FACILI"                                  , "EL PASO SOFT SIDED FACILITY"                 , "TX"   , "EPSSFTX"                , "84"               ,
  "EOTOH COUNTY CORRECTION CENTER"                             , "ECTOR COUNTY CORRECTION CENTER"              , "TX"   , "ECTORTX"                , "79"               ,
  "ESSEX CO. JAIL"                                             , "ESSEX CO. JAIL, MIDDLETON"                   , "MA"   , "ESSEXMA"                , "88"               ,
  "ETOWAH COUNTY JAIL"                                         , "ETOWAH COUNTY JAIL (AL)"                     , "AL"   , "ETOWAAL"                , "93"               ,
  "Elmore County Detention Center (Elmore County Jail)"        , "ELMORE COUNTY JAIL"                          , "ID"   , "ELMORID"                , "89"               ,
  "Eloy Detention Center"                                      , "ELOY FEDERAL CONTRACT FACILITY"              , "AZ"   , "EAZ"                    , "79"               ,
  "FDC Miami"                                                  , "MIAMI FED DET.CENTER"                        , "FL"   , "BOPMIM"                 , "41"               ,
  "FOLKSTON D RAY ICE PROCES"                                  , "FOLKSTON D RAY ICE PROCESSING CTR"           , "GA"   , "FIPCDGA"                , "97"               ,
  "FOLKSTON ICE PROCESSING CENTER ANNEX"                       , "FOLKSTON ANNEX IPC"                          , "GA"   , "FIPCAGA"                , "87"               ,
  "FRANKLIN COUNTY JAIL"                                       , "FRANKLIN COUNTY JAIL (VERMONT)"              , "VT"   , "FRACOVT"                , "88"               ,
  "FRANKLIN COUNTY JAIL"                                       , "FRANKLIN COUNTY JAIL, ME"                    , "ME"   , "FRANKME"                , "99"               ,
  "FREMONT COUNTY JAIL"                                        , "FREMONT COUNTY JAIL, CO"                     , "CO"   , "FREMOCO"                , "96"               ,
  "FREMONT COUNTY JAIL"                                        , "FREMONT COUNTY JAIL, WY"                     , "WY"   , "FREMOWY"                , "100"              ,
  "Federal Detention Center, Honolulu (FDC Honolulu)"          , "HONOLULU FEDERAL DETENTION CENTER"           , "HI"   , "BOPHON"                 , "85"               ,
  "Folkston D Ray ICE Processing Center"                       , "FOLKSTON D RAY ICE PROCESSING CTR"           , "GA"   , "FIPCDGA"                , "94"               ,
  "Folkston ICE Processing Center (Annex)"                     , "FOLKSTON ANNEX IPC"                          , "GA"   , "FIPCAGA"                , "87"               ,
  "Folkston ICE Processing Center (Main)"                      , "FOLKSTON MAIN IPC"                           , "GA"   , "FIPCMGA"                , "77"               ,
  "Freeborn County Jail Services"                              , "FREEBORN COUNTY ADULT DETENTION CENTER"      , "MN"   , "FREEBMN"                , "93"               ,
  "GRAND FORKS COUNTY GORREC"                                  , "GRAND FORKS COUNTY CORREC"                   , "ND"   , "GFCORND"                , "86"               ,
  "Geauga County Safety Center"                                , "GEAUGA COUNTY JAIL"                          , "OH"   , "GEAUGOH"                , "87"               ,
  "Goose Creek Correctional"                                   , "GOOSE CREEK CORRECTIONAL CENTER"             , "AK"   , "AKGSCCC"                , "99"               ,
  "Grand Forks County Correctional Center"                     , "GRAND FORKS COUNTY CORRECTIONAL FACILITY"    , "ND"   , "GFCORND"                , "98"               ,
  "Greentree Inn Houston Iah"                                  , "GREENTREE INN HOUSTON IAH AIRPORT"           , "TX"   , "HOUGTTX"                , "100"              ,
  "Guam Department of Corrections, Hagatna Detention Facility" , "DEPARTMENT OF CORRECTIONS HAGATNA"           , "GU"   , "GUDOCHG"                , "83"               ,
  "HANCOCK COUNTY PUBLIC SAFETY COMPLEX"                       , "HANCOCK CO PUB SFTY CPLX"                    , "MS"   , "HCPSCMS"                , "88"               ,
  "HOLIDAY INN EXPR & STES PHO/CHNDLR"                         , "HOLIDAY INN EXPRESS CASA DE LA LUZ"          , "AZ"   , "HESPCAZ"                , "90"               ,
  "HOLIDAY INN EXPRESS & SUITES EL PASO"                       , "HOLIDAY INN EXPRESS & SUITES"                , "TX"   , "HIELPTX"                , "97"               , # not so sure about this one
  "HOSANNA GIRLS RANCH"                                        , "HOSANA GIRLS RANCH"                          , "CA"   , "CAHGIRL"                , "98"               ,
  "Hancock County Public Safety Complex"                       , "HANCOCK CO PUB SFTY CPLX"                    , "MS"   , "HCPSCMS"                , "88"               ,
  "IAH Polk Adult Detention Facility"                          , "IAH SECURE ADULT DETENTION FACILITY (POLK)"  , "TX"   , "POLKCTX"                , "84"               ,
  "INH STATE PRISON FOR WOMEN"                                 , "NH STATE PRISON FOR WOMEN"                   , "NH"   , "NHSPWOM"                , "80"               ,
  "INS/WACKENHUT CON/FAC"                                      , "INS/WACKENHUT CON/FAC, NY"                   , "NY"   , "QUEICDF"                , "99"               ,
  "IONIA CO."                                                  , "IONIA CO., IONIA, MI."                       , "MI"   , "IONIAMI"                , "86"               ,
  "JFK INTERNATIONAL AP - T"                                   , "JFK INTERNATIONAL AP - T7"                   , "NY"   , "JFKTSNY"                , "98"               ,
  "JOE CORLEY ICE PROCESSING CENTER"                           , "JOE CORLEY PROCESSING CTR"                   , "TX"   , "JCRLYTX"                , "94"               ,
  "Joe Corley Processing Center"                               , "JOE CORLEY PROCESSING CTR"                   , "TX"   , "JCRLYTX"                , "97"               ,
  "KARNES COUNTY IMMIGRATION PROCESSING CENTER"                , "KARNES CO IMMIGRATION PROCESS CTR"           , "TX"   , "KRNRCTX"                , "98"               ,
  "KARNES COUNTY RESIDENTIAL CENTER2"                          , "KARNES COUNTY RESIDENTIAL CENTER"            , "TX"   , "KRNRCTX"                , "98"               ,
  "KENT CO."                                                   , "KENT COUNTY JAIL"                            , "MI"   , "KENTCMI"                , "82"               ,
  "Karnes County Immigration Processing Center"                , "KARNES CO IMMIGRATION PROCESS CTR"           , "TX"   , "KRNRCTX"                , "98"               ,
  "Kay County Detention Center"                                , "KAY COUNTY JUSTICE FACILITY"                 , "OK"   , "KCOJFOK"                , "87"               ,
  "Kenton County Detention Center"                             , "KENTON CO DETENTION CTR"                     , "KY"   , "KENTOKY"                , "96"               ,
  "LA PALMA CORRECTION CENTER - APSO"                          , "LA PALMA CORR CTR APSO"                      , "AZ"   , "LPAPSAZ"                , "71"               ,
  "LACKAWANA CNTY JAIL"                                        , "LACKAWANA CNTY JAIL, PA"                     , "PA"   , "LACKAPA"                , "100"              ,
  "LAPEER CO."                                                 , "LAPEER CO., LAPEER. MI."                     , "MI"   , "LAPEEMI"                , "95"               ,
  "LASALLE ICE PROCESSING CENTER (JENA)"                       , "LASALLE ICE PROCESSING CENTER"               , "LA"   , "JENADLA"                , "97"               ,
  "LAUREL COUNTY CORRECTIONAL CENTER"                          , "LAUREL COUNTY CORRECTIONS"                   , "KY"   , "LARELKY"                , "100"              ,
  "LAWRENCE CO. JAIL"                                          , "LAWRENCE CO. JAIL, SD"                       , "SD"   , "LAWRESD"                , "92"               ,
  "LEHIGH COUNTY JAIL"                                         , "LEHIGH COUNTY JAIL, PA"                      , "PA"   , "LEHIGPA"                , "100"              ,
  "LEXINGTON FED.MED CENTER"                                   , "LEXINTON FEDERAL MEDICAL CENTER"             , "KY"   , "BOPLEX"                 , "85"               ,
  "LOUISIANA ICE PROCESSING"                                   , "LOUISIANA ICE PROCESSING CENTER"             , "LA"   , "LICEPLA"                , "100"              ,
  "LUTHERAN SOC SERV"                                          , "LUTHERAN SOC SERV, LANSIN"                   , "MI"   , "LUTSSMI"                , "97"               ,
  "MACOMB COMT"                                                , "MACOMB CO.MT.CLEMENS,MI."                    , "MI"   , "MACCOMI"                , "87"               ,
  "MADISON CO. JAIL"                                           , "MADISON CO. JAIL, MS."                       , "MS"   , "MADISMS"                , "99"               ,
  "MADISON COUNTY JAIL"                                        , "MADISON CO. JAIL, MS."                       , "MS"   , "MADISMS"                , "77"               ,
  "MAIN - FOLKSTON IPC (D RAY JAMES)"                          , "MAIN FOLKSTON IPC DRJAMES"                   , "GA"   , "JAMESGA"                , "80"               ,
  "MATSU PRETRIAL"                                             , "MAT-SU PRETRIAL"                             , "AK"   , "AKMATSU"                , "88"               ,
  "MCLELLAN COUNTY JAIL"                                       , "MCCLELLAN COUNTY JAIL"                       , "TX"   , "MCCLETX"                , "96"               ,
  "MDC Brooklyn"                                               , "BROOKLYN MDC"                                , "NY"   , "BOPBRO"                 , "83"               ,
  "MEADE CO. JAIL"                                             , "MEADE CO. JAIL, SD"                          , "SD"   , "MEADESD"                , "100"              ,
  "MEMPHISSHELBY COUNTY JUV"                                   , "MEMPHIS/SHELBY COUNTY JUV"                   , "TN"   , "MEMPHTN"                , "98"               ,
  "MERCER CO. JAIL"                                            , "MERCER CO. JAIL, PA"                         , "PA"   , "MERCEPA"                , "100"              ,
  "MONROE CO."                                                 , "MONROE CO., MONROE, MI."                     , "MI"   , "MONROMI"                , "94"               ,
  "MONTGOMERY CONTY JAIL"                                      , "MONTGOMERY CNTY JAIL, PA"                    , "PA"   , "MONTGPA"                , "99"               ,
  "MONTGOMERY ICE PROCESSING CENTER"                           , "MONTGOMERY PROCESSING CTR"                   , "TX"   , "MTGPCTX"                , "84"               ,
  "MOUNTRAIL CO. JAIL"                                         , "MOUNTRAIL CO. JAIL, ND"                      , "ND"   , "MOUNTND"                , "100"              ,
  "Madison County Jail"                                        , "MADISON CO. JAIL, MS."                       , "MS"   , "MADISMS"                , "77"               ,
  "Mahoning County Justice Center"                             , "MAHONING COUNTY JAIL"                        , "OH"   , "MAHONOH"                , "83"               ,
  "Monroe County Jail"                                         , "MONROE COUNTY SHERIFF/JAIL-MAIN"             , "MI"   , "MNROEMI"                , "99"               ,
  "Montgomery ICE Processing Center"                           , "MONTGOMERY PROCESSING CTR"                   , "TX"   , "MTGPCTX"                , "84"               ,
  "Montgomery Ice Processing Center"                           , "MONTGOMERY PROCESSING CTR"                   , "TX"   , "MTGPCTX"                , "84"               ,
  "Montgomery Processing Center"                               , "MONTGOMERY PROCESSING CTR"                   , "TX"   , "MTGPCTX"                , "97"               ,
  "NELSON COLEMAN CORRECTION"                                  , "NELSON COLEMAN CORRECTIONS CENTER"           , "LA"   , "NLSCOLA"                , "95"               ,
  "NH STATE PRISON"                                            , "NH STATE PRISON, CONCORD"                    , "NH"   , "NHSPCON"                , "91"               ,
  "NORTH LAKE CORRECTIONAL F"                                  , "NORTH LAKE CORRECTIONAL FACILITY"            , "MI"   , "NRLKCMI"                , "100"              ,
  "NORTHWEST ICE PROCESSING CENTER"                            , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "89"               ,
  "NORTHWEST REGIONAL CORRECTIONS CENTER"                      , "NW REGIONAL CORRECTIONS CENTER"              , "MN"   , "NWRCCMN"                , "90"               ,
  "Natrona County Detention Center"                            , "NATRONA COUNTY JAIL"                         , "WY"   , "NATROWY"                , "95"               ,
  "North Lake Processing Center"                               , "NORTH LAKE CORRECTIONAL FACILITY"            , "MI"   , "NRLKCMI"                , "71"               ,
  "Northwest ICE Processing Center (NWIPC)"                    , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "89"               ,
  "OLDHAM COUNTY DETENTION CENTER"                             , "OLDHAM COUNTY JAIL"                          , "KY"   , "OLDHAKY"                , "87"               ,
  "ORANGE COUNTY JAIL (FL)"                                    , "ORANGE COUNTY JAIL"                          , "FL"   , "ORANGFL"                , "100"              ,
  "ORANGE COUNTY JAIL (NY)"                                    , "ORANGE CO JAIL, GOSHEN NY"                   , "NY"   , "ORANGNY"                , "88"               ,
  "OTTAWA CO. GRAND HAVEN"                                     , "OTTAWA COUNTY"                               , "MI"   , "OTTAWMI"                , "66"               ,
  "Oldham County Detention Center"                             , "OLDHAM COUNTY JAIL"                          , "KY"   , "OLDHAKY"                , "87"               ,
  "PANUNKEY REG JAIL"                                          , "PAMUNKEY REG JAIL"                           , "VA"   , "PAMREVA"                , "70"               ,
  "PENNINGTON CO. JAIL"                                        , "PENNINGTON CO. JAIL, MN"                     , "MN"   , "PENNIMN"                , "98"               ,
  "PHELPS COUNTY JAIL (MO)"                                    , "PHELPS COUNTY JAIL"                          , "MO"   , "PHELPMO"                , "100"              ,
  "PHELPS COUNTY JAIL (NE)"                                    , "PHELPS COUNTY JAIL"                          , "NE"   , "PHELPNE"                , "98"               ,
  "PHILLIPS COUNTY JAIL"                                       , "PHILLIPS COUNTY JAIL, MT"                    , "MT"   , "PHILLMT"                , "100"              ,
  "PICKENS COUNTY DETENTION CENTER"                            , "PICKENS COUNTY DET CTR"                      , "AL"   , "PCKNSAL"                , "93"               ,
  "PRINCE EDWARD COUNTY (FARMVILLE)"                           , "FARMVILLE DETENTION CENTER"                  , "VA"   , "FRMVLVA"                , "42"               ,
  "PT COUPEE PAR DETN CNTS"                                    , "PT COUPEE PAR DETEN. CNTS"                   , "LA"   , "PTCOULA"                , "91"               ,
  "PULASKI COUNTY DETENTION CENTER"                            , "PULASKI COUNTY JAIL"                         , "IL"   , "PULASIL"                , "83"               ,
  "Pennington County Jail"                                     , "PENNINGTON COUNTY JAIL SD"                   , "SD"   , "PENNISD"                , "98"               ,
  "Pickens County"                                             , "PICKENS COUNTY DET CTR"                      , "AL"   , "PCKNSAL"                , "79"               ,
  "Port Isabel Service Processing Center"                      , "PORT ISABEL SPC"                             , "TX"   , "PIC"                    , "87"               ,
  "Prince Edward County (Farmville)"                           , "FARMVILLE DETENTION CENTER"                  , "VA"   , "FRMVLVA"                , "42"               ,
  "RAMSEY ADC ANNEX"                                           , "RAMSEY ADC ANNEX, SPM"                       , "MN"   , "RAADCMN"                , "99"               ,
  "RICHWOOD CORRECTIONAL CENTER"                               , "RICHWOOD COR CENTER"                         , "LA"   , "RWCCMLA"                , "81"               ,
  "RIKERS ISLAND QUEENS"                                       , "RIKERS ISLAND, QUEENS, NY"                   , "NY"   , "RIKISNY"                , "100"              ,
  "ROBERT A DEYTON DETENTION"                                  , "ROBERT A DEYTON DETENTION FAC"               , "GA"   , "RADDFGA"                , "99"               ,
  "ROBERT A DEYTON DETENTION FACILITY"                         , "ROBERT A DEYTON DETENTION FAC"               , "GA"   , "RADDFGA"                , "99"               ,
  "ROBERT A. DEYTON DETENTION FACILITY"                        , "ROBERT A DEYTON DETENTION FAC"               , "GA"   , "RADDFGA"                , "99"               ,
  "ROBERTS CO. JAIL"                                           , "ROBERTS CO. JAIL, SD"                        , "SD"   , "ROBERSD"                , "100"              ,
  "ROUNDHOUSE PHI"                                             , "ROUNDHOUSE PHI, PA"                          , "PA"   , "ROUNDPA"                , "98"               ,
  "Richwood Correctional Center"                               , "RICHWOOD COR CENTER"                         , "LA"   , "RWCCMLA"                , "81"               ,
  "Rio Grande Processing Center"                               , "RIO GRANDE DETENTION CENTER"                 , "TX"   , "RGRNDTX"                , "85"               ,
  "Robert A Deyton Detention"                                  , "ROBERT A DEYTON DETENTION FAC"               , "GA"   , "RADDFGA"                , "99"               ,
  "Robert A. Deyton Detention Facility"                        , "ROBERT A DEYTON DETENTION FAC"               , "GA"   , "RADDFGA"                , "99"               ,
  "SAIPAN DEPARTMENT OF CORR"                                  , "SAIPAN DEPARTMENT OF CORRECTIONS"            , "MP"   , "MPSIPAN"                , "100"              ,
  "SAN MATEO JUVENILE HALL"                                    , "SAN MATEO JUVENILLE HALL"                    , "CA"   , "SMJUVCA"                , "99"               ,
  "SANTA CLARA CO MAIN JAIJ"                                   , "SANTA CLARA COUNTY MAIN JAIL"                , "CA"   , "SCNORCA"                , "77"               ,
  "SOUTH LOUISIANA ICE PROCESSING CENTER"                      , "SOUTH LOUISIANA ICE PROC CTR"                , "LA"   , "BASILLA"                , "96"               ,
  "SOUTH TEXAS FAMILY RESIDENTIAL CENTER1"                     , "SOUTH TEXAS FAM RESIDENTIAL CENTER"          , "TX"   , "STFRCTX"                , "99"               ,
  "SOUTHWEST KEY JUV -SAN JOSE"                                , "SOUTHWEST KEY JUV -SANJOSE"                  , "CA"   , "SWKSJCA"                , "97"               ,
  "SOUTHWEST KEYS JUV. FAC."                                   , "SOUTHWEST KEY PROGRAM (JUV)"                 , "TX"   , "SWDALTX"                , "89"               ,
  "ST CLAIR CO."                                               , "ST.CLAIR CO.,PT.HURON,MI."                   , "MI"   , "STCLAMI"                , "90"               ,
  "ST JOSEPH CO"                                               , "ST JOSEPH CO,CENTREVILLE,"                   , "MI"   , "STJOSMI"                , "88"               ,
  "STUTSMAN CO. JAIL"                                          , "STUTSMAN CO. JAIL, ND"                       , "ND"   , "STUTSND"                , "100"              ,
  "SUITES ON SCOTTSDALE-CASA DE ALEGRÍA"                       , "STES ON SCOTTSDALE CASA DE ALEGRIA"          , "AZ"   , "ALESSAZ"                , "87"               ,
  "Saipan Department Of Corrections (Suspe)"                   , "SAIPAN DEPARTMENT OF CORRECTIONS"            , "MP"   , "MPSIPAN"                , "99"               ,
  "Sherburne County Jail Services"                             , "SHERBURNE COUNTY JAIL"                       , "MN"   , "SHERBMN"                , "99"               ,
  "South Louisiana ICE Processing Center"                      , "SOUTH LOUISIANA ICE PROC CTR"                , "LA"   , "BASILLA"                , "96"               ,
  "Ste. Genevieve County Detention Center"                     , "STE. GENEVIEVE COUNTY SHERIFF/JAIL"          , "MO"   , "STGENMO"                , "93"               ,
  "Sweetwater County Detention Center"                         , "SWEETWATER COUNTY JAIL"                      , "WY"   , "SWEETWY"                , "98"               ,
  "TACOMA ICE PROCESSING CENTER (NORTHWEST DET CTR)"           , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "53"               ,
  "TAYLOR PD"                                                  , "TAYLOR PD,TAYLOR, MI"                        , "MI"   , "TAYPDMI"                , "82"               ,
  "TDC-WACO-MCLENN"                                            , "MCCLENNAN COUNTY DETENTION CENTER"           , "TX"   , "MCCDCTX"                , "39"               ,
  "TORRANCE/ESTANCIA"                                          , "TORRANCE/ESTANCIA, NM"                       , "NM"   , "TOORANM"                , "100"              ,
  "Tacoma ICE Processing Center (Northwest DET CTR)"           , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "53"               ,
  "Tacoma ICE Processing Center (Northwest Det Ctr)"           , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "53"               ,
  "Tacoma Ice Processing Center (northwest Det Ctr)"           , "NW ICE PROCESSING CTR"                       , "WA"   , "CSCNWWA"                , "53"               ,
  "US MARSHALS MID.DIST"                                       , "US MARSHALS,MID.DIST.FL"                     , "FL"   , "USMMDFL"                , "100"              ,
  "US MARSHALS WEST.DIST"                                      , "US MARSHALS,WEST.DIST.,TX"                   , "TX"   , "USMW1TX"                , "98"               ,
  "VA. PENINSULA REG JAIL"                                     , "VIRGINIA PENINSULA REGIONAL JAIL"            , "VA"   , "VPREGVA"                , "84"               ,
  "VAN BUREN COUNTY JAIL"                                      , "VAN BUREN COUNTY JAIL, MI"                   , "MI"   , "VANBUMI"                , "99"               ,
  "WASHINGTON CO. JAIL"                                        , "WASHINGTON CO. JAIL, PA"                     , "PA"   , "WASHIPA"                , "99"               ,
  "West Tennessee Detention Facility"                          , "WESTERN TENNESSEE DET. FAC."                 , "TN"   , "TNWESDF"                , "53"               ,
  "Wyatt Detention Facility"                                   , "WYATT DETENTION CENTER"                      , "RI"   , "WYATTRI"                , "90"               ,

  # found via manual review

  "SUFFOLK HDC SBAY"                                           , "SUFFOLK HOC SBAY"                            , "MA"   , "SUFFOMA"                , NA                 ,
  # "JUVENILE FACILITY"                                                         , ""                                            , "IL"   , ""                       , NA                 ,
  "WAYNE COUNTY"                                               , "WAYNE COUNTY, DETROIT"                       , "MI"   , "WAYNEMI"                , NA                 ,
  # "GUAYNABO ADC (SAN JUAN)"                                    , "GUAYNABO MDC (SAN JUAN)"                     , "PR"   , "BOPGUA"                 , NA                 , # this info is from Vera not ICE
  # "CCA CHER-TAZ DET.CTR."                                                     , ""                                            , "AZ"   , ""                       , NA                 ,
  # "AIRPORT DDP"                                                               , ""                                            , "PR"   , ""                       , NA                 ,
  # "U.S IMMIGRATION"                                                           , ""                                            , "MI"   , ""                       , NA                 ,
  # "JOHNSON COUNTY DETENTION CENTER"                                           , ""                                            , "NC"   , ""                       , NA                 ,
  "US MARSHALS"                                                , "US MARSHALS, OREGON  OR"                     , "OR"   , "USMOR"                  , NA                 ,
  # "CENTE"                                                                     , ""                                            , "WA"   , ""                       , NA                 ,
  "CONCORDIA PARISH C O"                                       , "CONCORDIA PARISH C C"                        , "LA"   , "CONCOLA"                , NA                 ,
  "RIVERIDGE HOSPITAL"                                         , "RIVEREDGE HOSPITAL"                          , "IL"   , "REHOSIL"                , NA                 ,
  # "MARSHFIELD E. CENTER"                                                      , ""                                            , "TX"   , ""                       , NA                 ,
  "US MARSHAL S"                                               , "US MARSHALS,MARYLAND"                        , "MD"   , "USMMD"                  , NA                 ,
  "Baker County Detention Center"                              , "BAKER COUNTY SHERIFF DEPT."                  , "FL"   , "BAKERFL"                , NA                 ,
  "Dilley Immigration Processing Center"                       , "TRUSTED ADULT SOUTH TEX DILLEY FSC"          , "TX"   , "TASTDTX"                , NA                 ,
  # "DOW Detention Facility at Fort Bliss"                                      , ""                                            , "TX"   , ""                       , NA                 ,
  "FCI Atlanta"                                                , "ATLANTA US PEN"                              , "GA"   , "BOPATL"                 , NA                 ,
  "FCI Leavenworth"                                            , "LEAVENWORTH USP"                             , "KS"   , "BOPLVN"                 , NA                 ,
  "Federal Correctional Institution - Berlin, NH"              , "FCI BERLIN"                                  , "NH"   , "BOPBER"                 , NA                 ,
  "Miami Correctional Facility (MCF)"                          , "MIAMI CORRECTIONAL CENTER"                   , "IN"   , "INMIAMI"                , NA                 ,
  # "Naval Station Guantanamo Bay (JTF Camp Six and Migrant Ops Center Main A)" , ""                                            , "GU"   , ""                       , NA                 ,
  "Ozark County Jail"                                          , "OZARK COUNTY SHERIFF'S OFFICE"               , "MO"   , "OZARKMO"                , NA                 ,
  # "CBP SAN YSIDRO POE"                                                        , ""                                            , "CA"   , ""                       , NA                 ,
  # "CTR FAM SVS JUNTOS PRF"                                                    , ""                                            , "NJ"   , ""                       , NA                 ,
  # "BEST WESTERN PLUS EL PASO AIRPORT HOTEL & CONFEREN"                        , ""                                            , "TX"   , ""                       , NA                 ,
  # "SUPER  BY WYNDHAM"                                                         , ""                                            , "TX"   , ""                       , NA                 ,
  # "PHARR POLICE DEPT"                                                         , ""                                            , "TX"   , ""                       , NA                 ,
  # "OMDC ENV USBP OFO TRNSPT"                                                  , ""                                            , "CA"   , ""                       , NA                 ,
  # "TIMBER RIDGE SCHOOL"                                                       , ""                                            , "VA"   , ""                       , NA                 ,
  # "JTF CAMP SIX"                                                              , ""                                            , "FL"   , ""                       , NA                 ,
  # "MIGRANT OPS CENTER MAIN A"                                  , ""               ,  , ""                , NA                 ,
  "DILLEY IMMIGRATION PROCESSING CENTER"                       , "TRUSTED ADULT SOUTH TEX DILLEY FSC"          , "TX"   , "TASTDTX"                , NA                 ,
  # "WICHITA COUNTY JAIL"                                                       , ""                                            , "TX"   , ""                       , NA                 ,
  # "DOD DETENTION FACILITY AT FORT BLISS"                                      , ""                                            , "TX"   , ""                       , NA                 ,
  "FCI ATLANTA"                                                , "ATLANTA US PEN"                              , "GA"   , "BOPATL"                 , NA                 ,
)

fuzzy_matches_manual <-
  fuzzy_matches_manual |>
  mutate(source = "ddp-manual", date = as.Date("2026-01-01")) |>
  left_join(
    name_state_match |>
      select(name, state, date_state = date, source_state = source) |>
      distinct(name, state, .keep_all = TRUE), # deals with the small number of cases that have multiple different codes attached to them (dealt with at later step)
    by = c("name_matched" = "name", "state")
  )

exact_non_matches_no_manual <-
  exact_non_matches |>
  anti_join(
    fuzzy_matches_manual |>
      transmute(name_join = clean_text(name.x), state),
    by = c("name_join", "state")
  )

matches <-
  bind_rows(
    exact_matches,
    fuzzy_matches_manual |>
      select(name = name.x, state, detention_facility_code, date, source)
  ) |>
  group_by(detention_facility_code, name, state) |>
  slice_max(date, n = 1, with_ties = FALSE) |>
  ungroup()

facility_attributes_matched <-
  facility_attributes |>
  distinct(name, state) |>
  # select(-detention_facility_code, -date, -source) |>
  inner_join(
    matches,
    by = c("name", "state")
  ) |>
  select(detention_facility_code, name, state, date, source) |>
  group_by(detention_facility_code, name, state) |>
  slice_max(date, n = 1, with_ties = FALSE) |>
  ungroup()

code_name_state_match <-
  bind_rows(
    facility_code,
    facility_attributes_matched
  ) |>
  select(-state) |>
  left_join(
    name_state_match |>
      group_by(detention_facility_code, state) |>
      slice_max(date, n = 1, with_ties = FALSE) |>
      ungroup() |>
      select(
        detention_facility_code,
        state,
        date_state = date,
        source_state = source
      ),
    by = "detention_facility_code"
  ) |>
  group_by(detention_facility_code, name, state) |>
  slice_max(date, n = 1, with_ties = FALSE) |>
  ungroup() |>
  rename(date_facility_code = date, source_facility_code = source)

arrow::write_feather(
  code_name_state_match,
  "data/facilities-name-code-match.feather"
)
