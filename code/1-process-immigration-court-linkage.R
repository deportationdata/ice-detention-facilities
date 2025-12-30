library(tidyverse)
library(xml2)
library(rvest)

# read the MHT / HTML file
# copy html table directly from site in Dia, paste into TextEdit, save as .html
html <- read_html("~/downloads/tmp2.html")
# html <- read_html(url("https://www.justice.gov/eoir/immigration-court-administrative-control-list"))

raw_tbl <-
  html |>
  html_element("table") |>
  html_table(fill = TRUE) |>
  as_tibble() |>
  setNames(c(
    "admin_control_court",
    "assigned_responsibility",
    "other_hearing_location"
  )) |>
  slice(-1)

raw_tbl |>
  separate(
    assigned_responsibility,
    into = str_c("facility_", 1:10),
    sep = "[\\r\\n]+"
  ) |>
  separate(
    other_hearing_location,
    into = str_c("other_facility_", 1:10),
    sep = "[\\r\\n]+"
  ) |>
  mutate(across(
    starts_with("facility_") | starts_with("other_facility_"),
    str_squish
  )) |>
  pivot_longer(
    starts_with("facility_") | starts_with("other_facility_"),
    names_to = "category",
    values_to = "value"
  ) |>
  mutate(
    category = case_when(
      str_starts(category, "facility_") ~ "Assigned responsibility",
      str_starts(category, "other_facility_") ~ "Other hearing location"
    ),
    admin_control_court = case_when(
      str_detect(
        admin_control_court,
        "Greenspoint"
      ) ~ "Houston - Greenspoint Park Immigration Court",
      str_starts(
        admin_control_court,
        "New York - Varick"
      ) ~ "New York - Varick Immigration Court",
      TRUE ~ admin_control_court
    ),
    admin_control_court = str_split_i(
      admin_control_court,
      "Immigration Court|Service Processing Center|Detention Center",
      i = 1
    ) |>
      str_squish()
  ) |>
  filter(value != "" & !is.na(value) & !is.null(value)) |>
  filter(!str_starts(value, "NOTE: ")) |>
  filter(
    !(str_detect(str_to_upper(value), "DHS") &
      str_detect(str_to_upper(value), "OFFICE"))
  ) |>
  mutate(
    category = factor(
      category,
      levels = c("Assigned responsibility", "Other hearing location"),
      labels = c("Assigned", "Other")
    )
  ) |>
  transmute(
    detention_facility_code = "",
    name = value,
    immigration_court = admin_control_court,
    category
  ) |>
  clipr::write_clip()

source("code/functions.R")

immigration_court_df <- read_delim(
  "inputs/immigration-court-administrative-control-list.csv"
) |>
  mutate(
    name_join = name |> clean_text()
  )

name_city_state_match <-
  arrow::read_feather(
    "data/facilities-name-state-match.feather"
  )

# name_code_matches <-
#   fuzzylink::fuzzylink(
#     immigration_court_df |> distinct(name, name_join),
#     name_city_state_match |> mutate(name_join = clean_text(name)),
#     record_type = "name of detention facility",
#     model = "gpt-3.5-turbo-instruct",
#     learner = "ranger",
#     instructions = "compare facility names to find possible matches; some are hospitals or medical centers and some are detention facilities",
#     by = "name_join",
#     max_labels = 50000,
#     openai_api_key = "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"
#   )

name_code_matches |> write_rds("~/Downloads/matches.rds")

name_code_matches |>
  filter(match == "Yes") |>
  select(
    name.x,
    detention_facility_code,
    name_matched = name.y,
    match,
    match_probability
  ) |>
  group_by(name.x, detention_facility_code) |>
  slice_max(order_by = match_probability, n = 1, with_ties = FALSE) |>
  ungroup() |>
  filter(match_probability >= 0.1) |>
  mutate(
    match_probability = round(match_probability * 100)
  ) |>
  select(-match) |>
  relocate(
    name.x,
    name_matched,
    detention_facility_code,
    match_probability
  ) |>
  clipr::write_clip()

matches_manual <- tribble(
  ~name.x                                                                         , ~name_matched                        , ~detention_facility_code , ~match_probability ,
  "AZ Department of Corrections (Tucson)"                                         , "AZ DPT CORRECTIONS TUCSON"          , "AZDCTUC"                , "96"               ,
  "Abraxas Academy-DHS Juvenile Detainees"                                        , "ABRAXAS ACADEMY DETENTION CENTER"   , "ABRXSPA"                , "94"               ,
  # "Adams County"                                                                                 , "ADAMS COUNTY JAIL"                  , "ADAMSCO"                , "82"               ,
  # "Adams County"                                                                                 , "ADAMS CO JAIL, NEBRASKA"            , "ADAMSNE"                , "66"               ,
  # "Adams County"                                                                                 , "ADAMS COUNTY"                       , "ADAMSWA"                , "100"              ,
  # "Adams County Correctional Facility"                                                           , "ADAMS COUNTY CORRECTIONAL CENTER"   , "ADAMSMS"                , "97"               ,
  # "Adams County Correctional Facility"                                                           , "ADAMS CO JAIL, NEBRASKA"            , "ADAMSNE"                , "82"               ,
  # "Adelanto Desert View"                                                                         , "DESERT VIEW ANNEX"                  , "CADESVI"                , "54"               , # this is wrong code
  "Adelanto Detention Facilities (East)"                                          , "ADELANTO ICE PROCESSING CENTER"     , "ADLNTCA"                , "96"               ,
  "Adelanto Detention Facilities (West)"                                          , "ADELANTO ICE PROCESSING CENTER"     , "ADLNTCA"                , "97"               ,
  "Adelanto Detention Facilities, West"                                           , "ADELANTO ICE PROCESSING CENTER"     , "ADLNTCA"                , "97"               ,
  # "Airway Heights Correctional Center"                                                           , "NORTHWEST STATE CORRECTIONAL CTR."  , "VTSTALB"                , "40"               ,
  # "Alaska Department of Corrections"                                                             , "ANCHORAGE JAIL"                     , "ANCHOAK"                , "46"               ,
  # "Allegheny County Jail"                                                                        , "ALLEGANY COUNTY JAIL"               , "ALLEGNY"                , "71"               ,
  # "Allegheny County Jail"                                                                        , "ALLEGHENY CO. JAIL"                 , "ALLEGPA"                , "96"               ,
  "Allen Parish"                                                                  , "ALLEN PARISH PUBLIC SAFETY COMPLEX" , "APPSCLA"                , "84"               ,
  # "Allenwood Correctional Institution, Whitedeer, PA"                                            , "ALLENWOOD (MED) FCI"                , "BOPALM"                 , "38"               ,
  "Arizona DOC, State Prison Complex- Perryville, Goodyear, AZ (IHP)"             , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "68"               ,
  # "Arizona Department of Corrections (AZDOC):"                                                   , "AZ DPT CORRECTIONS TUCSON"          , "AZDCTUC"                , "77"               ,
  # "Arizona Department of Corrections (AZDOC):"                                                   , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "85"               ,
  # "Arizona Department of Corrections (AZDOC):"                                                   , "ARIZONA STATE PRISON - YUMA"        , "AZSVCPC"                , "58"               ,
  # "Arizona Department of Corrections (AZDOC):"                                                   , "DEPARTMENT OF CORRECTIONS"          , "DEPCOGU"                , "69"               ,
  # "Arizona Department of Corrections (AZDOC):"                                                   , "IN DEPT. OF CORRECTIONS"            , "INDCMIC"                , "52"               ,
  # "Arizona Department of Corrections (Florence)"                                                 , "CCA, FLORENCE CORRECTIONAL CENTER"  , "CCAFLAZ"                , "57"               ,
  "Arizona Department of Corrections (Florence)"                                  , "DEPARTMENT OF CORRECTIONS"          , "DEPCOGU"                , "54"               ,
  "Arizona Department of Corrections (Perryville, Goodyear, AZ) (IHP)"            , "PERRYVILLE STATE PRISON"            , "AZSPPER"                , "51"               ,
  # "Arizona Department of Corrections (Phoenix West) (IHP)"                                       , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "76"               ,
  "Arizona State Prison-Phoenix West, Phoenix, AZ (IHP)"                          , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "73"               ,
  # "Baker Correctional Institution"                                                               , "BAKER CCF"                          , "CACFBAK"                , "78"               ,
  # "Baker Correctional Institution"                                                               , "BAKER C. I."                        , "FLBAKCI"                , "75"               ,
  # "Baker County Detention Center"                                                                , "BAKER COUNTY SHERIFF DEPT."         , "BAKERFL"                , "91"               ,
  # "Baker County Detention Center"                                                                , "BAKER CCF"                          , "CACFBAK"                , "63"               ,
  # "Baker County Detention Center"                                                                , "BAKER C. I."                        , "FLBAKCI"                , "73"               ,
  "Baker County Detention Center, Macclenny, FL"                                  , "BAKER COUNTY SHERIFF DEPT."         , "BAKERFL"                , "89"               ,
  "Beaver County Jail"                                                            , "BEAVER COUNTY JAIL"                 , "BEAVRPA"                , "100"              ,
  "Berks County Detention Center, Leesport, PA"                                   , "BERKS COUNTY JAIL, PA"              , "BERKSPA"                , "92"               ,
  # "Bloomington Detained - Lawrence County"                                                       , "LAWRENCE COUNTY JAIL"               , "LAWREPA"                , "61"               ,
  "Bluebonnet Detention Center"                                                   , "BLUEBONNET DET FCLTY"               , "BLBNATX"                , "64"               ,
  "Boone County Detention Center"                                                 , "BOONE COUNTY JAIL"                  , "BOONEKY"                , "99"               ,
  "Bossier"                                                                       , "BOSSIER PARISH COR. CENTER"         , "BOSSRLA"                , "52"               ,
  "Box Elder County Jail"                                                         , "BOX ELDER CO. JAIL"                 , "BOXELUT"                , "96"               ,
  "Broome County Jail, Binghamton, NY"                                            , "BROOME COUNTY JAIL"                 , "BROMMNY"                , "99"               ,
  "Broward Correctional Institute, Pembroke Pines, FL"                            , "BROWARD CORRECTIONAL"               , "BROCRFL"                , "89"               ,
  "Broward Transitional Center (BTC)"                                             , "BROWARD TRANSITIONAL CENTER"        , "WCCPBFL"                , "99"               ,
  "Burleigh County Jail"                                                          , "BURLEIGH CO. JAIL, ND"              , "BURLEND"                , "92"               ,
  "Butler County Jail, El Dorado, KS"                                             , "BUTLER COUNTY JAIL"                 , "BUTLEKS"                , "91"               ,
  "Butler County Jail, Hamilton, Ohio (CLE-BTD)"                                  , "BUTLER COUNTY JAIL"                 , "BUTLEOH"                , "87"               ,
  "Caldwell County Detention Center, Kingston, MO"                                , "CALDWELL COUNTY JAIL"               , "CALDWMO"                , "81"               ,
  "Calhoun Co. Sheriff's Department, Battle Creek, MI"                            , "CALHOMI CALHOUN CO., BATTLE CR,MI"  , "CALHOMI"                , "67"               ,
  # "California Correctional Center"                                                               , "CALIFORNIA STATE PRISON"            , "CASPSAT"                , "80"               ,
  # "Campbell County Detention Center"                                                             , "CAMPBELL COUNTY JAIL"               , "CAMCOWY"                , "98"               ,
  # "Campbell County Detention Center"                                                             , "CAMPBELL CO DET CTR"                , "CPBDCKY"                , "63"               ,
  "Caroline County Detention Facility Bowling Green, VA"                          , "CAROLINE DETENTION FACILITY"        , "CARDFVA"                , "89"               ,
  # "Central Arizona Correctional Facility (CACF) - Florence, AZ"                                  , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "46"               ,
  # "Central Arizona Correctional Facility (CACF) - Florence, AZ"                                  , "CCA, FLORENCE CORRECTIONAL CENTER"  , "CCAFLAZ"                , "44"               ,
  "Charlotte Correctional Inst., Punta Gorda, FL"                                 , "CHARLOTTE CI, PUNTA GORDA"          , "FLCHACI"                , "78"               ,
  "Chase County Jail, Cotton Falls, KS"                                           , "CHASE COUNTY JAIL"                  , "CHASEKS"                , "99"               ,
  "Chippewa County Jail, Sault Ste. Marie, MI"                                    , "CHIPPEWA CO., SSM"                  , "CHIPPMI"                , "69"               ,
  "Christian County Detention Center, Strafford, MO"                              , "CHRISTIAN CO SHERIFF DEPT"          , "CHRISMO"                , "73"               ,
  # "Christian County Jail"                                                                        , "CHRISTIAN COUNTY JAIL"              , "CHRISKY"                , "100"              ,
  # "Christian County Jail"                                                                        , "CHRISTIAN CO SHERIFF DEPT"          , "CHRISMO"                , "85"               ,
  "Cibola County Correctional Center, NM"                                         , "CIBOLA COUNTY CORRECTIONAL CENTER"  , "CIBOCNM"                , "99"               ,
  "Cimarron Detention Facility, Cushing, OK"                                      , "CIMARRON CORRECTIONAL FACILITY"     , "OKCIMMC"                , "95"               ,
  # "Clark County Jail"                                                                            , "CLARK COUNTY JAIL"                  , "CLARKID"                , "100"              ,
  # "Clark County Jail"                                                                            , "CLARK COUNTY JAIL"                  , "CLARKIN"                , "100"              ,
  # "Clark County Jail"                                                                            , "CLARK COUNTY JAIL"                  , "CLARKWA"                , "100"              ,
  # "Clark County Jail"                                                                            , "LAS VEGAS CITY JAIL"                , "LVGCINV"                , "41"               ,
  # "Clay County Jail"                                                                             , "CLAY COUNTY JAIL"                   , "CLACOMO"                , "100"              ,
  # "Clay County Jail"                                                                             , "CLAY COUNTY JAIL"                   , "CLACOSD"                , "100"              ,
  # "Clay County Jail"                                                                             , "CLAY COUNTY JAIL"                   , "CLAYCFL"                , "100"              ,
  # "Clay County Jail"                                                                             , "CLAY CO JAIL"                       , "CLAYJIA"                , "78"               ,
  # "Clinton County Detention Center"                                                              , "CLINTON COUNTY JAIL"                , "CLICONY"                , "98"               ,
  # "Clinton County Detention Center"                                                              , "CLINTON COUNTY JAIL"                , "CLINTIA"                , "98"               ,
  # "Clinton County Detention Center"                                                              , "CLINTON COUNTY JAIL"                , "CLINTIN"                , "98"               ,
  # "Clinton County Detention Center"                                                              , "CLINTON COUNTY JAIL"                , "CLINTMO"                , "98"               ,
  "Clinton County, IN"                                                            , "CLINTON COUNTY JAIL"                , "CLINTIN"                , "83"               ,
  "Coastal Bend Detention Center"                                                 , "COASTAL BEND DET. FACILITY"         , "CBENDTX"                , "94"               ,
  "Colorado Department of Corrections"                                            , "COLO DEPT OF CORRECTIONS"           , "CODCCSP"                , "59"               ,
  # "Commonwealth of the Northern Mariana Islands Dept. of Corrections"                            , "SAIPAN DEPARTMENT OF CORRECTIONS"   , "MPSIPAN"                , "79"               ,
  "Corrections Center of Northwest Ohio (CCNO)"                                   , "CORRECTIONS CENTER OF NW OHIO"      , "OHNORWE"                , "96"               ,
  "Cumberland County Jail, ME"                                                    , "CUMBERLAND COUNTY JAIL"             , "CUMBEME"                , "99"               ,
  "D. Ray James Detention Facility"                                               , "D. RAY JAMES PRISON"                , "GADRYJM"                , "99"               ,
  # "DHS Detention Facility"                                                                       , "DENVER CONTRACT DETENTION FACILITY" , "DENICDF"                , "60"               ,
  # "DHS Detention Facility"                                                                       , "EAST HIDALGO DETENTION CENTER"      , "EHDLGTX"                , "39"               ,
  # "DHS Detention Facility"                                                                       , "FACILITY 8 DETENTION FACILITY"      , "FCLT8CA"                , "40"               ,
  # "David L. Moss Correctional Center, Tulsa, OK"                                                 , "TULSA COUNTY JAIL"                  , "TULCOOK"                , "45"               ,
  "Daviess County Detention Center"                                               , "DAVIESS COUNTY DETENTION CENTER"    , "DVCSDKY"                , "100"              ,
  # "Davis County Jail"                                                                            , "DAVIS CO. JAIL"                     , "DAVISUT"                , "80"               ,
  "Delaney Hall"                                                                  , "DELANEY HALL DETENTION FACILITY"    , "DHDFNJ"                 , "95"               ,
  # "Dodge County Detention Facility"                                                              , "DODGE COUNTY JAIL"                  , "DODGENE"                , "97"               ,
  # "Dodge County Detention Facility"                                                              , "DODGE COUNTY JAIL, JUNEAU"          , "DODGEWI"                , "94"               ,
  "Douglas County Jail, Superior, WI"                                             , "DOUGLAS CO. WISCONSIN"              , "DOUGLWI"                , "66"               ,
  "East Hildago Detention Facility"                                               , "EAST HIDALGO DETENTION CENTER"      , "EHDLGTX"                , "86"               ,
  # "Eden Detention Center"                                                                        , "EDEN DET CENTER"                    , "EDENDTX"                , "53"               ,
  # "Eden Detention Center"                                                                        , "EDEN DETENTION CTR"                 , "EDNDCTX"                , "99"               ,
  # "El Paso Sector Centralized Processing Center"                                                 , "EL PASO SPC"                        , "EPC"                    , "65"               ,
  # "El Paso Sector Centralized Processing Center"                                                 , "EL PASO SOFT SIDED FACILITY"        , "EPSSFTX"                , "46"               ,
  "El Paso Service Processing Center"                                             , "EL PASO SPC"                        , "EPC"                    , "82"               ,
  "El Valle Detention Center"                                                     , "EL VALLE DETENTION FACILITY"        , "ELVDFTX"                , "96"               ,
  "El Valle Detention Facility — Detained CFR"                                  , "EL VALLE DETENTION FACILITY"        , "ELVDFTX"                , "96"               ,
  # "Erie County Detention Facility"                                                               , "ERIE COUNTY CORRECTIONAL"           , "EIRCONY"                , "95"               ,
  # "Erie County Detention Facility"                                                               , "ERIE CO HOLDING CENTER"             , "EIRHONY"                , "44"               ,
  # "Erie County Detention Facility"                                                               , "ERIE COUNTY HOLDING CENTER"         , "ERICONY"                , "91"               ,
  # "Erie County Detention Facility"                                                               , "ERIE COUNTY JAIL, PA"               , "ERICOPA"                , "94"               ,
  "Etowah County Sheriff's Office Detention Facility"                             , "ETOWAH COUNTY JAIL (AL)"            , "ETOWAAL"                , "96"               ,
  # "Etowah Detention Facility Gadson, Alabama"                                                    , "ETOWAH COUNTY JAIL (AL)"            , "ETOWAAL"                , "86"               ,
  "Eyman — Arizona State Prison Complex — Florence, AZ"                       , "ARIZONA STATE PRISON COMP"          , "AZSPFLO"                , "47"               ,
  # "FCO Oakdale IHP"                                                                              , "OAKDALE FED.DET.CENTER"             , "BOPOAD"                 , "56"               ,
  "Farmville Detention Center, Farmville, VA"                                     , "FARMVILLE DETENTION CENTER"         , "FRMVLVA"                , "98"               ,
  "Federal Detention Center - Philadelphia"                                       , "PHILADELPHIA DETENTION CT"          , "PHIDEPA"                , "80"               ,
  "Federal Detention Center-Oakdale 2, Oakdale LA"                                , "OAKDALE FED.DET.CENTER"             , "BOPOAD"                 , "81"               ,
  # "Florence Detention Center (DHS Detainees)"                                                    , "CCA, FLORENCE CORRECTIONAL CENTER"  , "CCAFLAZ"                , "46"               ,
  # "Florence Detention Center (DHS Detainees)"                                                    , "FLORENCE SPC"                       , "FLO"                    , "46"               ,
  # "Florence West Correction and Rehabilitation Facility — Florence, AZ"                        , "CCA, FLORENCE CORRECTIONAL CENTER"  , "CCAFLAZ"                , "64"               ,
  "Folkston ICE Detention Center (Annex)"                                         , "FOLKSTON ANNEX IPC"                 , "FIPCAGA"                , "90"               ,
  "Folkston ICE Detention Center (Main)"                                          , "FOLKSTON MAIN IPC"                  , "FIPCMGA"                , "60"               ,
  # "Franklin County House of Correction 160 Elm Street, Greenfield, MA 01301 (for ICE Detainees)" , "FRANKLIN HOUSE OF CORRECTIONS"      , "GREENMA"                , "75"               ,
  # "Franklin County House of Correction 160 Elm Street, Greenfield, MA 01301 (for ICE Detainees)" , "FRANKLIN HOUSE OF CORRECTIONS"      , "MAFRKHC"                , "75"               ,
  "GRAND FORKS COUNTY CORRECTIONAL"                                               , "GRAND FORKS COUNTY CORREC"          , "GFCORND"                , "95"               ,
  "Geauga County Sheriff's Office, Chardon, OH (CLE-GCD)"                         , "GEAUGA COUNTY JAIL (GEAUGOH)"       , "GEAUGOH"                , "87"               ,
  "Glades County Prison (GCP) Glades County Corrections"                          , "GLADES COUNTY DETENTION CENTER"     , "GLADEFL"                , "91"               ,
  "Golden State Annex Correctional Facility, McFarland, CA"                       , "GOLDEN STATE ANNEX"                 , "GLDSACA"                , "68"               ,
  "Grayson County Detention Facility"                                             , "GRAYSON COUNTY DETENTION CENTER"    , "GRAYSKY"                , "98"               ,
  "Henderson Detention Center (Henderson, NV)"                                    , "HENDERSON DETENTION"                , "HENDENV"                , "96"               ,
  "Hillsborough County Department of Corrections — Manchester, NH"              , "HILLSBOROUGH COUNTY JAIL"           , "HILLSNH"                , "92"               ,
  "Honolulu Federal Detention Center"                                             , "HONOLULU FEDERAL DETENTION CENTER"  , "BOPHON"                 , "100"              ,
  "Hopkins County Jail, Madisonville, KY"                                         , "HOPKINS COUNTY JAIL"                , "HPKNSKY"                , "98"               ,
  "Houston Contract Detention Facility"                                           , "HOUSTON CONTRACT DET.FAC."          , "HOUICDF"                , "81"               ,
  # "Houston Service Processing Center"                                                      , "HOUSTON CONTRACT DET.FAC."          , "HOUICDF"                , "59"               ,
  # "Houston Service Processing Center"                                                      , "SOUTH TEXAS ICE PROCESSING CENTER"  , "STCDFTX"                , "52"               ,
  "Huntsville Department of Corrections"                                          , "HUNTSVILLE STATE P."                , "TXHUNTS"                , "56"               ,
  # "Illinois Department of Corrections"                                                     , "ROCK ISLAND CORRECTIONAL CENTER"    , "RCKISIL"                , "56"               ,
  # "Immigration CourtHutto Residential Facility, Taylor, TX"                                , "SOUTH TEXAS FAM RESIDENTIAL CENTER" , "STFRCTX"                , "43"               ,
  "Imperial Regional Detention Facility"                                          , "IMPERIAL REGIONAL ADULT DET FAC"    , "IRADFCA"                , "97"               ,
  "Indiana Department of Corrections"                                             , "IN DEPT. OF CORRECTIONS"            , "INDCMIC"                , "45"               ,
  # "Jackson Parish"                                                                         , "JACKSON PARISH CORRECTIONAL CENTER" , "JKPCCLA"                , "93"               ,
  "Jackson Parish Correctional Center, 287 Industrial Drive, Jonesboro, LA 71251" , "JACKSON PARISH CORRECTIONAL CENTER" , "JKPCCLA"                , "89"               ,
  "Joe Corley Detention Center"                                                   , "JOE CORLEY PROCESSING CTR"          , "JCRLYTX"                , "95"               ,
  # "Karnes County Residential Center, Karnes, TX"                                           , "KARNES CTY CORR CTR"                , "KARNETX"                , "91"               ,
  # "Karnes County Residential Center, Karnes, TX"                                           , "KARNES COUNTY CIVIL DET. FACILITY"  , "KCCDCTX"                , "93"               ,
  # "Karnes County Residential Center, Karnes, TX"                                           , "KARNES CO IMMIGRATION PROCESS CTR"  , "KRNRCTX"                , "74"               ,
  # "Karnes County Residential Center, Karnes, TX"                                           , "SOUTH TEXAS FAM RESIDENTIAL CENTER" , "STFRCTX"                , "59"               ,
  "Kay County Detention Center, OK"                                               , "KAY CO JUSTICE FACILITY"            , "KCOJFOK"                , "45"               ,
  "Kenton County Detention Center, KY"                                            , "KENTON CO DETENTION CTR"            , "KENTOKY"                , "97"               ,
  "Krome North Service Processing Center"                                         , "KROME NORTH SPC"                    , "KRO"                    , "87"               ,
  # "La Palma Correctional Center"                                                           , "LA PALMA CORR CTR APSO"             , "LPAPSAZ"                , "58"               ,
  # "La Palma Correctional Center"                                                           , "LA PALMA CORRECTIONAL CENTER"       , "LPLCCAZ"                , "100"              ,
  # "La Palma Correctional Center — Eloy, AZ"                                              , "LA PALMA CORR CTR APSO"             , "LPAPSAZ"                , "66"               ,
  # "La Palma Correctional Center — Eloy, AZ"                                              , "LA PALMA CORRECTIONAL CENTER"       , "LPLCCAZ"                , "91"               ,
  # "LaSalle Correctional Center, Cotulla, TX"                                               , "LASALLE COUNTY JAIL"                , "LASALTX"                , "78"               ,
  # "LaSalle Correctional Center, Cotulla, TX"                                               , "LA SALLE CO REGIONAL DET. CENTER"   , "LSRDCTX"                , "44"               ,
  # "LaSalle Detention Center"                                                               , "LASALLE COUNTY JAIL"                , "LASALTX"                , "82"               ,
  # "LaSalle Detention Center"                                                               , "LASALLE CORR CTR OLLA"              , "OLACCLA"                , "43"               ,
  # "Laredo IHF (MPP)"                                                                       , "LAREDO PROCESSING CENTER"           , "LRDICDF"                , "52"               ,
  "Laredo Processing Center, Laredo, TX"                                          , "LAREDO PROCESSING CENTER"           , "LRDICDF"                , "98"               ,
  "Laredo Rio Grande Detention Center Laredo, TX"                                 , "RIO GRANDE DETENTION CENTER"        , "RGRNDTX"                , "64"               ,
  "Laredo Rio Grande Detention Center Laredo, Texas"                              , "RIO GRANDE DETENTION CENTER"        , "RGRNDTX"                , "79"               ,
  # "Laredo Sector Centralized Processing Center"                                            , "LAREDO PROCESSING CENTER"           , "LRDICDF"                , "85"               ,
  # "Laredo Sector Centralized Processing Center"                                            , "SOUTH TEXAS ICE PROCESSING CENTER"  , "STCDFTX"                , "47"               ,
  # "Laredo Webb County Detention Center Laredo, TX"                                         , "WEBB COUNTY JAIL"                   , "WEBCOTX"                , "72"               ,
  # "Laredo Webb County Detention Center Laredo, TX"                                         , "WEBB COUNTY DETENTION CENTER (CCA)" , "WEBDCTX"                , "61"               ,
  # "Larkin Hospital (sub-facility for mentally incompetent or mentally unstable detainees)" , "LARKIN BEHAVIORAL HEALTH SVCS"      , "LRKBHFL"                , "57"               ,
  # "Larkin Hospital (sub-facility for mentally incompetent or mentally unstable detainees)" , "LARKIN HOSPITAL"                    , "LRKNHFL"                , "82"               ,
  "Limestone Detention Center, Groesbeck, TX"                                     , "LIMESTONE DET CENTER"               , "LIMESTX"                , "78"               ,
  "MDC Guaynabo, PR (Puerto Rico, Metropolitan Detention Center)"                 , "GUAYNABO MDC (SAN JUAN)"            , "BOPGUA"                 , "49"               ,
  "Mahoning County Jail"                                                          , "MAHONING COUNTY JAIL"               , "MAHONOH"                , "100"              ,
  "Marion County Correctional Center, Marion Co., OR"                             , "MARION CO JAIL"                     , "MARICOR"                , "95"               ,
  # "Marion County Correctional Facility"                                           , "MARION CORRECTIONAL INST."          , "FLMARIC"                , "96"               ,
  # "Marion County Correctional Facility"                                           , "MARION CO JAIL"                     , "MARICOR"                , "93"               ,
  # "Marion County Correctional Facility"                                           , "MARION COUNTY JAIL"                 , "MARIOFL"                , "99"               ,
  # "Marion County Correctional Facility"                                           , "MARION COUNTY JAIL"                 , "MARIOIA"                , "99"               ,
  # "Marion County Correctional Facility"                                           , "MARION COUNTY JAIL"                 , "MARIOIN"                , "99"               ,
  # "Marion County Jail"                                                            , "MARION CORRECTIONAL INST."          , "FLMARIC"                , "66"               ,
  # "Marion County Jail"                                                            , "MARION CO JAIL"                     , "MARICOR"                , "95"               ,
  # "Marion County Jail"                                                            , "MARION COUNTY JAIL"                 , "MARIOFL"                , "100"              ,
  # "Marion County Jail"                                                            , "MARION COUNTY JAIL"                 , "MARIOIA"                , "100"              ,
  # "Marion County Jail"                                                            , "MARION COUNTY JAIL"                 , "MARIOIN"                , "100"              ,
  # "Mesa Verde Detention Facility, Bakersfield, CA"                                , "MESA VERDE ICE PROCESSING CENTER"   , "CACFMES"                , "56"               ,
  "Metropolitan Detention Center, Brooklyn, NY"                                   , "BROOKLYN MDC"                       , "BOPBRO"                 , "72"               ,
  "Miami Federal Detention Center"                                                , "MIAMI FED.DET.CENTER"               , "BOPMIM"                 , "96"               ,
  "Minnesota Dept. of Corr., Stillwater, MN"                                      , "MINN.C.F., STILLWATER"              , "MNCFSTW"                , "57"               ,
  "Monroe Co. Sheriff's Dept., Monroe, MI"                                        , "MONROE COUNTY SHERIFF/JAIL-MAIN"    , "MNROEMI"                , "88"               ,
  # "Monroe County Jail (MCD) Monroe County Corrections"                            , "MONROE COUNTY SHERIFF/JAIL-MAIN"    , "MNROEMI"                , "93"               ,
  # "Monroe County Jail (MCD) Monroe County Corrections"                            , "MONROE COUNTY JAIL"                 , "MONROFL"                , "92"               ,
  # "Monroe County Jail (MCD) Monroe County Corrections"                            , "MONROE COUNTY DETENTION-DORM"       , "MONROMI"                , "92"               ,
  # "Monroe County Jail (MCD) Monroe County Corrections"                            , "MONROE COUNTY JAIL"                 , "MONRONY"                , "92"               ,
  # "Montgomery County Detention Center"                                            , "MONTGOMERY CO MAC SIM BUTLER D.F."  , "MONTGAL"                , "52"               ,
  # "Montgomery County Detention Center"                                            , "MONTGOMERY COUNTY JAIL"             , "MONTGIA"                , "97"               ,
  # "Montgomery County Detention Center"                                            , "MONTGOMERY COUNTY JAIL"             , "MONTGMO"                , "97"               ,
  # "Montgomery County Detention Center"                                            , "MONTGOMERY COUNTY JAIL"             , "MONTGNY"                , "97"               ,
  # "Montgomery County Detention Center"                                            , "MONTGOMERY COUNTY JAIL"             , "MONTGTX"                , "97"               ,
  "Montgomery Processing Center (CIC)"                                            , "MONTGOMERY PROCESSING CTR"          , "MTGPCTX"                , "98"               ,
  # "Morgan County Detention Center"                                                , "MORGAN COUNTY JAIL"                 , "MORGACO"                , "99"               ,
  # "Morgan County Detention Center"                                                , "MORGAN COUNTY SHERIFF'S DEPT"       , "MORGNMO"                , "93"               ,
  "NYE County Jail: 1521 Siri Lane, Pahrump, Nevada 89060"                        , "NYE COUNTY SHERIFF-PAHRUMP"         , "NYEPANV"                , "83"               ,
  # "Nassau County Jail"                                                            , "NASSAU COUNTY JAIL"                 , "NASSAFL"                , "100"              ,
  # "Nassau County Jail"                                                            , "NASSAU CO CORREC CENTER"            , "NASSANY"                , "78"               ,
  # "Nebraska Department of Corrections"                                            , "OMAHA CORRECTIONAL CENTER"          , "NEOMAHA"                , "71"               ,
  "Nevada Southern Detention Center — CCA (Pahrump, NV)"                        , "NEVADA SOUTHERN DETENTION CENTER"   , "NVSDCNV"                , "99"               ,
  # "New Hampshire Department of Corrections"                                       , "NH STATE PRISON, CONCORD"           , "NHSPCON"                , "83"               ,
  # "New Hampshire Department of Corrections"                                       , "NH STATE PRISON FOR WOMEN"          , "NHSPWOM"                , "75"               ,
  "Niagara County Jail, NY"                                                       , "NIAGARA COUNTY JAIL"                , "NIAGANY"                , "99"               ,
  # "Northeast Ohio Correctional Center (NEOCC)"                                    , "CCA NORTHEAST OH CORRECTS"          , "CCANOOH"                , "50"               ,
  # "Northeast Ohio Correctional Center (NEOCC)"                                    , "NORTHEAST CORR CENTER"              , "CTNEACI"                , "66"               ,
  "Oldham County Detention Center"                                                , "OLDHAM COUNTY JAIL"                 , "OLDHAKY"                , "97"               ,
  "Orange County (New York) Jail"                                                 , "ORANGE COUNTY JAIL"                 , "ORANGNY"                , "92"               ,
  # "Oregon State Penitentiary (Salem)"                                             , "NORTHERN OREGON CORR.FAC."          , "NORCOOR"                , "49"               ,
  "Otero County Processing Center, NM"                                            , "OTERO CO PROCESSING CENTER"         , "OTRPCNM"                , "80"               ,
  # "Patrick Moore Detention Center, Okmulgee, OK"                                  , "OKMULGEE COUNTY JAIL"               , "OKMULOK"                , "48"               ,
  # "Pike County Detention Center"                                                  , "PIKE COUNTY JAIL"                   , "PIKCOPA"                , "99"               ,
  # "Pike County Detention Center"                                                  , "PIKE COUNTY JAIL"                   , "PKCNTPA"                , "99"               ,
  "Pine Prairie"                                                                  , "PINE PRAIRIE ICE PROCESSING CENTER" , "PINEPLA"                , "83"               ,
  "Platte County Jail, Platte City, MO"                                           , "PLATTE COUNTY JAIL"                 , "PLATTMO"                , "98"               ,
  "Plymouth County Correctional Facility, Plymouth, MA"                           , "PLYMOUTH CO COR FACILTY"            , "PLYMOMA"                , "87"               ,
  # "Polk County Detention Center"                                                  , "POLK COUNTY JAIL"                   , "POLKCMN"                , "96"               ,
  # "Polk County Detention Center"                                                  , "POLK COUNTY JAIL"                   , "POLKJFL"                , "96"               ,
  # "Polk County Detention Center"                                                  , "POLK COUNTY JAIL"                   , "POLKJIA"                , "96"               ,
  # "Port Isabel Detention Center"                                                  , "PORT ISABEL SPC"                    , "PIC"                    , "95"               ,
  "Prairieland Detention Center"                                                  , "PRAIRIELAND DETENTION CENTER"       , "PRLDCTX"                , "100"              ,
  "Prairieland Detention Facility — Detained CFR"                               , "PRAIRIELAND DETENTION CENTER"       , "PRLDCTX"                , "98"               ,
  "Reeves County Detention Center"                                                , "REEVES COUNTY DET. CENTER"          , "REDETTX"                , "92"               ,
  "Rice City Jail, Lyons, KS"                                                     , "LYON COUNTY JAIL"                   , "LYONJKS"                , "40"               ,
  "Richwood"                                                                      , "RICHWOOD COR CENTER"                , "RWCCMLA"                , "75"               ,
  # "Rio Grande Valley Centralized Processing Center"                               , "RIO GRANDE DETENTION CENTER"        , "RGRNDTX"                , "70"               ,
  # "Rio Grande Valley Centralized Processing Center"                               , "SOUTH TEXAS ICE PROCESSING CENTER"  , "STCDFTX"                , "41"               ,
  # "Rio Grande Valley Centralized Processing Center"                               , "URSULA CENTRALIZED PROCESSING CNTR" , "URSLATX"                , "32"               ,
  "River Correctional Facility, LA"                                               , "River Correctional Center"          , "RVRCCLA"                , "95"               ,
  # "River Corrections Center"                                                      , "River Correctional Center"          , "RVRCCLA"                , "96"               ,
  "Rockingham County Department of Corrections — Brentwood, NH"                 , "ROCKINGHAM COUNTY JAIL"             , "ROCKINH"                , "89"               ,
  "SPC Aguadilla, PR (Puerto Rico, Service Processing Center)"                    , "AGUADILLA SPC"                      , "AGC"                    , "46"               ,
  "Salt Lake County Jail"                                                         , "SALT LAKE COUNTY JAIL"              , "SLSLCUT"                , "100"              ,
  "San Francisco Detention Center,San Francisco, CA"                              , "SAN FRANCISCO CO JAIL"              , "SFCOJCA"                , "89"               ,
  "Seneca County Jail, Tiffin, OH (CLE-CLD)"                                      , "SENECA COUNTY JAIL"                 , "SENECOH"                , "98"               ,
  "Shawnee County Detention Center, Shawnee, KS"                                  , "SHAWNEE COUNTY DEPT OF CORR."       , "SHACOKS"                , "91"               ,
  # "South Florida Reception Center, Miami, FL"                                     , "MIAMI CORRECTIONAL CENTER"          , "INMIAMI"                , "46"               ,
  # "South Texas Detention Facility"                                                , "COASTAL BEND DET. FACILITY"         , "CBENDTX"                , "43"               ,
  # "South Texas Detention Facility"                                                , "HIDALGO COUNTY DETENTION CENTER"    , "HIDALNM"                , "51"               ,
  # "South Texas Detention Facility"                                                , "KARNES COUNTY CIVIL DET. FACILITY"  , "KCCDCTX"                , "43"               ,
  # "South Texas Detention Facility"                                                , "SOUTH TEXAS ICE PROCESSING CENTER"  , "STCDFTX"                , "80"               ,
  # "South Texas Detention Facility"                                                , "SOUTH TEXAS FAM RESIDENTIAL CENTER" , "STFRCTX"                , "87"               ,
  # "South Texas Detention Facility"                                                , "WILLACY CO  REGIONAL DETN FACIL"    , "WCRDFTX"                , "40"               ,
  # "South Texas Detention Facility"                                                , "WILLACY COUNTY DETENTION CENTER"    , "WILLCTX"                , "47"               ,
  "South Texas Family Residential Center, Dilley, TX"                             , "SOUTH TEXAS FAM RESIDENTIAL CENTER" , "STFRCTX"                , "97"               ,
  "South Texas ICE Processing Center"                                             , "SOUTH TEXAS ICE PROCESSING CENTER"  , "STCDFTX"                , "100"              ,
  "Southern Desert Correctional Facility at Indian Springs."                      , "SOUTHERN DESERT CORR CTR"           , "NVSODES"                , "74"               ,
  "St. Clair County Jail, Port Huron, MI"                                         , "ST. CLAIR COUNTY JAIL"              , "STCLAMI"                , "98"               ,
  "St. Louis, Missouri Detention Center"                                          , "ST. LOUIS COUNTY JAIL"              , "STLOUMO"                , "79"               ,
  "Stewart Detention Center"                                                      , "STEWART DETENTION CENTER"           , "STWRTGA"                , "100"              ,
  "Strafford County House of Corrections, NH"                                     , "STRAFFORD CO DEPT OF CORR"          , "STRAFNH"                , "96"               ,
  # "Summitt County Jail"                                                           , "SUMMIT COUNTY JAIL"                 , "SUMMICO"                , "91"               ,
  # "Summitt County Jail"                                                           , "SUMMIT COUNTY JAIL"                 , "SUMMIUT"                , "91"               ,
  "The Federal Detention Center, Sea Tac"                                         , "SEATAC FED.DET.CENTER"              , "BOPSET"                 , "77"               ,
  "Torrance County Detention Facility, NM"                                        , "TORRANCE/ESTANCIA, NM"              , "TOORANM"                , "77"               ,
  "Tri County Detention Center"                                                   , "TRI-COUNTY JAIL"                    , "TRICOIL"                , "99"               ,
  "Two Bridges Regional Jail, ME"                                                 , "TWO BRIDGES REGIONAL JAIL"          , "TBRJLME"                , "99"               ,
  "Utah Correctional Facility"                                                    , "UTAH STATE PRISON"                  , "UTSPDRA"                , "82"               ,
  "Utah County Jail"                                                              , "UTAH COUNTY JAIL"                   , "UTAHCUT"                , "100"              ,
  "Victorville Federal Correctional Institute"                                    , "FCI VICTORVILLE"                    , "BOPVIM"                 , "82"               ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WAJAIMN"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASCONY"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY DET. CTR"         , "WASCOTN"                , "86"               ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASHCIA"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASHCUT"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASHIAR"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASHICO"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON CO. DETENTION"           , "WASHIMD"                , "91"               ,
  # "Washington County Jail"                                                        , "WASHINGTON COUNTY JAIL"             , "WASHIME"                , "100"              ,
  # "Washington County Jail"                                                        , "WASHINGTON CO JAIL, BLAIR"          , "WASHINE"                , "74"               ,
  # "Washington County Jail"                                                        , "WASHINGTON CO. JAIL, PA"            , "WASHIPA"                , "91"               ,
  # "Washington DCO-Washington State Penitentiary, Walla Walla, WA"                 , "WASHINGTON CORRECTIONAL"            , "NYWASHC"                , "83"               ,
  # "Washington Department of Corrections (Monroe)"                                 , "DEPARTMENT OF CORRECTIONS"          , "DEPCOGU"                , "57"               ,
  # "Washington Department of Corrections (Monroe)"                                 , "WASHINGTON CORRECTIONAL"            , "NYWASHC"                , "88"               ,
  # "Washington Department of Corrections (Monroe)"                                 , "WEST MONROE CORR. DEPT"             , "WESMOLA"                , "67"               ,
  # "Washington Department of Corrections (Walla Walla)"                            , "WASHINGTON CORRECTIONAL"            , "NYWASHC"                , "89"               ,
  "Washoe County Jail, NV"                                                        , "WASHOE COUNTY JAIL"                 , "WASHONV"                , "98"               ,
  "Weber County Jail"                                                             , "WEBER COUNTY JAIL"                  , "WEBERUT"                , "100"              ,
  "West TN Detention Facility"                                                    , "WESTERN TENNESSEE DET. FAC."        , "TNWESDF"                , "43"               ,
  # "Winnfield"                                                                     , "WINN CORRECTIONAL CENTER"           , "LAWINCI"                , "39"               ,
  # "Women's Community Correctional Center"                                         , "WOMEN'S DETENTION CENTER"           , "WDMIAFL"                , "97"               ,
  "Wyatt Detention Center, RI"                                                    , "WYATT DETENTION CENTER"             , "WYATTRI"                , "99"               ,
  "York Correctional Center, Niantic, CT"                                         , "YORK CORR INST"                     , "CTYORCI"                , "57"               ,
  # "Yuma Sector Centralized Processing Center"                                     , "YUMA BORDER PATROL"                 , "IBPSYUM"                , "62"
)
