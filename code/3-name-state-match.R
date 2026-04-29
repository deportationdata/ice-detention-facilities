library(tidyverse)
library(tidylog)

facility_attributes <- arrow::read_parquet(
  "data/facilities-attributes-raw.parquet"
)

name_state_match_inputs <-
  facility_attributes |>
  filter(
    # just ICE sources
    !source %in%
      c(
        "eoir",
        "hifld_local_law_enforcement",
        "hifld_prisons",
        "hospitals",
        "jails_prisons",
        "marshall",
        "vera"
      )
  ) |>
  filter(!is.na(detention_facility_code) & !is.na(name))

code_state_match <-
  name_state_match_inputs |>
  filter_out(is.na(state)) |>
  group_by(detention_facility_code) |>
  arrange(date) |>
  summarize(
    state = last(state),
    date = last(date),
    source = last(source),
    states = list(unique(state)),
    n_states = n_distinct(state),
    .groups = "drop"
  )

# confirm there are none
# code_state_match |>
#   filter(n_states > 1) |>
#   arrange(detention_facility_code)

name_code_state_match_interim <-
  name_state_match_inputs |>
  distinct(detention_facility_code, name) |>
  left_join(
    code_state_match |> select(detention_facility_code, state, date, source),
    by = "detention_facility_code"
  )

name_code_state_match_from_data <-
  name_code_state_match_interim |>
  filter(!is.na(state)) |>
  mutate(
    state = case_when(
      detention_facility_code == "HACMCNJ" ~ "NJ", # based on manual lookup "HACMCNJ - HACKENSACK MERIDIAN HEALTH CENTER, NEW JERSEY" - error in 05655 and 22955 datasets attributes it to NYC (210 E 64th St, New York NY)
      TRUE ~ state
    )
  )

# treat hold rooms separately
# name_code_state_match_interim |>
#   filter(is.na(state)) |>
#   filter(str_detect(detention_facility_code, "HOLD")) |>
#   clipr::write_clip()

hold_room_state_match <- tribble(
  ~detention_facility_code , ~name                         , ~state ,
  "DULHOLD"                , "DULUTH HOLD ROOM"            , "MN"   , # https://www.vera.org/ice-detention-trends
  "RICHOLD"                , "RICHLAND HOLD ROOM"          , "WA"   , # https://www.ice.gov/node/62177
  "SYRHOLD"                , "SYRACUSE HOLD ROOM"          , "NY"   , # https://www.vera.org/ice-detention-trends
  "CBPHOLD"                , "BUFFALO USBP HOLD ROOM"      , "NY"   , # https://www.cbp.gov/border-security/along-us-borders/border-patrol-sectors/buffalo-sector-new-york and confirmed arrests are in Buffalo NY via matching to arrests table
  "CHSHOLD"                , "ERO CHARLESTON WV HOLD ROOM" , "WV"   , # stated in name
  "SAVHOLD"                , "SAVANNAH HOLD ROOM"          , "GA"   , # TODO: best guess
)

# code_name_state_guesses <-
#   name_code_state_match_interim |>
#   filter(is.na(state), !str_detect(detention_facility_code, "HOLD")) |>
#   select(-state) |>
#   arrange(detention_facility_code, name) |>
#   mutate(
#     state_guess_end = str_sub(detention_facility_code, -2),
#     state_guess_start = str_sub(detention_facility_code, 1, 2),
#     state = case_when(
#       str_sub(detention_facility_code, 1, 3) == "BOP" ~ NA_character_,
#       nchar(detention_facility_code) < 7 ~ NA_character_,
#       state_guess_start %in%
#         c(state.abb, "PR") &
#         state_guess_end %in%
#           c(state.abb, "PR") &
#         state_guess_start == state_guess_end ~ state_guess_start,
#       state_guess_start %in%
#         c(state.abb, "PR") &
#         state_guess_end %in%
#           c(state.abb, "PR") ~ NA_character_,
#       state_guess_start %in%
#         c(state.abb, "PR") ~ state_guess_start,
#       state_guess_end %in%
#         c(state.abb, "PR") ~ state_guess_end,
#       TRUE ~ NA_character_
#     )
#   )

# code_name_state_unambiguous <-
#   code_name_state_guesses |>
#   filter(!is.na(state)) |>
#   select(-state_guess_start, -state_guess_end)

# find the short ones that don't have state info and add them in manually
# code_name_state_guesses |>
#   filter(nchar(detention_facility_code) < 7) |>
#   select(detention_facility_code, name) |>
#   clipr::write_clip()

# TODO: there is one with code "OTHER" that is "OTHER-OTHER FACILITY" - assuming that is catchall and not a specific place
code_name_state_unambiguous_short_manual <-
  tribble(
    ~detention_facility_code , ~name                                , ~state ,
    "BOPATL"                 , "ATLANTA U.S. PEN."                  , "GA"   , # https://www.bop.gov/locations/institutions/atl/
    "BOPBER"                 , "FCI BERLIN"                         , "NH"   , # https://www.bop.gov/locations/institutions/ber/
    "BOPMRG"                 , "MORGANTOWN FED.CORR.INST."          , "WV"   , # https://www.bop.gov/locations/institutions/mrg/
    "BOPNEO"                 , "NORTHEAST OHIO CORRECTIONAL CENTER" , "OH"   , # https://www.ice.gov/detain/detention-facilities/northeast-ohio-correctional-center
    "BOPPHL"                 , "FDC PHILADELPHIA"                   , "PA"   , # https://www.bop.gov/locations/institutions/phl/phl_prea.pdf?v=1.0.2
    "EROFCB"                 , "ERO EL PASO CAMP EAST MONTANA"      , "TX"   , # https://www.ice.gov/detain/detention-facilities/camp-east-montana
    "FY14"                   , "YAKIMA SUB-OFFICE"                  , "WA"   , # 05655 dataset for YAKHOLD - same name different code
    # "OTHER"                  , "OTHER-OTHER FACILITY"               , ""     ,
    "SALARM"                 , "SALVATION ARMY, SAN JUAN, PR"       , "PR"   , # based on text
    "BOPLAT"                 , "LA TUNA FCI"                        , "TX" # https://www.bop.gov/locations/institutions/lat/
  )

# code_name_state_guesses |>
#   filter(nchar(detention_facility_code) >= 7) |>
#   transmute(
#     detention_facility_code,
#     name,
#     state_guess = case_when(
#       !is.na(state) ~ state,
#       TRUE ~ str_c(state_guess_start, " ", state_guess_end)
#     )
#   ) |>
#   left_join(
#     code_name_state_manual |> select(-name) |> rename(state_manual = state),
#     by = "detention_facility_code"
#   ) |>
#   mutate(
#     state_final = case_when(
#       !is.na(state_manual) ~ state_manual,
#       TRUE ~ state_guess
#     ),
#     .keep = "unused"
#   ) |>
#   relocate(notes, .after = state_final) |>
#   clipr::write_clip()

code_name_state_manual <-
  tribble(
    ~detention_facility_code , ~name                                , ~state , ~notes                                                                                 ,
    "ABIRJVA"                , "SW VIRGINIA REG JAIL AUTH-ABG FAC"  , "VA"   , ""                                                                                     ,
    "ABRMCTX"                , "ABILENE REGIONAL MED CTR"           , "TX"   , ""                                                                                     ,
    "ADAMSIA"                , "ADAMS COUNTY JAIL"                  , "IA"   , ""                                                                                     ,
    "ADVHBCA"                , "ADVENTIST HEALTH BAKERSFIELD"       , "CA"   , ""                                                                                     ,
    "ADVHCHI"                , "ADVENTIST HEALTH CASTLE"            , "HI"   , ""                                                                                     ,
    "ADVHDCA"                , "ADVENTIST HEALTH DELANO"            , "CA"   , ""                                                                                     ,
    "AHGRFND"                , "ALTRU HOSPITAL GRAND FORKS"         , "ND"   , ""                                                                                     ,
    "ALAMOCO"                , "ALAMOSA COUNTY JAIL"                , "CO"   , ""                                                                                     ,
    "ALEXAAL"                , "ALEXANDRIA CITY JAIL"               , "AL"   , ""                                                                                     ,
    "ALLEGMI"                , "ALLEGAN CO. SHER. ALL.,MI"          , "MI"   , ""                                                                                     ,
    "ANDERSC"                , "ANDERSON COUNTY DET CENTER"         , "SC"   , ""                                                                                     ,
    "ANSGHTX"                , "ANSON GENERAL HOSPITAL"             , "TX"   , ""                                                                                     ,
    "ANWHMMN"                , "ABBOTT NORTHWESTERN HOSP"           , "MN"   , ""                                                                                     ,
    "ASHECNC"                , "ASHE COUNTY JAIL"                   , "NC"   , ""                                                                                     ,
    "AUGCOVA"                , "AUGUSTA CO JAIL"                    , "VA"   , ""                                                                                     ,
    "BALCOMD"                , "BALTO. CNTY DET CNTR"               , "MD"   , ""                                                                                     ,
    "BANHOAZ"                , "BANNER UNIVERSITY MEDICAL PHOENIX"  , "AZ"   , ""                                                                                     ,
    "BARREKY"                , "BARREN COUNTY DET. CENTER"          , "KY"   , ""                                                                                     ,
    "BAYPTFL"                , "BAY POINT SCHOOLS"                  , "FL"   , ""                                                                                     ,
    "BELTRMN"                , "BELTRAMI COUNTY JAIL"               , "MN"   , ""                                                                                     ,
    "BFLHPNY"                , "BUFFALO GENERAL HOSPITAL"           , "NY"   , ""                                                                                     ,
    "BHCSICA"                , "BHC SIERRA VISTA(HOSP.)"            , "CA"   , ""                                                                                     ,
    "BIINCCO"                , "BI INCORORATED, GEO GROUP COMPANY"  , "CO"   , ""                                                                                     ,
    "BIMDCAZ"                , "BANNER IRONWOOD MED CTR"            , "AZ"   , ""                                                                                     ,
    "BLKREOK"                , "BLACKWELL REGIONAL HOSPITAL"        , "OK"   , ""                                                                                     ,
    "BMCSATX"                , "BAPTIST MEDICAL CENTER"             , "TX"   , ""                                                                                     ,
    "BNMCTAZ"                , "BANNER MEDICAL CTR TUCSON"          , "AZ"   , ""                                                                                     ,
    "BONHOSD"                , "BON HOMME CO., SD"                  , "SD"   , ""                                                                                     ,
    "BOONEMO"                , "BOONE COUNTY JAIL"                  , "MO"   , ""                                                                                     ,
    "BOURBKY"                , "BOURBON CO DET CENTER"              , "KY"   , ""                                                                                     ,
    "BRDFDFL"                , "BRADFORD COUNTY JAIL"               , "FL"   , ""                                                                                     ,
    "BRLHOAK"                , "BARTLETT REGIONAL HOSPITAL"         , "AK"   , ""                                                                                     ,
    "BROHOMI"                , "BRONSON METHODIST HOSPITAL"         , "MI"   , ""                                                                                     ,
    "BROWNWI"                , "BROWN COUNTY JAIL"                  , "WI"   , ""                                                                                     ,
    "BRZYTTX"                , "BRAZORIA COUNTY YOUTH HOMES"        , "TX"   , ""                                                                                     ,
    "BSQUETX"                , "BOSQUE COUNTY SHERIFF'S DEPT."      , "TX"   , ""                                                                                     ,
    "BSWRRTX"                , "BAYLOR SCOTT WHITE MED RR"          , "TX"   , ""                                                                                     ,
    "BTHHMPA"                , "BETHANY CHILDRENS' HOME"            , "PA"   , ""                                                                                     ,
    "BTSHOMI"                , "COREWELL HLTH GR HOSP-BUTTERWORTH"  , "MI"   , ""                                                                                     ,
    "BWRGMTX"                , "BEST WEST ROSE GRDN I&S"            , "TX"   , ""                                                                                     ,
    "CACENCW"                , "CENTRAL CA WOMEN'S FACILI"          , "CA"   , ""                                                                                     ,
    "CACFMCF"                , "MCFARLAND CCF"                      , "CA"   , ""                                                                                     ,
    "CAINTRC"                , "CALIF.INST.RECEPTION CTR."          , "CA"   , ""                                                                                     ,
    "CARCWOM"                , "CALIF. REHAB. CTR./WOMEN"           , "CA"   , ""                                                                                     ,
    "CATHOFL"                , "CATHOLIC HOME"                      , "FL"   , ""                                                                                     ,
    "CC2JVTX"                , "CHILDREN'S CENTER-FOSTER CARE"      , "TX"   , ""                                                                                     ,
    "CCCSSTX"                , "CHRISTUS SPOHN HOSP CC - SOUTH"     , "TX"   , ""                                                                                     ,
    "CCFCPFL"                , "CATHOLIC CHARITIES - FCP"           , "FL"   , ""                                                                                     ,
    "CCHAUTX"                , "CROSS CREEK HOSPITAL"               , "TX"   , ""                                                                                     ,
    "CCHRTTX"                , "CEDAR CREST HOSP TREAT"             , "TX"   , ""                                                                                     ,
    "CCRMCCA"                , "CONTRA COSTA REG MED CTR"           , "CA"   , ""                                                                                     ,
    "CCRMHMN"                , "CENTRACARE RICE MEMORIAL HOSP"      , "MN"   , ""                                                                                     ,
    "CCSCCCA"                , "CATHOLIC CHARITIES"                 , "CA"   , ""                                                                                     ,
    "CCSTJTX"                , "ST JEROME'S GROUP HOME"             , "TX"   , ""                                                                                     ,
    "CDEJVVA"                , "CASA DE ESPERANZA"                  , "VA"   , ""                                                                                     ,
    "CENTRVA"                , "CENTRA HEALTH"                      , "VA"   , ""                                                                                     ,
    "CHANRAZ"                , "CHANDLER REG MED CTR"               , "AZ"   , ""                                                                                     ,
    "CHENANY"                , "CHENANGO COUNTY JAIL"               , "NY"   , ""                                                                                     ,
    "CHFLDVA"                , "CHESTERFIELD CO. JAIL"              , "VA"   , ""                                                                                     ,
    "CHIHMIA"                , "CHI HEALTH MERCY COUNCIL BLUFFS"    , "IA"   , ""                                                                                     ,
    "CHIHSFL"                , "CHILDREN'S HOME SOCIETY FLORIDA"    , "FL"   , ""                                                                                     ,
    "CHISLTX"                , "CHI ST LUKE'S HEALTH MEM"           , "TX"   , ""                                                                                     ,
    "CHPHOVA"                , "CHIPPENHAM HOSPITAL - RICHMOND"     , "VA"   , ""                                                                                     ,
    "CHPKPNY"                , "CHILDREN'S HOME OF POUGHKEEPSIE"    , "NY"   , ""                                                                                     ,
    "CHSPKVA"                , "CHESAPEAKE CITY JAIL"               , "VA"   , ""                                                                                     ,
    "CHSRHTX"                , "CHRISTUS SANTA ROSA HOSPITAL - MED" , "TX"   , ""                                                                                     ,
    "CHTAMFL"                , "THE CHILDREN'S HOME (TAMPA)"        , "FL"   , ""                                                                                     ,
    "CHTLNFL"                , "CHATLIN HOUSE"                      , "FL"   , ""                                                                                     ,
    "CIBOGNM"                , "CIBOLA GENERAL HOSPITAL"            , "NM"   , ""                                                                                     ,
    "CISAETX"                , "COMFORT INN STE ELP AIRP"           , "TX"   , ""                                                                                     ,
    "CISEPTX"                , "COMFORT SUITES CASA CONSUELO"       , "TX"   , ""                                                                                     ,
    "CISPVLA"                , "COUNTY INN & STE BY RAD"            , "LA"   , ""                                                                                     ,
    "CLARKIN"                , "CLARK COUNTY JAIL"                  , "IN"   , ""                                                                                     ,
    "CMCLSNY"                , "CARDINAL MCCLOSKEY"                 , "NY"   , ""                                                                                     ,
    "CNTRHWA"                , "CENTRAL WASHINGTON HOSPITAL"        , "WA"   , ""                                                                                     ,
    "COLUMFL"                , "COLUMBIA COUNTY JAIL"               , "FL"   , "https://columbiasheriff.org/detention-facility/"                                      ,
    "CONHOTX"                , "CONNALLY MEMORIAL MEDICAL CENTER"   , "TX"   , ""                                                                                     ,
    "COOCONH"                , "COOS COUNTY JAIL"                   , "NH"   , ""                                                                                     ,
    "COOKCMN"                , "COOK CO. JAIL, MN"                  , "MN"   , ""                                                                                     ,
    "COTTOMN"                , "COTTONWOOD COUNTY JAIL"             , "MN"   , ""                                                                                     ,
    "COXMEMO"                , "COXHEALTH MEDICAL CENTER SOUTH"     , "MO"   , "https://www.coxhealth.com/our-locations/cox-medical-center-south/"                    ,
    "CPBDCKY"                , "CAMPBELL CO DET CTR"                , "KY"   , ""                                                                                     ,
    "CPPAPAZ"                , "CROWNE PLAZA PHOENIX AP"            , "AZ"   , ""                                                                                     ,
    "CRITTAR"                , "CRITTENDEN COUNTY DET CTR"          , "AR"   , ""                                                                                     ,
    "CSANOLA"                , "COMFORT SUITES ALEXANDRIA"          , "LA"   , ""                                                                                     ,
    "CSHEPTX"                , "CHASE STE HOTEL ELP"                , "TX"   , ""                                                                                     ,
    "CSLLFTX"                , "CHI ST LUKE'S H M LUFKIN"           , "TX"   , ""                                                                                     ,
    "CSSCHVA"                , "CENTRA SOUTHSIDE COMM HOSP"         , "VA"   , ""                                                                                     ,
    "CVANXCA"                , "CENTRAL VALLEY ANNEX"               , "CA"   , ""                                                                                     ,
    "CVECONY"                , "THE CHILDRENS VILLAGE- ECHO HILLS"  , "NY"   , ""                                                                                     ,
    "CWHCCMP"                , "COMMONWEALTH HEALTHCARE CORP"       , "MP"   , "https://www.chcc.health/"                                                             ,
    "DAYBROH"                , "DAYBREAK RUNAWAY"                   , "OH"   , ""                                                                                     ,
    "DEKABGA"                , "DEKALB COUNTY JAIL"                 , "GA"   , ""                                                                                     ,
    "DIIHBMD"                , "DAYS INN INNER HARBOR BALTIMORE"    , "MD"   , ""                                                                                     ,
    "DISPTAZ"                , "DRURY INN STES PHO TEMPE"           , "AZ"   , ""                                                                                     ,
    "DISSATX"                , "DRURY INN & STES SNA AP"            , "TX"   , ""                                                                                     ,
    "DPD09MI"                , "DET PD 9TH PREC."                   , "MI"   , ""                                                                                     ,
    "DPLJVTX"                , "DEPELCHIN CHILDREN'S CENTER"        , "TX"   , ""                                                                                     ,
    "DRHCSTX"                , "DIMMIT REGIONAL HOSPITAL"           , "TX"   , ""                                                                                     ,
    "DVCSDKY"                , "DAVIESS COUNTY DETENTION CENTER"    , "KY"   , ""                                                                                     ,
    "DVHSPNV"                , "DESERT VIEW HOSPITAL"               , "NV"   , ""                                                                                     ,
    "DYKCCWA"                , "RUTH DYKEMAN CHILD CENTER"          , "WA"   , ""                                                                                     ,
    "ECRMCCA"                , "EL CENTRO REGIONAL MED CTR"         , "CA"   , ""                                                                                     ,
    "EFMHMFL"                , "ED FRASER MEMORIAL HOSPITAL"        , "FL"   , ""                                                                                     ,
    "ELKHAIN"                , "ELKHART COUNTY JAIL"                , "IN"   , ""                                                                                     ,
    "EMBCRNC"                , "EMBASSY SUITES BY HILTON"           , "NC"   , ""                                                                                     ,
    "EMBSATX"                , "EMBASSY STE HLTN SNA AP"            , "TX"   , ""                                                                                     ,
    "ENLMCCA"                , "ENLOE MEDICAL CENTER"               , "CA"   , ""                                                                                     ,
    "EPSSFTX"                , "EL PASO SOFT SIDED FACILITY"        , "TX"   , ""                                                                                     ,
    "ESAPAAZ"                , "EXTENDED STAY AMERICA PHO AP"       , "AZ"   , ""                                                                                     ,
    "ESHSATX"                , "EMBASSY STE HLTN NW I-10"           , "TX"   , ""                                                                                     ,
    "ESKHLIN"                , "ESKENAZI HEALTH"                    , "IN"   , ""                                                                                     ,
    "EXTSAAZ"                , "EXTENDED STAY AMERICA"              , "AZ"   , ""                                                                                     ,
    "FAYETIL"                , "FAYETTE COUNTY JAIL"                , "IL"   , ""                                                                                     ,
    "FIPCDGA"                , "FOLKSTON D RAY ICE PROCESSING CTR"  , "GA"   , ""                                                                                     ,
    "FLBAKCI"                , "BAKER C. I."                        , "FL"   , ""                                                                                     ,
    "FLCHACI"                , "CHARLOTTE CI, PUNTA GORDA"          , "FL"   , ""                                                                                     ,
    "FLDADCI"                , "DADE CORRECTIONAL INST"             , "FL"   , ""                                                                                     ,
    "FLDCTAL"                , "FL DEPT OF CORRECTIONS"             , "FL"   , ""                                                                                     ,
    "FLDSSFS"                , "FLORIDA SOFT-SIDED FACILITY-SOUTH"  , "FL"   , ""                                                                                     ,
    "FLDSSFS"                , "FLORIDA SOFT SIDED FACILITY SOUTH"  , "FL"   , "ice.gov/detain/detention-facilities (no hyphens variant)"                             ,
    "FLHOSFL"                , "FORT LAUDERDALE HOSPITAL"           , "FL"   , ""                                                                                     ,
    "FLSPSTA"                , "FLORIDA ST. PRIS.,STARKE"           , "FL"   , ""                                                                                     ,
    "FNDDUWI"                , "FOND DU LAC COUNTY SHERIFF/JAIL"    , "WI"   , ""                                                                                     ,
    "FRANKPA"                , "FRANKLIN COUNTY JAIL"               , "PA"   , ""                                                                                     ,
    "FRFLDWY"                , "FAIRFIELD INN & SUITES"             , "WY"   , ""                                                                                     ,
    "FSATCOK"                , "FT SILL ARMY TRAINING CENTER"       , "OK"   , ""                                                                                     ,
    "GADRYJM"                , "D. RAY JAMES PRISON"                , "GA"   , ""                                                                                     ,
    "GCJFCFL"                , "GULF COAST JEWISH & FAMILY COM SVC" , "FL"   , ""                                                                                     ,
    "GILLICO"                , "GILLIAM YOUTH CENTER"               , "CO"   , ""                                                                                     ,
    "GOODCID"                , "GOODING COUNTY JAIL"                , "ID"   , ""                                                                                     ,
    "GRGBVTX"                , "GEORGETOWN BEHAVIORAL HOSPITAL"     , "TX"   , ""                                                                                     ,
    "GSAMHCA"                , "GOOD SAMARITAN HOSPITAL"            , "CA"   , ""                                                                                     ,
    "GTMOACU"                , "WINDWARD HOLDING FACILITY"          , "CU"   , ""                                                                                     ,
    "GTMOBCU"                , "MIGRANT OPS CENTER EAST"            , "CU"   , ""                                                                                     ,
    "GTMODCU"                , "MIGRANT OPS CENTER MAIN AV622"      , "CU"   , ""                                                                                     ,
    "GUAD2IL"                , "CASA GUADALUPE #2"                  , "IL"   , ""                                                                                     ,
    "GUAD3IL"                , "CASA GUADALUPE #3"                  , "IL"   , ""                                                                                     ,
    "GUAMHGU"                , "GUAM MEMORIAL HOSPITAL"             , "GU"   , ""                                                                                     ,
    "HAMPITX"                , "HAMPTON INN PEARSALL"               , "TX"   , ""                                                                                     ,
    "HARRIMS"                , "HARRISON DETENTION CENTER"          , "MS"   , ""                                                                                     ,
    "HASMHTX"                , "HASKELL MEMORIAL HOSPITAL"          , "TX"   , ""                                                                                     ,
    "HCAFMFL"                , "HCA FLORIDA MEMORIAL HOSPITAL"      , "FL"   , ""                                                                                     ,
    "HCMCMMN"                , "HENNEPIN COUNTY MED CTR"            , "MN"   , ""                                                                                     ,
    "HDHSPNV"                , "HENDERSON HOSPITAL"                 , "NV"   , ""                                                                                     ,
    "HDLGOTX"                , "HIDALGO POLICE DEPT."               , "TX"   , ""                                                                                     ,
    "HGHBECO"                , "HIGHLANDS BEHAVIORAL HEALTH"        , "CO"   , ""                                                                                     ,
    "HIELPTX"                , "HOLIDAY INN EXPRESS & SUITES"       , "TX"   , ""                                                                                     ,
    "HIESATX"                , "HOLIDAY INN EXP SAN ANTONIO"        , "TX"   , ""                                                                                     ,
    "HIESPAZ"                , "HOLIDAY INN EXP PHOENIX"            , "AZ"   , ""                                                                                     ,
    "HIEXSTX"                , "HOLIDAY INN EXPR & STES PEARSALL"   , "TX"   , ""                                                                                     ,
    "HIPANAZ"                , "HAMPTON INN PHO AIRPORT N"          , "AZ"   , ""                                                                                     ,
    "HIPDBAZ"                , "HOLIDAY INN PHO DWTN BALLPARK"      , "AZ"   , ""                                                                                     ,
    "HIRAYTX"                , "HOLIDAY INN EXPRESS & SUITES"       , "TX"   , ""                                                                                     ,
    "HISAXLA"                , "HAMPTON INN & STES ALEXANDRIA"      , "LA"   , ""                                                                                     ,
    "HISDBCA"                , "HOLIDAY INN SND BAYSIDE"            , "CA"   , ""                                                                                     ,
    "HISEPTX"                , "HAMPTON INN & STES ELP AP"          , "TX"   , ""                                                                                     ,
    "HISPAAZ"                , "HOLIDAY INN & STE PHO AP"           , "AZ"   , ""                                                                                     ,
    "HITCHNE"                , "HITCHCOCK COUNTY JAIL"              , "NE"   , ""                                                                                     ,
    "HLFRJVA"                , "B.R.R.J. HALIFAX"                   , "VA"   , ""                                                                                     ,
    "HNYHOMI"                , "HENRY FORD HOSPITAL"                , "MI"   , ""                                                                                     ,
    "HOLTCNE"                , "HOLT COUNTY JAIL"                   , "NE"   , ""                                                                                     ,
    "HPKNSKY"                , "HOPKINS COUNTY JAIL"                , "KY"   , ""                                                                                     ,
    "HSELPTX"                , "HAWTHORN STES EL PASO AIRP"         , "TX"   , ""                                                                                     ,
    "HTLPPTX"                , "HOTEL PHARR PLAZA"                  , "TX"   , ""                                                                                     ,
    "HUNMHTX"                , "HUNTSVILLE MEMORIAL HOSPITAL"       , "TX"   , ""                                                                                     ,
    "IESDRTX"                , "INTERNATIONAL EDU SVC-DRISCOLL"     , "TX"   , ""                                                                                     ,
    "IESHDTX"                , "INTERNATIONAL EDU SVC-HIDALGO"      , "TX"   , ""                                                                                     ,
    "IESHRTX"                , "INTERNATIONAL EDU SVC-HARLINGEN"    , "TX"   , ""                                                                                     ,
    "IESWSTX"                , "INTERNATIONAL EDU SVCS-WESLACO"     , "TX"   , ""                                                                                     ,
    "ILPICKN"                , "PINCKNEYVILLE CORRECTIONAL CENTER"  , "IL"   , ""                                                                                     ,
    "INDCMIC"                , "IN DEPT. OF CORRECTIONS"            , "IN"   , ""                                                                                     ,
    "INDMPIN"                , "INDIANAPOLIS METROPOLITAN PD"       , "IN"   , ""                                                                                     ,
    "INMIAMI"                , "MIAMI CORRECTIONAL CENTER"          , "IN"   , "https://www.ice.gov/detain/detention-facilities/miami-correctional-facility-mcf"      ,
    "INTEGOK"                , "INTEGRIS HEALTH BAPTIST MEDICAL"    , "OK"   , ""                                                                                     ,
    "JACKSIN"                , "JACKSON COUNTY JAIL"                , "IN"   , ""                                                                                     ,
    "JACKSTN"                , "JACKSON COUNTY SHERIFF"             , "TN"   , ""                                                                                     ,
    "JEFFEKY"                , "JEFFERSON COUNTY JAIL"              , "KY"   , ""                                                                                     ,
    "JHOGGTX"                , "JIM HOGG COUNTY SHERIFF"            , "TX"   , ""                                                                                     ,
    "JMESNTX"                , "JAMESON TRANSITIONAL FOSTER CARE"   , "TX"   , ""                                                                                     ,
    "JMTHOVA"                , "JOHNSTON MEMORIAL HOSPITAL"         , "VA"   , ""                                                                                     ,
    "JOPSHTX"                , "JOHN PETER SMITH HOSPITAL"          , "TX"   , ""                                                                                     ,
    "JUVCETX"                , "JUVENILE CENTER"                    , "TX"   , ""                                                                                     ,
    "KALKAMI"                , "KALKASKA CO.,KALKASKA,MI"           , "MI"   , ""                                                                                     ,
    "KANECIL"                , "KANE COUNTY JAIL"                   , "IL"   , ""                                                                                     ,
    "KDSFCPA"                , "KIDSPEACE FOSTER CARE PROGRAM"      , "PA"   , ""                                                                                     ,
    "KDSPCMD"                , "KIDSPEACE LTFC-BALTIMORE"           , "MD"   , ""                                                                                     ,
    "KENMHNY"                , "KENMORE MERCY HOSPITAL"             , "NY"   , ""                                                                                     ,
    "KENTHRI"                , "KENT HOSPITAL"                      , "RI"   , ""                                                                                     ,
    "KERNHCA"                , "KERN MEDICAL HOSPITAL"              , "CA"   , ""                                                                                     ,
    "KIOWAKS"                , "KIOWA COUNTY JAIL"                  , "KS"   , ""                                                                                     ,
    "KPCHOAZ"                , "PROMISE HOSPITAL"                   , "AZ"   , ""                                                                                     ,
    "KSELPCF"                , "EL DORADO CORR. FACILITY"           , "KS"   , "https://www.doc.ks.gov/facilities/edcf"                                               ,
    "LAFAYMS"                , "LAFAYETTE COUNTY, MS"               , "MS"   , ""                                                                                     ,
    "LAKCOFL"                , "LAKE COUNTY JAIL"                   , "FL"   , ""                                                                                     ,
    "LAKCOOH"                , "LAKE COUNTY JAIL"                   , "OH"   , ""                                                                                     ,
    "LAKE2IL"                , "LAKESIDE - WASHINGTON PARK"         , "IL"   , ""                                                                                     ,
    "LAVACTX"                , "LAVACA COUNTY JAIL"                 , "TX"   , ""                                                                                     ,
    "LBJHSTX"                , "HARRIS HEALTH LBJ HOSPITAL"         , "TX"   , ""                                                                                     ,
    "LCIMCTX"                , "LA COPA INN HOTEL"                  , "TX"   , ""                                                                                     ,
    "LHMCBMA"                , "LAHEY HOSPITAL & MED CTR"           , "MA"   , ""                                                                                     ,
    "LICEPLA"                , "LOUISIANA ICE PROCESSING CENTER"    , "LA"   , ""                                                                                     ,
    "LIMCJTX"                , "LIMESTONE COUNTY JAIL"              , "TX"   , ""                                                                                     ,
    "LIMMCTX"                , "LIMESTON MEDICAL CENTER"            , "TX"   , ""                                                                                     ,
    "LIRSFCO"                , "LIRS - LFSRM FORT COLLINS LTFC CO"  , "CO"   , ""                                                                                     ,
    "LOCMHNY"                , "LOCKPORT MEMORIAL HOSPITAL"         , "NY"   , ""                                                                                     ,
    "LOGANOK"                , "LOGAN COUNTY JAIL"                  , "OK"   , ""                                                                                     ,
    "LPD77CA"                , "LOS ANGELES POLICE PD 77TH DIV."    , "CA"   , ""                                                                                     ,
    "LPZRHAZ"                , "LA PAZ REGIONAL HOSPITAL"           , "AZ"   , ""                                                                                     ,
    "LQSNATX"                , "LA QUINTA INN BY WYNDHAM SNA"       , "TX"   , ""                                                                                     ,
    "LRDMCTX"                , "LAREDO MEDICAL CENTER"              , "TX"   , ""                                                                                     ,
    "LSSTFNY"                , "LUTHERAN SOC SVCS OF NY TFC"        , "NY"   , ""                                                                                     ,
    "LTJMSFL"                , "LORRAINE THOMAS SHELTER"            , "FL"   , ""                                                                                     ,
    "LUTCSWA"                , "LUTHERAN COMMUNTIY SERVICES NW"     , "WA"   , ""                                                                                     ,
    "LUTHEPA"                , "LUTHERAN CHILDEREN AND FAMILY SVC"  , "PA"   , ""                                                                                     ,
    "LUTSSMI"                , "LUTHERAN SOC SERV, LANSIN"          , "MI"   , ""                                                                                     ,
    "LVYHOPA"                , "LEHIGH VALLEY HOSPITAL - POCONO"    , "PA"   , ""                                                                                     ,
    "MACIPLY"                , "MCI PLYMOUTH S CARVER"              , "MA"   , ""                                                                                     ,
    "MADISTN"                , "MADISON COUNTY JAIL"                , "TN"   , ""                                                                                     ,
    "MARIOFL"                , "MARION COUNTY JAIL"                 , "FL"   , ""                                                                                     ,
    "MARTNTX"                , "MARTIN COUNTY JAIL"                 , "TX"   , ""                                                                                     ,
    "MASONTN"                , "TOWN OF MASON"                      , "TN"   , ""                                                                                     ,
    "MCHALMN"                , "MAYO CLINIC HEALTH SYS ALBERT LEA"  , "MN"   , ""                                                                                     ,
    "MCLEOMN"                , "MCLEOD COUNTY JAIL, MN"             , "MN"   , ""                                                                                     ,
    "MDLNDNE"                , "CHI HEALTH MIDLANDS HOSPITAL"       , "NE"   , ""                                                                                     ,
    "MEHOSCA"                , "MERCY HOSP DOWNTOWN BAKERSFIELD"    , "CA"   , ""                                                                                     ,
    "MEMCRCA"                , "MERCY MEDICAL CTR REDDING"          , "CA"   , ""                                                                                     ,
    "MEMHOCA"                , "MEMORIAL HOSPITAL BAKERSFIELD"      , "CA"   , ""                                                                                     ,
    "MERCYFL"                , "MERCY HOSPITAL"                     , "FL"   , ""                                                                                     ,
    "MESJUCO"                , "GRAND MESA YOUTH SVCS"              , "CO"   , ""                                                                                     ,
    "MHCHSMI"                , "MUNSON HEALTHCARE CADILLAC HOSP"    , "MI"   , ""                                                                                     ,
    "MHNSATX"                , "METHODIST HOSPITAL NORTHEAST"       , "TX"   , ""                                                                                     ,
    "MHRHOPA"                , "MILTON S. HERSHEY MEDICAL CENTER"   , "PA"   , ""                                                                                     ,
    "MHSOSTX"                , "METHODIST HOSPITAL STONE OAK"       , "TX"   , ""                                                                                     ,
    "MHTFHOH"                , "MERCY HEALTH - TIFFIN HOSPITAL"     , "OH"   , ""                                                                                     ,
    "MIDMHTX"                , "MIDLAND MEMORIAL HOSPITAL"          , "TX"   , ""                                                                                     ,
    "MIREFIO"                , "MI REFORMATORY,IONIA,MI"            , "MI"   , ""                                                                                     ,
    "MIRHPRI"                , "MIRIAM HOSPITAL"                    , "RI"   , ""                                                                                     ,
    "MLPHHMI"                , "MCLAREN PORT HURON HOSP"            , "MI"   , ""                                                                                     ,
    "MMCCRIA"                , "MERCY MED CTR CEDAR RAPID"          , "IA"   , ""                                                                                     ,
    "MMMCSMI"                , "MYMICHIGAN MEDICAL CENTER SAULT"    , "MI"   , ""                                                                                     ,
    "MONROIA"                , "MONROE COUNTY JAIL"                 , "IA"   , ""                                                                                     ,
    "MONROIL"                , "MONROE COUNTY JAIL"                 , "IL"   , ""                                                                                     ,
    "MRMCSCA"                , "MARIAN REGIONAL MEDICAL CENTER"     , "CA"   , ""                                                                                     ,
    "MRRADOR"                , "MORRISON CENTER-SECURE"             , "OR"   , ""                                                                                     ,
    "MRREGVA"                , "MIDDLE RIVER REGIONAL JAIL"         , "VA"   , ""                                                                                     ,
    "MRRSNOR"                , "MORRISON CENTER"                    , "OR"   , ""                                                                                     ,
    "MRYHOMO"                , "MERCY HOSPITAL SOUTH"               , "MO"   , ""                                                                                     ,
    "MRYHPMN"                , "MERCY HOSPITAL"                     , "MN"   , ""                                                                                     ,
    "MSKGNMI"                , "MUSKEGON COUNTY JAIL"               , "MI"   , ""                                                                                     ,
    "MTHOSTX"                , "METHODIST HOSPITAL SOUTH"           , "TX"   , ""                                                                                     ,
    "MTNITPA"                , "MOUNT NITTANY MEDICAL CENTER"       , "PA"   , ""                                                                                     ,
    "MVJUVCO"                , "MONTE VIEW JUVENILE FACIL"          , "CO"   , ""                                                                                     ,
    "MVMCMAZ"                , "MOUNTAIN VISTA MED CTR"             , "AZ"   , ""                                                                                     ,
    "MYOMKMN"                , "MAYO CLINIC HEALTH SYS - MANKATO"   , "MN"   , ""                                                                                     ,
    "NASTRNY"                , "NASSAU COUNTY ICE TRAILER"          , "NY"   , ""                                                                                     ,
    "NCBHOTX"                , "NORTH CENTRAL BAPTIST HOSPITAL"     , "TX"   , ""                                                                                     ,
    "NEBHSTX"                , "NORTHEAST BAPTIST HOSPITAL"         , "TX"   , ""                                                                                     ,
    "NFMIAFL"                , "NEIGHBOR TO FAMILY MIA"             , "FL"   , ""                                                                                     ,
    "NFMIGFL"                , "NEIGHBOR-FAMILY MIAMI GARDENS"      , "FL"   , ""                                                                                     ,
    "NFSMTGA"                , "NEIGHBOR-FAMILY, STONE MOUNTAIN"    , "GA"   , ""                                                                                     ,
    "NFUACGA"                , "NEIGHBOR-FAMILY UAC PGM, ATL"       , "GA"   , ""                                                                                     ,
    "NLSCOLA"                , "NELSON COLEMAN CORRECTIONS CENTER"  , "LA"   , ""                                                                                     ,
    "NMCENCF"                , "CENTRAL N.M.C.F.LOS LUNAS"          , "NM"   , ""                                                                                     ,
    "NMCOMNE"                , "NEBRASKA MED CTR OMAHA"             , "NE"   , ""                                                                                     ,
    "NMGRANT"                , "WESTERN N.M.C.F,GRANTS"             , "NM"   , ""                                                                                     ,
    "NNVMCNV"                , "NORTHERN NEVADA MED CTR"            , "NV"   , ""                                                                                     ,
    "NRLKCMI"                , "NORTH LAKE CORRECTIONAL FACILITY"   , "MI"   , ""                                                                                     ,
    "NUMCENY"                , "NASSAU UNIV MED CTR"                , "NY"   , ""                                                                                     ,
    "NYADIRC"                , "ADIRONDACK CF, RAYBROOK"            , "NY"   , ""                                                                                     ,
    "NYWASHC"                , "WASHINGTON CORRECTIONAL"            , "NY"   , ""                                                                                     ,
    "OHNORWE"                , "CORRECTIONS CENTER OF NW OHIO"      , "OH"   , ""                                                                                     ,
    "OKCIMMC"                , "CIMARRON CORRECTIONAL FACILITY"     , "OK"   , "https://www.corecivic.com/facilities/cimarron-facility"                               ,
    "OKMHKTX"                , "OTTO KAISER MEMORIAL HOSPITAL"      , "TX"   , ""                                                                                     ,
    "OSCEOFL"                , "OSCEOLA COUNTY JAIL"                , "FL"   , ""                                                                                     ,
    "OTTERMN"                , "OTTER TAIL COUNTY JAIL"             , "MN"   , ""                                                                                     ,
    "OZARKMO"                , "OZARK COUNTY SHERIFF'S OFFICE"      , "MO"   , ""                                                                                     ,
    "PAMSATX"                , "PAM HEALTH SPECIALTY HOSPITAL 1"    , "TX"   , ""                                                                                     ,
    "PBDPDMA"                , "PEABODY PD"                         , "MA"   , ""                                                                                     ,
    "PCRMMGA"                , "PIEDMOUNT COLUMBUS REG MIDTOWN MC"  , "GA"   , ""                                                                                     ,
    "PELCNTX"                , "PELICAN ISLAND CENTER"              , "TX"   , ""                                                                                     ,
    "PENDRNC"                , "PENDER COUNTY JAIL"                 , "NC"   , ""                                                                                     ,
    "PHELPMO"                , "PHELPS COUNTY JAIL"                 , "MO"   , ""                                                                                     ,
    "PHELSMO"                , "PHELPS HEALTH HOSPITAL"             , "MO"   , ""                                                                                     ,
    "PHOSMGA"                , "PHOEBE SUMTER MED CTR"              , "GA"   , ""                                                                                     ,
    "PHPMHGA"                , "PHOEBE PUTNEY MEMORIAL HOSP"        , "GA"   , ""                                                                                     ,
    "PHRCENJ"                , "PLAZA HEALTHCARE REHAB CENTER"      , "NJ"   , ""                                                                                     ,
    "PIERCND"                , "PIERCE COUNTY JAIL"                 , "ND"   , ""                                                                                     ,
    "PKCNTPA"                , "PIKE COUNTY JAIL"                   , "PA"   , ""                                                                                     ,
    "PKLDHTX"                , "PARKLAND HOSPITAL"                  , "TX"   , ""                                                                                     ,
    "PKMEDIN"                , "PARKVIEW REGIONAL MEDICAL CENTER"   , "IN"   , ""                                                                                     ,
    "PMMCAHI"                , "PALI MOMI MEDICAL CENTER"           , "HI"   , ""                                                                                     ,
    "PMMRHMI"                , "PROMEDICA MONROE REG HOSP"          , "MI"   , ""                                                                                     ,
    "POTTRTX"                , "POTTER COUNTY JAIL"                 , "TX"   , ""                                                                                     ,
    "PRACFMN"                , "PRAIRIE CORRECT. FACILITY"          , "MN"   , ""                                                                                     ,
    "PRVMDKS"                , "PROVIDENCE MEDICAL GROUP"           , "KS"   , ""                                                                                     ,
    "PRYCOAL"                , "PERRY COUNTY JAIL"                  , "AL"   , ""                                                                                     ,
    "PTISATX"                , "PEAR TREE INN SNA AIRPORT"          , "TX"   , ""                                                                                     ,
    "PUBLIIA"                , "PUBLIC SAFETY CENTER"               , "IA"   , ""                                                                                     ,
    "PUTMAFL"                , "PUTMAN COUNTY JAIL"                 , "FL"   , ""                                                                                     ,
    "PUTMAFL"                , "PUTNAM COUNTY JAIL"                 , "FL"   , ""                                                                                     ,
    "PUTNMTN"                , "PUTNAM COUNTY SHERIFF"              , "TN"   , ""                                                                                     ,
    "PVHOSIN"                , "PARKVIEW HOSPITAL RANDALLIA"        , "IN"   , ""                                                                                     ,
    "RAINBGA"                , "RAINBOW HOUSE"                      , "GA"   , ""                                                                                     ,
    "RAMEPTX"                , "RAMADA BY WYNDHAM EL PASO"          , "TX"   , ""                                                                                     ,
    "RDHEATX"                , "RADISSON HTL ELP AIRPORT"           , "TX"   , ""                                                                                     ,
    "RDHOSPA"                , "READING HOSPITAL"                   , "PA"   , ""                                                                                     ,
    "RDRVHTX"                , "RED RIVER HOSPITAL"                 , "TX"   , ""                                                                                     ,
    "RENVIMN"                , "RENVILLE COUNTY JAIL"               , "MN"   , ""                                                                                     ,
    "RESIDTX"                , "RESIDENCE INN DALLAS AP"            , "TX"   , ""                                                                                     ,
    "RIHPRRI"                , "RHODE ISLAND HOSPITAL"              , "RI"   , ""                                                                                     ,
    "RMWYPAZ"                , "RAMADA BY WYNDHAM PHO"              , "AZ"   , ""                                                                                     ,
    "ROCGHNY"                , "ROCHESTER GENERAL HOSPITAL"         , "NY"   , ""                                                                                     ,
    "RVBHHTX"                , "RIO VISTA BEHV HLTH HSP"            , "TX"   , ""                                                                                     ,
    "SANPEUT"                , "SANPETE COUNTY JAIL"                , "UT"   , ""                                                                                     ,
    "SAUKCWI"                , "SAUK COUNTY SHERIFF"                , "WI"   , ""                                                                                     ,
    "SBNTOTX"                , "SAN BENITO POLICE DEPT."            , "TX"   , ""                                                                                     ,
    "SCHLETX"                , "SCHLEICHER COUNTY JAIL"             , "TX"   , ""                                                                                     ,
    "SDSPSID"                , "SD STATE PENN SIOUX FALLS"          , "SD"   , ""                                                                                     ,
    "SHELBIA"                , "SHELBY COUNTY JAIL, IOWA"           , "IA"   , ""                                                                                     ,
    "SHMCHNV"                , "SUMMERLIN HOSPITAL MEDICAL CENTER"  , "NV"   , ""                                                                                     ,
    "SHNMCTX"                , "SHANNON MEDICAL CENTER"             , "TX"   , ""                                                                                     ,
    "SJUANTX"                , "SAN JUAN POLICE DEPT."              , "TX"   , ""                                                                                     ,
    "SKIPCOR"                , "SKIPWORTH JUV. DETENTION"           , "OR"   , ""                                                                                     ,
    "SLBHSTX"                , "ST LUKE'S BAPTIST HOSPITAL"         , "TX"   , ""                                                                                     ,
    "SLHTWTX"                , "ST LUKE'S HEALTH - THE WOODLANDS"   , "TX"   , ""                                                                                     ,
    "SLINPAZ"                , "SLEEP INN PHOENIX"                  , "AZ"   , ""                                                                                     ,
    "SLJUVCA"                , "SAN LUIS OBISPO CO. JUVEN"          , "CA"   , ""                                                                                     ,
    "SMNOLFL"                , "SEMINOLE COUNTY JAIL"               , "FL"   , ""                                                                                     ,
    "SMRVLTX"                , "SOMERVELL COUNTY SHERIFF'S DEPT."   , "TX"   , ""                                                                                     ,
    "SNTRHNY"                , "SUNRISE HOSPITAL"                   , "NY"   , ""                                                                                     ,
    "SONMACA"                , "SONOMA CO MAIN ADULT DET"           , "CA"   , ""                                                                                     ,
    "SP8WWTX"                , "SUPER 8 WYNDHAM WESLACO"            , "TX"   , ""                                                                                     ,
    "SPIPDTX"                , "SOUTH PADRE ISLAND POLICE DEPT."    , "TX"   , ""                                                                                     ,
    "SPRHONY"                , "SPRING VALLEY HOSPITAL"             , "NY"   , ""                                                                                     ,
    "SROSAFL"                , "SANTA ROSA COUNTY JAIL"             , "FL"   , ""                                                                                     ,
    "SSHBWTX"                , "SURESTAY HOTEL BEST WEST"           , "TX"   , ""                                                                                     ,
    "SSHNHNY"                , "SOUTHSIDE HOSP NORTHWELL HLTH"      , "NY"   , ""                                                                                     ,
    "SSHSWMA"                , "SOUTH SHORE HOSPITAL"               , "MA"   , ""                                                                                     ,
    "SSPBAAZ"                , "STAYBRIDGE STE PHO  B A"            , "AZ"   , ""                                                                                     ,
    "STCHOVA"                , "SENTARA CAREPLEX HOSPITAL"          , "VA"   , ""                                                                                     ,
    "STCHRLA"                , "ST CHARLES PARISH JAIL"             , "LA"   , ""                                                                                     ,
    "STELHOH"                , "ST ELIZABETH YOUNGSTOWN HOSPITAL"   , "OH"   , ""                                                                                     ,
    "STFRHGA"                , "ST FRANCIS HOSPITAL"                , "GA"   , ""                                                                                     ,
    "STGENMO"                , "STE. GENEVIEVE COUNTY SHERIFF/JAIL" , "MO"   , ""                                                                                     ,
    "STJHCCA"                , "ST JOHN'S HOSPITAL CAMARILLO"       , "CA"   , ""                                                                                     ,
    "STJOSCO"                , "SAINT JOSEPH HOSPITAL"              , "CO"   , ""                                                                                     ,
    "STJOSMN"                , "ST. JOSEPHS, MN FOR KIDS"           , "MN"   , ""                                                                                     ,
    "STLUCFL"                , "ST. LUCIE COUNTY JAIL"              , "FL"   , ""                                                                                     ,
    "STRMONY"                , "STRONG MEMORIAL HOSPITAL"           , "NY"   , ""                                                                                     ,
    "SUPHDTX"                , "SUPER 8 BY WYNDHAM"                 , "TX"   , ""                                                                                     ,
    "SUTMCCA"                , "SUTTER MED CTR SACRAMENTO"          , "CA"   , ""                                                                                     ,
    "SWKBRTX"                , "SOUTHWEST KEY #946"                 , "TX"   , ""                                                                                     ,
    "SWKCBTX"                , "SOUTHWEST KEY COMBES FACILITY"      , "TX"   , ""                                                                                     ,
    "SWYOUIA"                , "SW IOWA YOUTH EMERG.SVCES"          , "IA"   , ""                                                                                     ,
    "TACGHWA"                , "TACOMA GENERAL HOSPITAL"            , "WA"   , ""                                                                                     ,
    "TAKARTX"                , "TRUSTED ADULT KARNES FSC"           , "TX"   , ""                                                                                     ,
    "TASTDTX"                , "TRUSTED ADULT SOUTH TEX DILLEY FSC" , "TX"   , ""                                                                                     ,
    "TBRJLME"                , "TWO BRIDGES REGIONAL JAIL"          , "ME"   , ""                                                                                     ,
    "TEMPDTX"                , "TEMPLE P.D."                        , "TX"   , ""                                                                                     ,
    "THAYENE"                , "THAYER COUNTY JAIL"                 , "NE"   , ""                                                                                     ,
    "THHHBTX"                , "TEX HEALTH HUGULEY HOSP"            , "TX"   , ""                                                                                     ,
    "THPMCTX"                , "THOP MEMORIAL CAMPUS ELP"           , "TX"   , ""                                                                                     ,
    "THPSCTX"                , "THOP SIERRA CAMPUS ELP"             , "TX"   , ""                                                                                     ,
    "TRAIDIL"                , "TRAVELERS & IMMIGRANT AID"          , "IL"   , ""                                                                                     ,
    "TRVLIAZ"                , "TRAVELER'S INN - PHOENIX"           , "AZ"   , ""                                                                                     ,
    "TRYOSCA"                , "TRINITY YOUTH SERVICES LTFC"        , "CA"   , ""                                                                                     ,
    "TWSJUAZ"                , "TUMBLEWEED - DESERT COVE"           , "AZ"   , ""                                                                                     ,
    "UBRKHWV"                , "WVU MEDICINE BERKELEY MEDICAL CNTR" , "WV"   , ""                                                                                     ,
    # "UCBPMCA"                , "CBP MOVEMENT COORDINATION AREA"     , ""     , "?"                                                                                ,
    "UCCHIIL"                , "UCHICAGO MEDICINE HOSPITAL CHICAGO" , "IL"   , ""                                                                                     ,
    "UCHRVIL"                , "UCHICAGO MEDICINE INGALLS MEMORIAL" , "IL"   , ""                                                                                     ,
    "UHGMCOH"                , "UNIV HOSP GEAUGA MED CTR"           , "OH"   , ""                                                                                     ,
    "UHOSPIN"                , "UNION HOSPITAL"                     , "IN"   , ""                                                                                     ,
    "UHSHONY"                , "UHS WILSON MEDICAL CENTER"          , "NY"   , ""                                                                                     ,
    "UMASSMA"                , "UMASS MEMORIAL MEDICAL CENTER"      , "MA"   , ""                                                                                     ,
    "UMCLVNV"                , "UNIVERSITY MEDICAL CENTER"          , "NV"   , ""                                                                                     ,
    "UMEMCNY"                , "UNITED MEMORIAL MED CTR"            , "NY"   , ""                                                                                     ,
    "UMHMEPA"                , "UNITED METHODIST HOME FOR CHILDREN" , "PA"   , ""                                                                                     ,
    "UNIHONY"                , "UNITY HOSPITAL"                     , "NY"   , ""                                                                                     ,
    "UNJUVNJ"                , "COUNTY OF UNION JUV DET CENTER"     , "NJ"   , ""                                                                                     ,
    "UNMEDMD"                , "UNIV OF MARYLAND MEDICAL CENTER"    , "MD"   , ""                                                                                     ,
    "UPMCAPA"                , "UPMC ALTOONA"                       , "PA"   , ""                                                                                     ,
    "UPMCPPA"                , "UPMC PRESBYTERIAN HOSPITAL"         , "PA"   , ""                                                                                     ,
    "UPSLSIA"                , "UNITYPOINT HLTH ST LUKES HOSP SXC"  , "IA"   , ""                                                                                     ,
    "URSLATX"                , "URSULA CENTRALIZED PROCESSING CNTR" , "TX"   , ""                                                                                     ,
    "UVACVVA"                , "UVA UNIV Hospital Center"           , "VA"   , ""                                                                                     ,
    "VALEMTX"                , "VAL VERDE EMERGENCY SHELT."         , "TX"   , ""                                                                                     ,
    "VCUHOVA"                , "VCU HEALTH TAPPAHANNOCK HOSPITAL"   , "VA"   , ""                                                                                     ,
    "VNTNBCA"                , "VENTURA NAVAL BASE"                 , "CA"   , ""                                                                                     ,
    "VWHMCAZ"                , "VALLEYWISE HEALTH MED CTR"          , "AZ"   , ""                                                                                     ,
    "WALTNFL"                , "WALTON COUNTY D.O.C."               , "FL"   , ""                                                                                     ,
    "WARRENJ"                , "WARREN COUNTY JAIL"                 , "NJ"   , ""                                                                                     ,
    "WARRENY"                , "WARREN COUNTY JAIL"                 , "NY"   , ""                                                                                     ,
    "WAYOUMI"                , "WAYNE COUNTY YOUTHT.MI"             , "MI"   , ""                                                                                     ,
    "WKBHMFL"                , "WEST KENDALL BAPTIST HOSP"          , "FL"   , ""                                                                                     ,
    "WLTSOFL"                , "WALTON COUNTY JAIL"                 , "FL"   , ""                                                                                     ,
    "WMDBHFL"                , "WEST MIAMI DADE BAPT HEALTH EMERG"  , "FL"   , ""                                                                                     ,
    "WOODFKY"                , "WOODFORD COUNTY SHERIFF/JAIL"       , "KY"   , ""                                                                                     ,
    "WRMEDWI"                , "WATERTOWN REGIONAL MEDICAL CENTER"  , "WI"   , ""                                                                                     ,
    "WVPTMAC"                , "POTOMAC HIGHLANDS REGIONAL JAIL"    , "WV"   , ""                                                                                     ,
    "WWDHDNH"                , "WENTWORTH DOUGLAS HOSPITAL"         , "NH"   , ""                                                                                     ,
    "YRMDCAZ"                , "YUMA REGIONAL MED CTR"              , "AZ"   , ""                                                                                     ,

    # new ones
    "ALLEGVA"                , ""                                   , "VA"   , "https://alleghanycountysheriff.com/inmate-information/"                               ,
    "AMHERVA"                , ""                                   , "VA"   , "https://brrja.state.va.us/acadc-2/"                                                   ,
    "LYNRJVA"                , ""                                   , "VA"   , "https://brrja.state.va.us/ladc-2/"                                                    ,
    # "UCBPMCA"              ,""  , ""                                        ,
    "CBPORIL"                , ""                                   , "IL"   , "https://www.cbp.gov/about/contact/ports/chicago-illinois-3901"                        ,
    "CHTHMGA"                , ""                                   , "GA"   , "https://gdc.georgia.gov/locations/chatham-county-jail"                                ,
    "OKDBACK"                , ""                                   , "OK"   , "https://www.ice.gov/detain/detention-facilities/diamondback-correctional-facility"    ,
    "DILLSAF"                , ""                                   , "TX"   , "https://www.ice.gov/detain/detention-facilities/dilley-immigration-processing-center" ,
    "EROFCB"                 , ""                                   , "TX"   , "https://www.ice.gov/detain/detention-facilities/camp-east-montana"                    ,
    "CHSHOLD"                , ""                                   , "WV"   , "in name"                                                                              ,
    "FULCJIN"                , ""                                   , "IN"   , "https://co.fulton.in.us/235/Jail-Division"                                            ,
    "KFMANTX"                , ""                                   , "TX"   , "https://www.kaufmanso.com/jail-info/inmate-records/"                                  ,
    "BOPLAT"                 , ""                                   , "TX"   , "https://www.bop.gov/locations/institutions/lat/"                                      ,
    "LEONCFL"                , ""                                   , "FL"   , "https://www.leoncountyso.com/About-us/Departments/Detention-Facility/Inmate-search"   ,
    "LBRTYFL"                , ""                                   , "FL"   , "https://www.ice.gov/detain/detention-facilities/liberty-county-sheriffs-office"       ,
    "NEMCCOI"                , ""                                   , "NE"   , "https://www.ice.gov/detain/detention-facilities/mccook-detention-center"              ,
    "NATJVWY"                , ""                                   , "WY"   , "https://www.natronacounty-wy.gov/193/Juvenile-Detention-Center"                       ,
    "WVNCENT"                , ""                                   , "WV"   , "https://dcr.wv.gov/facilities/pages/prisons-and-jails/ncrjcf.aspx"                    ,
    "ONSLONC"                , ""                                   , "NC"   , "https://www.onslowcountync.gov/1318/Detention-Division"                               ,
    # "SAVHOLD"   Savannah Ho,""ld Room             , ""                                 , "unknown"                                                                                    ,
    "WVSOUTH"                , ""                                   , "WV"   , "https://dcr.wv.gov/facilities/Pages/prisons-and-jails/srjcf.aspx"                     ,
    "WVSWEST"                , ""                                   , "WV"   , "https://dcr.wv.gov/facilities/Pages/prisons-and-jails/swrjcf.aspx"                    ,
    "WVWESTR"                , ""                                   , "WV"   , "https://dcr.wv.gov/facilities/Pages/prisons-and-jails/wrjcf.aspx"                     ,
    # "XXWICHI"     Wichita County Jail           , "TX"                                 , "—"
    # State-only stub (name lives in 4-name-code-match.R so the matcher's
    # lookup sees it; this tribble does select(-name)).
    "XXMRRCK"                , ""                                   , "KS"   , "https://www.ice.gov/detain/detention-facilities (Leavenworth; no ICE code yet)"
  ) |>
  select(-name)

name_state_match_manual <-
  bind_rows(
    code_name_state_unambiguous_short_manual,
    hold_room_state_match,
    code_name_state_manual
  ) |>
  select(-notes) |>
  mutate(source = "ddp-manual", date = as.Date("2026-01-01"))

name_state_match <-
  bind_rows(
    name_code_state_match_from_data,
    name_state_match_manual
  )

arrow::write_parquet(
  name_state_match,
  "data/facilities-name-state-match.parquet"
)
