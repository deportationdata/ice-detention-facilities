library(tidyverse)
library(tidylog)

source("code/functions.R")

facility_attributes <- arrow::read_feather(
  "data/facilities-attributes-raw.feather"
)

hospital_list_full <-
  arrow::read_feather("data/hospitals.feather")

hospital_list <-
  hospital_list_full |>
  distinct(name, state) |>
  mutate(name_join = clean_text(name))

name_state_match <- arrow::read_feather(
  "data/facilities-name-state-match.feather"
)

facilities_from_detentions <-
  arrow::read_feather(
    "data/facilities-from-detentions.feather"
  ) |>
  mutate(name_join = clean_text(detention_facility)) |>
  filter(
    str_detect(
      name_join,
      "hosp|medicine|\\bnurs\\b|nursing|medical|\\bmed\\b|health|clinic|emerg|kaiser|hca|surgic|surger|orthop|spine|shriners|transplant|hlth|mayo|infirmary|wellness|physician|ambulatory|\\beye\\b|medstar"
    ) |
      name_join %in% c("samaritan behavioral center", "Scan inc")
  ) |>
  select(detention_facility_code, name = detention_facility, name_join) |>
  filter(!str_detect(detention_facility_code, "^BOP")) |>
  left_join(
    name_state_match |> select(detention_facility_code, state),
    by = c("detention_facility_code")
  )

# library(fuzzylink)

# matches_cleaned <-
#   fuzzylink::fuzzylink(
#     facilities_from_detentions,
#     hospital_list,
#     record_type = "names of medical facilities",
#     model = "gpt-3.5-turbo-instruct",
#     learner = "ranger",
#     instructions = "compare names of detention facilities to a list of hospitals and medical centers to find possible matches",
#     by = "name_join",
#     blocking.variables = "state",
#     max_labels = 50000,
#     openai_api_key = "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"
#   )

# write_rds(matches_cleaned, file = "~/downloads/matches_cleaned_hospitals.rds")

matches_cleaned <- read_rds("~/downloads/matches_cleaned_hospitals.rds")

matches_cleaned |>
  as_tibble() |>
  filter(match == "Yes", match_probability >= 0.75) |>
  transmute(
    name.x,
    name.y,
    state,
    detention_facility_code,
    match_probability = round(match_probability, 2)
  ) |>
  arrange(state, name.x) |>
  clipr::write_clip()

# Verification instructions:
# it is ok to have more than one match for a single name.x facility (detention facilities) -- the name.y comes from multiple sources and we want to link to all of them
# delete any matches that are CLEARLY wrong
# for ones where there are multiple possible matches and you're unsure, comment them ALL out then do research to figure out which is right
# if you figure out one that's commented, delete the wrong ones and uncomment the right one

matches_cleaned <-
  tribble(
    ~name.x                              , ~name.y                                                              , ~state , ~detention_facility_code , ~match_probability ,
    "BARTLETT REGIONAL HOSPITAL"         , "BARTLETT REGIONAL HOSPITAL"                                         , "AK"   , "BRLHOAK"                , "1"                ,
    "BANNER IRONWOOD MED CTR"            , "BANNER IRONWOOD MEDICAL CENTER"                                     , "AZ"   , "BIMDCAZ"                , "0.99"             ,
    "BANNER MEDICAL CTR TUCSON"          , "BANNER - UNIVERSITY MEDICAL CENTER TUCSON CAMPUS"                   , "AZ"   , "BNMCTAZ"                , "0.76"             ,
    # "BANNER UNIVERSITY MEDICAL PHOENIX"  , "BANNER - UNIVERSITY MEDICAL CENTER PHOENIX"                                    , "AZ"   , "BANHOAZ"                , "1"                , # likely correct bc no mention of south campus: https://www.ice.gov/news/releases/ethiopian-national-ice-custody-passes-away-phoenix-area-hospital
    # "BANNER UNIVERSITY MEDICAL PHOENIX"  , "BANNER-UNIVERSITY MEDICAL CENTER PHOENIX"                                      , "AZ"   , "BANHOAZ"                , "1"                , # keep
    # "BANNER UNIVERSITY MEDICAL PHOENIX"  , "BANNER-UNIVERSITY MEDICAL CENTER SOUTH CAMPUS"                                 , "AZ"   , "BANHOAZ"                , "0.93"             , # delete
    # "BANNER UNIVERSITY MEDICAL PHOENIX"  , "BANNER - UNIVERSITY MEDICAL CENTER SOUTH CAMPUS"                               , "AZ"   , "BANHOAZ"                , "0.93"             , # delete
    # "BANNER UNIVERSITY MEDICAL PHOENIX"  , "BANNER UNIVERSITY MEDICAL CENTER AT THE ARIZONA HEALTH SC"                     , "AZ"   , "BANHOAZ"                , "0.87"             , # delete
    "CHANDLER REG MED CTR"               , "CHANDLER REGIONAL MEDICAL CENTER"                                   , "AZ"   , "CHANRAZ"                , "0.97"             ,
    "FLORENCE HOSPITAL"                  , "FLORENCE HOSPITAL"                                                  , "AZ"   , "FLORHAZ"                , "1"                ,
    "LA PAZ REGIONAL HOSPITAL"           , "LA PAZ REGIONAL HOSPITAL"                                           , "AZ"   , "LPZRHAZ"                , "1"                ,
    "MOUNTAIN VISTA MED CTR"             , "HONORHEALTH MOUNTAIN VISTA MEDICAL CENTER"                                     , "AZ"   , "MVMCMAZ"                , "0.8"              ,
    "MOUNTAIN VISTA MED CTR"             , "MOUNTAIN VISTA MEDICAL CENTER, LP"                                             , "AZ"   , "MVMCMAZ"                , "0.86"             ,
    "ST MARY'S HOSPITAL"                 , "ST. MARY'S HOSPITAL"                                                , "AZ"   , "STMARAZ"                , "1"                ,
    "ST. JOSEPH'S HOSPITAL & MED. CEN."  , "ST JOSEPH'S HOSPITAL"                                                          , "AZ"   , "STJOSAZ"                , "0.84"             ,
    "ST. JOSEPH'S HOSPITAL & MED. CEN."  , "ST JOSEPHS HOSPITAL AND MEDICAL CENTER"                                        , "AZ"   , "STJOSAZ"                , "0.92"             ,
    "VALLEYWISE HEALTH MED CTR"          , "VALLEYWISE HEALTH MEDICAL CENTER"                                   , "AZ"   , "VWHMCAZ"                , "0.99"             ,
    "YUMA REGIONAL MED CTR"              , "YUMA REGIONAL MEDICAL CENTER"                                       , "AZ"   , "YRMDCAZ"                , "0.99"             ,
    "ADVENTIST HEALTH BAKERSFIELD"       , "ADVENTIST HEALTH BAKERSFIELD"                                       , "CA"   , "ADVHBCA"                , "1"                ,
    "ALHAMBRA BHC HOSPITAL"              , "BHC ALHAMBRA HOSPITAL"                                              , "CA"   , "BHCALCA"                , "0.91"             ,
    "ANAHEIM GLOBAL MEDICAL CENTER"      , "ANAHEIM GLOBAL MEDICAL CENTER"                                      , "CA"   , "AGMDCCA"                , "1"                ,
    "ARROWHEAD REGIONAL MED CENTER"      , "ARROWHEAD REGIONAL MEDICAL CENTER"                                  , "CA"   , "ARRMCCA"                , "1"                ,
    "CONTRA COSTA REG MED CTR"           , "CONTRA COSTA REGIONAL MEDICAL CENTER"                               , "CA"   , "CCRMCCA"                , "0.93"             ,
    "COSTA MESA COLLEGE HOSP"            , "COLLEGE HOSPITAL COSTA MESA"                                        , "CA"   , "COSTACA"                , "0.9"              ,
    "DESERT VALLEY HOSPITAL"             , "DESERT VALLEY HOSPITAL"                                             , "CA"   , "DSRTVCA"                , "1"                ,
    "EL CENTRO REGIONAL MED CTR"         , "EL CENTRO REGIONAL MEDICAL CENTER"                                  , "CA"   , "ECRMCCA"                , "1"                ,
    # "ENLOE MEDICAL CENTER"               , "ENLOE HEALTH"                                                                  , "CA"   , "ENLMCCA"                , "0.82"             , # here is a link to multiple locations but i'm still unsure: https://www.enloe.org/locations/
    # "ENLOE MEDICAL CENTER"               , "ENLOE MEDICAL CENTER - COHASSET"                                               , "CA"   , "ENLMCCA"                , "0.91"             ,
    # "ENLOE MEDICAL CENTER"               , "ENLOE MEDICAL CENTER - ESPLANADE"                                              , "CA"   , "ENLMCCA"                , "0.97"             ,
    "GOOD SAMARITAN HOSPITAL"            , "GOOD SAMARITAN HOSPITAL"                                            , "CA"   , "GSAMHCA"                , "1"                ,
    "KERN MEDICAL HOSPITAL"              , "KERN MEDICAL CENTER"                                                           , "CA"   , "KERNHCA"                , "0.86"             ,
    "LOMA LINDA UNIVERSITY MED CENTER"   , "LOMA LINDA UNIVERSITY MEDICAL CENTER"                                          , "CA"   , "LLUMCCA"                , "1"                ,
    "LOMA LINDA UNIVERSITY MED CENTER"   , "LOMA LINDA UNIVERSITY MEDICAL"                                                 , "CA"   , "LLUMCCA"                , "0.9"              ,
    "MARIAN REGIONAL MEDICAL CENTER"     , "MARIAN REGIONAL MEDICAL CENTER"                                     , "CA"   , "MRMCSCA"                , "1"                ,
    "MEMORIAL HOSPITAL BAKERSFIELD"      , "BAKERSFIELD MEMORIAL HOSPITAL"                                      , "CA"   , "MEMHOCA"                , "0.94"             ,
    "MERCY MEDICAL CTR REDDING"          , "MERCY MEDICAL CENTER REDDING"                                       , "CA"   , "MEMCRCA"                , "1"                ,
    "ORANGE CO GLOBAL MEDICAL CENTER"    , "ORANGE COUNTY GLOBAL MEDICAL CENTER"                                , "CA"   , "OGCMCCA"                , "0.97"             ,
    "PALMDALE REGIONAL MEDICAL CENTER"   , "PALMDALE REGIONAL MEDICAL CENTER"                                   , "CA"   , "PLRMCCA"                , "1"                ,
    "PARADISE VALLEY HOSPITAL"           , "PARADISE VALLEY HOSPITAL"                                           , "CA"   , "PVLYHCA"                , "1"                ,
    # "SAN DIEGO AREA HOSPITAL"            , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED CTR"                             , "CA"   , "SDHOSCA"                , "0.8"              , # keep
    # "SAN DIEGO AREA HOSPITAL"            , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MEDICAL CENTER"                      , "CA"   , "SDHOSCA"                , "0.9"              , # keep
    # "SAN DIEGO AREA HOSPITAL"            , "PROMISE HOSPITAL OF SAN DIEGO"                                                 , "CA"   , "SDHOSCA"                , "0.86"             , # delete? seems like it's closed: https://www.molinahealthcare.com/~/media/Molina/PublicWebsite/PDF/providers/ca/Duals/JTF-04-03-2017-PROMISE-HOSPITAL-OF-SAN-DIEGO-CLOSED-SD.pdf
    "SHARP CHULA VISTA MEDICAL CENTER"   , "SHARP CHULA VISTA MEDICAL CENTER"                                   , "CA"   , "SCVMCCA"                , "1"                ,
    "ST JOHN'S HOSPITAL CAMARILLO"       , "ST. JOHN'S HOSPITAL CAMARILLO"                                      , "CA"   , "STJHCCA"                , "1"                ,
    "ST MARY MEDICAL CENTER"             , "ST MARY MEDICAL CENTER"                                             , "CA"   , "STMARCA"                , "1"                ,
    "ST MARY MEDICAL CENTER"             , "ST. MARY MEDICAL CENTER"                                            , "CA"   , "STMARCA"                , "1"                ,
    "SUTTER MED CTR SACRAMENTO"          , "SUTTER MEDICAL CENTER, SACRAMENTO"                                  , "CA"   , "SUTMCCA"                , "0.96"             ,
    "SUTTER MED CTR SACRAMENTO"          , "SUTTER MEDICAL CENTER - SACRAMENTO"                                 , "CA"   , "SUTMCCA"                , "0.96"             ,
    # "UC SAN DIEGO MEDICAL CENTER"        , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED CTR"                             , "CA"   , "UCSDMCA"                , "0.85"             , # couldn't find anything but seems like it's hillcrest
    # "UC SAN DIEGO MEDICAL CENTER"        , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MEDICAL CENTER"                      , "CA"   , "UCSDMCA"                , "0.84"             , # keep
    # "UC SAN DIEGO MEDICAL CENTER"        , "UC SAN DIEGO HEALTH LA JOLLA - JACOBS MEDICAL CENTER AND SULPIZIO CARDIOVASCU" , "CA"   , "UCSDMCA"                , "0.82"             , # delete? here's link to it anyway: https://healthlocations.ucsd.edu/san-diego/9300-campus-point-drive/10
    # "UNV CALIFORNIA SAN DIEGO HOSPITAL"  , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MED CTR"                             , "CA"   , "UCSDHCA"                , "0.89"             , # keep
    # "UNV CALIFORNIA SAN DIEGO HOSPITAL"  , "UC SAN DIEGO HEALTH HILLCREST - HILLCREST MEDICAL CENTER"                      , "CA"   , "UCSDHCA"                , "0.86"             , # keep
    "VICTOR VALLEY GLOBAL MED CENTER"    , "VICTOR VALLEY GLOBAL MEDICAL CENTER"                                , "CA"   , "VVGMCCA"                , "1"                ,
    # "COLORADO MENTAL HEALTH INSTITUTE"   , "COLORADO MENTAL HEALTH HOSPITAL IN PUEBLO"                                     , "CO"   , "COMHICO"                , "0.88"             , # "serves individuals with pending criminal charges": https://cdhs.colorado.gov/CMHHIP
    # "COLORADO MENTAL HEALTH INSTITUTE"   , "COLORADO MENTAL HEALTH HOSPITAL IN FORT LOGAN"                                 , "CO"   , "COMHICO"                , "0.87"             , # unsure which one it is but here is link for fort logan anyway: https://cdhs.colorado.gov/CMHHIFL
    # "COLORADO MENTAL HEALTH INSTITUTE"   , "COLORADO MENTAL HEALTH INSTITUTE AT FT LOGAN"                                  , "CO"   , "COMHICO"                , "0.94"             , # same as above
    # "COLORADO MENTAL HEALTH INSTITUTE"   , "COLORADO MENTAL HEALTH INSTITUTE AT PUEBLO - PSYCH"                            , "CO"   , "COMHICO"                , "0.9"              , # same as first
    "DENVER HEALTH MEDICAL CENTER"       , "DENVER HEALTH MEDICAL CENTER"                                       , "CO"   , "DNHMCCO"                , "1"                ,
    "HIGHLANDS BEHAVIORAL HEALTH"        , "HIGHLANDS BEHAVIORAL HEALTH SYSTEM"                                 , "CO"   , "HGHBECO"                , "0.98"             ,
    "SAINT JOSEPH HOSPITAL"              , "SAINT JOSEPH HOSPITAL"                                              , "CO"   , "STJOSCO"                , "1"                ,
    "BROWARD GENERAL MEDICAL CENTER"     , "BROWARD HEALTH MEDICAL CENTER"                                      , "FL"   , "BRGMCFL"                , "0.85"             ,
    "ED FRASER MEMORIAL HOSPITAL"        , "ED FRASER MEMORIAL HOSPITAL"                                        , "FL"   , "EFMHMFL"                , "1"                ,
    "FORT LAUDERDALE HOSPITAL"           , "FORT LAUDERDALE BEHAVIORAL HEALTH CENTER"                           , "FL"   , "FLHOSFL"                , "0.75"             ,
    "FT LAUDERDALE BEHAVOR HLTH CTR"     , "FORT LAUDERDALE BEHAVIORAL HEALTH CENTER"                           , "FL"   , "FLBHCFL"                , "0.93"             ,
    # "HCA FLORIDA KENDALL HOSPITAL"       , "HCA FLORIDA KENDALL HOSPITAL"                                       , "FL"   , "CKHOSFL"                , "1"                , # keep: https://www.hcafloridahealthcare.com/locations/kendall-hospital
    # "HCA FLORIDA KENDALL HOSPITAL"       , "WEST KENDALL BAPTIST HOSPITAL"                                      , "FL"   , "CKHOSFL"                , "0.85"             , # delete bc west kendall baptist hospital already exists below
    # "HCA FLORIDA KENDALL HOSPITAL"       , "KENDALL REGIONAL MEDICAL CENTER"                                    , "FL"   , "CKHOSFL"                , "0.81"             , # keep
    # "HCA FLORIDA MEMORIAL HOSPITAL"      , "HCA FLORIDA MERCY HOSPITAL"                                         , "FL"   , "HCAFMFL"                , "0.9"              , # not sure which one: https://www.hcafloridahealthcare.com/locations/mercy-hospital
    # "HCA FLORIDA MEMORIAL HOSPITAL"      , "HCA FLORIDA MEMORIAL HOSPITAL"                                      , "FL"   , "HCAFMFL"                , "1"                , # not sure which one: https://www.hcafloridahealthcare.com/locations/memorial-hospital
    "HENDRY REGIONAL MEDICAL CENTER"     , "HENDRY REGIONAL MEDICAL CENTER"                                     , "FL"   , "HNRMCFL"                , "1"                ,
    "JACKSON MEMORIAL HOSPITAL"          , "JACKSON MEMORIAL HOSPITAL"                                          , "FL"   , "JMHOSFL"                , "1"                ,
    "LAKESIDE MEDICAL CENTER"            , "LAKESIDE MEDICAL CENTER"                                            , "FL"   , "LAKMCFL"                , "1"                ,
    "LARKIN BEHAVIORAL HEALTH SVCS"      , "LARKIN COMMUNITY HOSPITAL BEHAVIORAL HEALTH SRVS"                   , "FL"   , "LRKBHFL"                , "0.9"              ,
    "LARKIN BEHAVIORAL HEALTH SVCS"      , "LARKIN COMMUNITY HOSPITAL BEHAVIORAL HEALTH SERVICES"               , "FL"   , "LRKBHFL"                , "0.9"              ,
    # "LARKIN HOSPITAL"                    , "LARKIN COMMUNITY HOSPITAL"                                          , "FL"   , "LRKNHFL"                , "0.85"             , # keep: https://www.ice.gov/news/releases/ice-detainee-passes-away-larkin-community-hospital
    # "LARKIN HOSPITAL"                    , "LARKIN COMMUNITY HOSPITAL BEHAVIORAL HEALTH SRVS"                   , "FL"   , "LRKNHFL"                , "0.83"             , # delete?
    # "LARKIN HOSPITAL"                    , "LARKIN COMMUNITY HOSPITAL BEHAVIORAL HEALTH SERVICES"               , "FL"   , "LRKNHFL"                , "0.85"             , # delete?
    "LOWER KEYS MEDICAL CENTER"          , "LOWER KEYS MEDICAL CENTER"                                          , "FL"   , "LRKMCFL"                , "1"                ,
    "MEMORIAL REGIONAL HOSP."            , "MEMORIAL REGIONAL HOSPITAL"                                         , "FL"   , "MEMRHFL"                , "0.99"             ,
    "MERCY HOSPITAL"                     , "MERCY HOSPITAL"                                                     , "FL"   , "MERCYFL"                , "1"                ,
    "NORTH BROWARD MEDICAL CENTER"       , "BROWARD HEALTH MEDICAL CENTER"                                      , "FL"   , "NBRMCFL"                , "0.88"             ,
    "PALMS WEST HOSPITAL"                , "HCA FLORIDA PALMS WEST HOSPITAL"                                    , "FL"   , "PWHOSFL"                , "0.79"             ,
    "PALMS WEST HOSPITAL"                , "PALMS WEST HOSPITAL"                                                , "FL"   , "PWHOSFL"                , "1"                ,
    # "UNIVERSITY OF MIAMI HOSPITAL"       , "UNIVERSITY OF MIAMI HOSPITAL AND CLINICS - BASCOM PALMER EYE INST"  , "FL"   , "UMIAHFL"                , "0.8"              , # unsure of which one it is: https://umiamihealth.org/en/bascom-palmer-eye-institute
    # "UNIVERSITY OF MIAMI HOSPITAL"       , "UNIVERSITY OF MIAMI HOSPITAL AND CLINICS - SYLVESTER COMPREHENSIVE" , "FL"   , "UMIAHFL"                , "0.78"             , # other option: https://umiamihealth.org/en/locations/sylvester-comprehensive-cancer-center
    # "UNIVERSITY OF MIAMI HOSPITAL"       , "UNIVERSITY OF MIAMI HOSPITAL AND CLINICS - UHEALTH TOWER"           , "FL"   , "UMIAHFL"                , "0.9"              , # confusing bc also there's an east and west tower: https://umiamihealth.org/en/locations/uhealth-tower
    # "WEST KENDALL BAPTIST HOSP"          , "BAPTIST HOSPITAL OF MIAMI"                                          , "FL"   , "WKBHMFL"                , "0.83"             , # doesn't seem to match the facility code, so probably wrong and we should delete: https://baptisthealth.net/locations/hospitals/baptist-hospital-of-miami
    # "WEST KENDALL BAPTIST HOSP"          , "BAPTIST HOSPITAL"                                                   , "FL"   , "WKBHMFL"                , "0.83"             , # same as above
    # "WEST KENDALL BAPTIST HOSP"          , "HCA FLORIDA KENDALL HOSPITAL"                                       , "FL"   , "WKBHMFL"                , "0.77"             , # delete bc hca florida kendall hospital already exists above
    # "WEST KENDALL BAPTIST HOSP"          , "WEST KENDALL BAPTIST HOSPITAL"                                      , "FL"   , "WKBHMFL"                , "1"                , # keep
    # "WEST MIAMI DADE BAPT HEALTH EMERG"  , "BAPTIST HEALTH HOSPITAL AT DORAL"                                   , "FL"   , "WMDBHFL"                , "0.78"             , # confused by this one
    "GRADY MEMORIAL HOSPITAL"            , "GRADY MEMORIAL HOSPITAL"                                            , "GA"   , "GMHATGA"                , "1"                ,
    "GRADY MEMORIAL HOSPITAL"            , "GRADY GENERAL HOSPITAL"                                             , "GA"   , "GMHATGA"                , "0.8"              ,
    "PHOEBE PUTNEY MEMORIAL HOSP"        , "PHOEBE PUTNEY MEMORIAL HOSPITAL"                                    , "GA"   , "PHPMHGA"                , "1"                ,
    "PHOEBE SUMTER MED CTR"              , "PHOEBE SUMTER MEDICAL CENTER"                                       , "GA"   , "PHOSMGA"                , "0.86"             ,
    "ST FRANCIS HOSPITAL"                , "ST FRANCIS HOSPITAL- EMORY HEALTHCARE"                              , "GA"   , "STFRHGA"                , "0.84"             ,
    "ST FRANCIS HOSPITAL"                , "ST FRANCIS HOSPITAL - EMORY HEALTHCARE"                             , "GA"   , "STFRHGA"                , "0.84"             ,
    # "GUAM MEMORIAL HOSPITAL"             , "GUAM MEMORIAL  HOSPITAL AUTHORITY"                                  , "GU"   , "GUAMHGU"                , "0.96"             , # keep: https://www.gmha.org/
    # "GUAM MEMORIAL HOSPITAL"             , "GUAM MEMORIAL HOSPITAL AUTHORITY"                                   , "GU"   , "GUAMHGU"                , "0.96"             , # keep
    # "GUAM MEMORIAL HOSPITAL"             , "NAVAL HOSPITAL GUAM"                                                , "GU"   , "GUAMHGU"                , "0.77"             , # delete?
    "ADVENTIST HEALTH CASTLE"            , "ADVENTIST HEALTH CASTLE"                                            , "HI"   , "ADVHCHI"                , "1"                ,
    "PALI MOMI MEDICAL CENTER"           , "PALI MOMI MEDICAL CENTER"                                           , "HI"   , "PMMCAHI"                , "1"                ,
    # "QUEEN'S MEDICAL CENTER"             , "THE QUEEN'S MEDICAL CENTER"                                         , "HI"   , "QUEMCHI"                , "0.95"             , # keep: https://www.queens.org/locations/hospitals/qmc/
    # "QUEEN'S MEDICAL CENTER"             , "THE QUEEN'S MEDICAL CENTER - HALE PULAMA MAU"                       , "HI"   , "QUEMCHI"                , "0.78"             , # this is a specific building but from my understanding, it's the same medical center
    "CHI HEALTH MERCY COUNCIL BLUFFS"    , "CHI HEALTH MERCY COUNCIL BLUFFS"                                    , "IA"   , "CHIHMIA"                , "1"                ,
    "MERCY MED CTR CEDAR RAPID"          , "MERCY MEDICAL CENTER - CEDAR RAPIDS"                                , "IA"   , "MMCCRIA"                , "0.85"             ,
    "UNITY POINT HLTH ST LUKES HOSP"     , "ST LUKES HOSPITAL"                                                  , "IA"   , "UPSLHIA"                , "0.85"             ,
    "UNITYPOINT HLTH ST LUKES HOSP SXC"  , "ST LUKES HOSPITAL"                                                  , "IA"   , "UPSLSIA"                , "0.76"             ,
    "UNITYPOINT HLTH ST LUKES HOSP SXC"  , "ST LUKES REGIONAL MEDICAL CENTER"                                   , "IA"   , "UPSLSIA"                , "0.76"             ,
    "COOK COUNTY HOSPITAL-PROVIDENT"     , "PROVIDENT HOSPITAL OF CHICAGO"                                      , "IL"   , "CCHOSIL"                , "0.89"             ,
    "COOK COUNTY HOSPITAL-PROVIDENT"     , "PROVIDENT HOSPITAL OF COOK COUNTY"                                  , "IL"   , "CCHOSIL"                , "0.93"             ,
    "HOLY CROSS HOSPITAL"                , "HOLY CROSS HOSPITAL"                                                , "IL"   , "HLYCHIL"                , "1"                ,
    "LOYOLA CENTER FOR HEALTH"           , "LOYOLA UNIVERSITY MEDICAL CENTER"                                   , "IL"   , "LYOLAIL"                , "0.79"             ,
    # "LOYOLA MEDICAL CENTER"              , "LOYOLA GOTTLIEB MEMORIAL HOSPITAL"                                  , "IL"   , "LOYMCIL"                , "0.78"             , # seems wrong based on facility code - delete?: https://www.loyolamedicine.org/location/gottlieb-memorial-hospital-0
    # "LOYOLA MEDICAL CENTER"              , "LOYOLA UNIVERSITY MEDICAL CENTER"                                   , "IL"   , "LOYMCIL"                , "0.81"             , # keep?: https://www.loyolamedicine.org/
    # "LOYOLA MEDICAL CENTER"              , "FOSTER G. MCGAW HOSPITAL LOYOLA UNIVERSITY MEDICAL CENTER"          , "IL"   , "LOYMCIL"                , "0.85"             , # keep?: https://hfsrb.illinois.gov/project.foster-g--mcgaw-hospital-loyola-university-medical-center--maywood-e-002-14.html
    "RIVEREDGE HOSPITAL"                 , "AERIES HEALTHCARE OF ILLINOIS, INC., DBA RIVEREDGE HOSPITAL"        , "IL"   , "REHOSIL"                , "0.9"              ,
    "RIVEREDGE HOSPITAL"                 , "RIVEREDGE HOSPITAL INC"                                             , "IL"   , "REHOSIL"                , "0.93"             ,
    # "UCHICAGO MEDICINE HOSPITAL CHICAGO" , "THE UNIVERSITY OF CHICAGO MEDICAL CENTER"                           , "IL"   , "UCCHIIL"                , "0.83"             , # keep: https://www.uchicagomedicine.org/
    # "UCHICAGO MEDICINE HOSPITAL CHICAGO" , "UNIVERSITY OF ILLINOIS HOSPITAL AT CHICAGO"                         , "IL"   , "UCCHIIL"                , "0.79"             , # seems wrong based on facility code - delete?: https://hospital.uillinois.edu/
    # "UCHICAGO MEDICINE HOSPITAL CHICAGO" , "UCHICAGO MEDICINE"                                                  , "IL"   , "UCCHIIL"                , "0.93"             , # keep - same as first
    "ESKENAZI HEALTH"                    , "ESKENAZI HEALTH"                                                    , "IN"   , "ESKHLIN"                , "1"                ,
    # "PARKVIEW HOSPITAL RANDALLIA"        , "PARKVIEW REGIONAL MEDICAL CENTER"                                   , "IN"   , "PVHOSIN"                , "0.83"             , # seems wrong based on facility code - delete?
    # "PARKVIEW HOSPITAL RANDALLIA"        , "PARKVIEW HOSPITAL RANDALLIA"                                        , "IN"   , "PVHOSIN"                , "1"                , # keep: https://www.parkview.com/locations/parkview-hospital-randallia
    # "PARKVIEW REGIONAL MEDICAL CENTER"   , "PARKVIEW REGIONAL MEDICAL CENTER"                                   , "IN"   , "PKMEDIN"                , "1"                , # keep: https://www.parkview.com/locations/parkview-regional-medical-center
    # "PARKVIEW REGIONAL MEDICAL CENTER"   , "PARKVIEW HOSPITAL RANDALLIA"                                        , "IN"   , "PKMEDIN"                , "0.83"             , # seems wrong based on facility code - delete?
    # "UNION HOSPITAL"                     , "UNION HOSPITAL INC"                                                 , "IN"   , "UHOSPIN"                , "0.97"             , # keep bc mentions Union Hospital in Terre Haute (which is this one) in detention inspection: https://www.ice.gov/doclib/facilityInspections/ClayCoJail_CL_05-20-2021.pdf
    # "UNION HOSPITAL"                     , "UNION HOSPITAL CLINTON"                                             , "IN"   , "UHOSPIN"                , "0.92"             , # likely the wrong facility (see above) - delete
    "OSAWATAMIE STATE HOSPITAL"          , "ADAIR ACUTE CARE AT OSAWATOMIE STATE HOSPITAL"                      , "KS"   , "OSAHOKS"                , "0.76"             ,
    "OSAWATAMIE STATE HOSPITAL"          , "OSAWATOMIE STATE HOSPITAL"                                          , "KS"   , "OSAHOKS"                , "0.98"             ,
    # i'm unsure of all of these. i found a link to an article about "Beth Israel Deaconess Hospital-Needham (none of hospitals below are this one): https://www.wgbh.org/news/local/2025-09-22/continued-ice-activity-leads-to-sightings-of-agents-at-local-hospitals"
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS HOSPITAL PLYMOUTH"                            , "MA"   , "BIDHPMA"                , "0.86"             , # keep?: https://bidplymouth.org/
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS HOSPITAL - PLYMOUTH"                          , "MA"   , "BIDHPMA"                , "0.86"             , # keep?
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS MEDICAL CENTER"                               , "MA"   , "BIDHPMA"                , "0.95"             , # seems wrong based on facility code - delete?: https://bidmc.org/locations/beth-israel-deaconess-medical-center
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS HOSPITAL - MILTON"                            , "MA"   , "BIDHPMA"                , "0.93"             , # seems wrong based on facility code - delete?: https://bidmilton.org/
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS HOSPITAL - MILTON INC"                        , "MA"   , "BIDHPMA"                , "0.9"              , # same as above
    # "BETH ISRAEL DEACONESS HOSP"         , "BETH ISRAEL DEACONESS MED CTR - WEST"                               , "MA"   , "BIDHPMA"                , "0.89"             , # seems wrong based on facility code - delete?: https://bidmc.org/locations/beth-israel-deaconess-medical-center/west-campus
    # "LAHEY HOSPITAL & MED CTR"           , "LAHEY HOSPITAL AND MEDICAL CENTER - BURLINGTON"                     , "MA"   , "LHMCBMA"                , "0.93"             , # multiple locations in burlington - which one?: https://www.lahey.org/locations?keyword=lahey%20hospital&listPage=1
    # "LAHEY HOSPITAL & MED CTR"           , "LAHEY MEDICAL CTR, PEADODY - INPT ST"                               , "MA"   , "LHMCBMA"                , "0.79"             , # seems wrong based on facility code - delete?: https://www.lahey.org/locations/lahey-medical-center-peabody
    "LEMUEL SHATTUCK HOSPITAL"           , "LEMUEL SHATTUCK HOSPITAL"                                           , "MA"   , "LSHOSMA"                , "1"                ,
    "SOUTH SHORE HOSPITAL"               , "SOUTH SHORE HOSPITAL"                                               , "MA"   , "SSHSWMA"                , "1"                ,
    "TAUNTON STATE HOSPITAL"             , "TAUNTON STATE HOSPITAL"                                             , "MA"   , "MATSHOS"                , "1"                ,
    "UMASS MEMORIAL MEDICAL CENTER"      , "UMASS MEMORIAL MEDICAL CENTER/UNIVERSITY CAMPUS"                    , "MA"   , "UMASSMA"                , "0.93"             ,
    "UNIV OF MARYLAND MEDICAL CENTER"    , "UNIVERSITY OF MARYLAND MEDICAL CENTER"                              , "MD"   , "UNMEDMD"                , "0.92"             ,
    "BRONSON BATTLE CREEK HOSP"          , "BRONSON BATTLE CREEK HOSPITAL"                                      , "MI"   , "BBCHPMI"                , "1"                ,
    "BRONSON BATTLE CREEK HOSP"          , "BRONSON BATTLE CREEK HOSPITAL - FIELDSTONE CENTER"                  , "MI"   , "BBCHPMI"                , "0.87"             ,
    "BRONSON HOSPITAL FIELDSTONE CENTER" , "BRONSON BATTLE CREEK HOSPITAL - FIELDSTONE CENTER"                  , "MI"   , "BHFLDMI"                , "0.95"             ,
    "BRONSON METHODIST HOSPITAL"         , "BRONSON METHODIST HOSPITAL"                                         , "MI"   , "BROHOMI"                , "1"                ,
    # "HENRY FORD HOSPITAL"                , "HENRY FORD HEALTH HOSPITAL"                                         , "MI"   , "HNYHOMI"                , "0.96"             , # no clue which of the following is correct: https://www.henryford.com/locations/henry-ford-hospital
    # "HENRY FORD HOSPITAL"                , "Henry Ford Health Warren Hospital"                                  , "MI"   , "HNYHOMI"                , "0.76"             , # https://www.henryford.com/locations/warren-hospital
    # "HENRY FORD HOSPITAL"                , "HENRY FORD HEALTH WEST BLOOMFIELD HOSPITAL"                         , "MI"   , "HNYHOMI"                , "0.87"             , # https://www.henryford.com/locations/west-bloomfield
    # "HENRY FORD HOSPITAL"                , "HENRY FORD HOSPITAL"                                                , "MI"   , "HNYHOMI"                , "1"                , # same as first
    # "HENRY FORD HOSPITAL"                , "HENRY FORD WYANDOTTE HOSPITAL"                                      , "MI"   , "HNYHOMI"                , "0.93"             , # https://www.henryford.com/locations/wyandotte
    # "HENRY FORD HOSPITAL"                , "HENRY FORD WEST BLOOMFIELD HOSPITAL"                                , "MI"   , "HNYHOMI"                , "0.87"             , # same as third
    # "HENRY FORD HOSPITAL"                , "HENRY FORD HEALTH WYANDOTTE HOSPITAL"                               , "MI"   , "HNYHOMI"                , "0.94"             , # same as fifth
    "MCLAREN PORT HURON HOSP"            , "MCLAREN PORT HURON"                                                 , "MI"   , "MLPHHMI"                , "0.96"             , # https://www.mclaren.org/port-huron/mclaren-port-huron-home
    "MUNSON HEALTHCARE CADILLAC HOSP"    , "MUNSON HEALTHCARE CADILLAC HOSPITAL"                                , "MI"   , "MHCHSMI"                , "1"                ,
    "PROMEDICA MONROE REG HOSP"          , "PROMEDICA MONROE REGIONAL HOSPITAL"                                 , "MI"   , "PMMRHMI"                , "0.94"             ,
    "SAMARITAN BEHAVIORAL CENTER"        , "SAMARITAN BEHAVIORAL CENTER"                                        , "MI"   , "SAMBCMI"                , "1"                ,
    "ABBOTT NORTHWESTERN HOSP"           , "ABBOTT NORTHWESTERN HOSPITAL"                                       , "MN"   , "ANWHMMN"                , "0.97"             ,
    # "CENTRACARE RICE MEMORIAL HOSP"      , "CENTRACARE- RICE MEMORIAL HOSPITAL"                                 , "MN"   , "CCRMHMN"                , "1"                , # keep: https://www.centracare.com/locations/profile/rice-memorial-hospital/
    # "CENTRACARE RICE MEMORIAL HOSP"      , "CENTRACARE HEALTH SYSTEM - MELROSE HOSPITAL"                        , "MN"   , "CCRMHMN"                , "0.81"             , # seems wrong based on facility code - delete?: https://www.centracare.com/locations/profile/melrose-hospital/
    "HENNEPIN COUNTY MED CTR"            , "HENNEPIN COUNTY MEDICAL CTR"                                        , "MN"   , "HCMCMMN"                , "0.96"             ,
    "M HEALTH FAIRVIEW SOUTHDALE HOSP"   , "M HEALTH FAIRVIEW SOUTHDALE HOSPITAL"                               , "MN"   , "MHFSHMN"                , "1"                ,
    "M HEALTH FAIRVIEW SOUTHDALE HOSP"   , "FAIRVIEW SOUTHDALE HOSPITAL"                                        , "MN"   , "MHFSHMN"                , "0.81"             ,
    # "MAYO CLINIC HEALTH SYS - MANKATO"   , "MAYO CLINIC HEALTH SYSTEM - ALBERT LEA AND AUSTIN"                  , "MN"   , "MYOMKMN"                , "0.83"             , # seems wrong based on facility code - delete?
    # "MAYO CLINIC HEALTH SYS - MANKATO"   , "MAYO CLINIC HEALTH SYSTEM - MANKATO"                                , "MN"   , "MYOMKMN"                , "1"                , # keep: https://www.mayoclinichealthsystem.org/locations/mankato
    # "MAYO CLINIC HEALTH SYS - MANKATO"   , "MAYO CLINIC HEALTH SYS MANKATO"                                     , "MN"   , "MYOMKMN"                , "1"                , # keep
    # "MAYO CLINIC HEALTH SYS - MANKATO"   , "MAYO CLINIC HEALTH SYS WASECA"                                      , "MN"   , "MYOMKMN"                , "0.81"             , # seems wrong based on facility code - delete?: https://www.mayoclinichealthsystem.org/locations/waseca
    # "MAYO CLINIC HEALTH SYS - MANKATO"   , "MAYO CLINIC HEALTH SYSTEM"                                          , "MN"   , "MYOMKMN"                , "0.9"              , # same as second
    # "MAYO CLINIC HEALTH SYS ALBERT LEA"  , "MAYO CLINIC HEALTH SYSTEM - ALBERT LEA AND AUSTIN"                  , "MN"   , "MCHALMN"                , "0.95"             , # keep: https://www.mayoclinichealthsystem.org/locations/albert-lea
    # "MAYO CLINIC HEALTH SYS ALBERT LEA"  , "MAYO CLINIC HEALTH SYSTEM - MANKATO"                                , "MN"   , "MCHALMN"                , "0.79"             , # seems wrong based on facility code - delete?
    # "MAYO CLINIC HEALTH SYS ALBERT LEA"  , "MAYO CLINIC ALBERT LEA AUSTIN"                                      , "MN"   , "MCHALMN"                , "0.91"             , # same as first
    # "MERCY HOSPITAL"                     , "MERCY HOSPITAL"                                                     , "MN"   , "MRYHPMN"                , "1"                ,
    # "MERCY HOSPITAL"                     , "MERCY HOSPITAL - UNITY CAMPUS"                                      , "MN"   , "MRYHPMN"                , "0.84"             ,
    # "REGIONS MEDICAL CENTER"             , "ST FRANCIS REGIONAL MEDICAL CENTER"                                 , "MN"   , "REMEDMN"                , "0.77"             ,
    # "MERCY HOSPITAL SOUTH"               , "MERCY HOSPITAL ST LOUIS"                                            , "MO"   , "MRYHOMO"                , "0.88"             ,
    # "MERCY HOSPITAL SOUTH"               , "MERCY HOSPITAL ST. LOUIS"                                           , "MO"   , "MRYHOMO"                , "0.88"             ,
    # "MERCY HOSPITAL SOUTH"               , "MERCY HOSPITAL SOUTH"                                               , "MO"   , "MRYHOMO"                , "1"                ,
    # "MERCY HOSPITAL SOUTH"               , "MERCY HOSPITAL SOUTHEAST"                                           , "MO"   , "MRYHOMO"                , "0.99"             ,
    "CHI HEALTH MIDLANDS HOSPITAL"       , "CHI HEALTH MIDLANDS"                                                , "NE"   , "MDLNDNE"                , "0.95"             , # https://www.chihealth.com/locations/midlands
    "NEBRASKA MED CTR OMAHA"             , "THE NEBRASKA MEDICAL CENTER"                                        , "NE"   , "NMCOMNE"                , "0.79"             ,
    "WENTWORTH DOUGLAS HOSPITAL"         , "WENTWORTH-DOUGLASS HOSPITAL"                                        , "NH"   , "WWDHDNH"                , "1"                , # https://www.wdhospital.org/wdh
    "WENTWORTH DOUGLAS HOSPITAL"         , "WENTWORTH - DOUGLASS HOSPITAL"                                      , "NH"   , "WWDHDNH"                , "1"                ,
    # "HACKENSACK MED CTR"                 , "HACKENSACK UNIVERSITY MEDICAL CENTER"                               , "NJ"   , "HACMCNJ"                , "0.9"              ,
    # "HACKENSACK MED CTR"                 , "CAREPOINT HEALTH-HOBOKEN UNIVERSITY MEDICAL CENTER"                 , "NJ"   , "HACMCNJ"                , "0.76"             ,
    # "HACKENSACK MED CTR"                 , "CAREPOINT HEALTH - HOBOKEN UNIVERSITY MEDICAL CENTER"               , "NJ"   , "HACMCNJ"                , "0.76"             ,
    # "HACKENSACK MED CTR"                 , "HACKENSACK MERIDIAN HEALTH PASCACK VALLEY MEDICAL"                  , "NJ"   , "HACMCNJ"                , "0.75"             ,
    # "HACKENSACK MED CTR"                 , "HACKENSACKMERIDIAN HEALTH, MOUNTAINSIDE MEDICAL CENTER"             , "NJ"   , "HACMCNJ"                , "0.85"             ,
    # "HACKENSACK MED CTR"                 , "PASCACK VALLEY MEDICAL CENTER, HACKENSACK MERIDIAN HEALTH"          , "NJ"   , "HACMCNJ"                , "0.89"             ,
    "JERSEY CITY MED CTR"                , "JERSEY CITY MEDICAL CENTER"                                         , "NJ"   , "JYCMCNJ"                , "0.85"             ,
    # "TRINITAS REGIONAL MED CTR"          , "TRINITAS REGIONAL MEDICAL CENTER"                                   , "NJ"   , "TRRMCNJ"                , "0.99"             ,
    # "TRINITAS REGIONAL MED CTR"          , "CARE ONE AT TRINITAS REGIONAL MEDICAL CENTER"                       , "NJ"   , "TRRMCNJ"                , "0.86"             ,
    "UNIVERSITY HOSPITAL"                , "THE UNIVERSITY HOSPITAL"                                            , "NJ"   , "UNIVHNJ"                , "0.91"             ,
    "UNIVERSITY HOSPITAL"                , "UNIVERSITY HOSPITAL"                                                , "NJ"   , "UNIVHNJ"                , "1"                ,
    "CIBOLA GENERAL HOSPITAL"            , "CIBOLA GENERAL HOSPITAL"                                            , "NM"   , "CIBOGNM"                , "1"                ,
    # "PRESBYTERIAN HOSP ABQ"              , "PRESBYTERIAN RUST MEDICAL CENTER"                                   , "NM"   , "PRSHANM"                , "0.79"             ,
    "UNIVERSITY OF NEW MEXICO HOSPITAL"  , "UNM HOSPITAL"                                                       , "NM"   , "UNMHANM"                , "0.83"             ,
    "DESERT VIEW HOSPITAL"               , "DESERT VIEW HOSPITAL"                                               , "NV"   , "DVHSPNV"                , "1"                ,
    # "HENDERSON HOSPITAL"                 , "HENDERSON HOSPITAL"                                                 , "NV"   , "HDHSPNV"                , "1"                ,
    # "HENDERSON HOSPITAL"                 , "WEST HENDERSON HOSPITAL"                                            , "NV"   , "HDHSPNV"                , "0.87"             ,
    # "HENDERSON HOSPITAL"                 , "ENCOMPASS HEALTH REHABILITATION HOSPITAL OF HENDERSON"              , "NV"   , "HDHSPNV"                , "0.92"             ,
    # "NORTHERN NEVADA MED CTR"            , "NORTHERN NEVADA MEDICAL CENTER"                                     , "NV"   , "NNVMCNV"                , "0.99"             ,
    # "NORTHERN NEVADA MED CTR"            , "NORTHERN NEVADA SIERRA MEDICAL CENTER"                              , "NV"   , "NNVMCNV"                , "0.85"             ,
    "SUMMERLIN HOSPITAL MEDICAL CENTER"  , "SUMMERLIN HOSPITAL MEDICAL CENTER"                                  , "NV"   , "SHMCHNV"                , "1"                ,
    # "UNIVERSITY MEDICAL CENTER"          , "UNIVERSITY MEDICAL CENTER"                                          , "NV"   , "UMCLVNV"                , "1"                ,
    # "UNIVERSITY MEDICAL CENTER"          , "UNIVERSITY MEDICAL CENTER OF SOUTHERN NEVADA"                       , "NV"   , "UMCLVNV"                , "0.88"             ,
    # "BRONXCARE HEALTH SYSTEM"            , "BRONXCARE HOSPITAL CENTER"                                          , "NY"   , "BXCHSNY"                , "0.9"              ,
    # "BUFFALO GENERAL HOSPITAL"           , "MERCY HOSPITAL OF BUFFALO"                                          , "NY"   , "BFLHPNY"                , "0.89"             ,
    # "BUFFALO GENERAL HOSPITAL"           , "BUFFALO GENERAL MEDICAL CENTER"                                     , "NY"   , "BFLHPNY"                , "0.94"             ,
    "ERIE COUNTY MEDICAL CENTER"         , "ERIE COUNTY MEDICAL CENTER"                                         , "NY"   , "ECMCBNY"                , "1"                ,
    "Jamaica Hospital Med Ctr"           , "JAMAICA HOSPITAL MEDICAL CENTER"                                    , "NY"   , "JHMCJNY"                , "0.99"             ,
    "KENMORE MERCY HOSPITAL"             , "KENMORE MERCY HOSPITAL"                                             , "NY"   , "KENMHNY"                , "1"                ,
    "KINGS COUNTY HOSPITAL"              , "KINGS COUNTY HOSPITAL CENTER"                                       , "NY"   , "KCHOSNY"                , "0.94"             , # https://www.nychealthandhospitals.org/locations/kings-county/
    "LOCKPORT MEMORIAL HOSPITAL"         , "LOCKPORT MEMORIAL HOSPITAL, A CAMPUS OF MOUNT ST MARY'S"            , "NY"   , "LOCMHNY"                , "0.82"             , # https://www.chsbuffalo.org/lockport-memorial-hospital/
    "NASSAU UNIV MED CTR"                , "NASSAU UNIVERSITY MEDICAL CENTER"                                   , "NY"   , "NUMCENY"                , "0.95"             ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK-PRESBYTERIAN HOSPITAL"                                     , "NY"   , "PRESBNY"                , "1"                ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK - PRESBYTERIAN BROOKLYN METHODIST HOSPITAL"                , "NY"   , "PRESBNY"                , "0.83"             ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK - PRESBYTERIAN - LOWER MANHATTAN HOSPITAL"                 , "NY"   , "PRESBNY"                , "0.83"             ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK - PRESBYTERIAN HOSPITAL - NEW YORK WEILL CORNELL CENTER"   , "NY"   , "PRESBNY"                , "0.93"             ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK - PRESBYTERIAN HOSPITAL - COLUMBIA PRESBYTERIAN CENTER"    , "NY"   , "PRESBNY"                , "0.93"             ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK - PRESBYTERIAN HOSPITAL - ALLEN HOSPITAL"                  , "NY"   , "PRESBNY"                , "0.9"              ,
    # "NEW YORK PRESBYTERIAN HOSPITAL"     , "NEW YORK PRESBYTERIAN MORGAN STANLEY CHILDREN'S HOSPITAL"           , "NY"   , "PRESBNY"                , "0.84"             ,
    "ROCHESTER GENERAL HOSPITAL"         , "ROCHESTER GENERAL HOSPITAL"                                         , "NY"   , "ROCGHNY"                , "1"                ,
    "STRONG MEMORIAL HOSPITAL"           , "STRONG MEMORIAL HOSPITAL"                                           , "NY"   , "STRMONY"                , "1"                ,
    "UHS WILSON MEDICAL CENTER"          , "UNITED HEALTH SERVICES HOSPITALS INC - WILSON MEDICAL CENTER"       , "NY"   , "UHSHONY"                , "0.82"             ,
    # "UNITED MEMORIAL MED CTR"            , "UNITED MEMORIAL MEDICAL CENTER"                                     , "NY"   , "UMEMCNY"                , "1"                ,
    # "UNITED MEMORIAL MED CTR"            , "UNITED MEMORIAL MEDICAL CENTER NORTH STREET CAMPUS"                 , "NY"   , "UMEMCNY"                , "0.86"             ,
    # "UNITED MEMORIAL MED CTR"            , "UNITED MEMORIAL MEDICAL CENTER BANK STREET CAMPUS"                  , "NY"   , "UMEMCNY"                , "0.86"             ,
    "UNITY HOSPITAL"                     , "UNITY HOSPITAL"                                                     , "NY"   , "UNIHONY"                , "1"                ,
    "BELLMONT PINES HOSPITAL"            , "BELMONT PINES HOSPITAL"                                             , "OH"   , "BPHOSOH"                , "1"                ,
    "MERCY HEALTH - TIFFIN HOSPITAL"     , "MERCY HEALTH - TIFFIN HOSPITAL"                                     , "OH"   , "MHTFHOH"                , "1"                ,
    "MERCY HEALTH - TIFFIN HOSPITAL"     , "MERCY HEALTH TIFFIN HOSPITAL"                                       , "OH"   , "MHTFHOH"                , "1"                ,
    # "ST ELIZABETH YOUNGSTOWN HOSPITAL"   , "ST ELIZABETH YOUNGSTOWN HOSPITAL"                                   , "OH"   , "STELHOH"                , "1"                ,
    # "ST ELIZABETH YOUNGSTOWN HOSPITAL"   , "MERCY HEALTH ST. ELIZABETH YOUNGSTOWN HOSPITAL"                     , "OH"   , "STELHOH"                , "0.94"             ,
    # "ST ELIZABETH YOUNGSTOWN HOSPITAL"   , "MERCY HEALTH ST. ELIZABETH BOARDMAN HOSPITAL"                       , "OH"   , "STELHOH"                , "0.83"             ,
    # "UNIV HOSP GEAUGA MED CTR"           , "UHHS - GEAUGA REGIONAL HOSPITAL"                                    , "OH"   , "UHGMCOH"                , "0.84"             ,
    "INTEGRIS HEALTH BAPTIST MEDICAL"    , "INTEGRIS BAPTIST MEDICAL CENTER, INC"                               , "OK"   , "INTEGOK"                , "0.89"             , # https://baptist.integrishealth.org/
    "INTEGRIS HEALTH BAPTIST MEDICAL"    , "INTEGRIS BASS BAPTIST HEALTH CENTER"                                , "OK"   , "INTEGOK"                , "0.79"             ,
    # "INTEGRIS HEALTH PONCA CITY HOSPITA" , "INTEGRIS HEALTH PONCA CITY"                                         , "OK"   , "INTPOOK"                , "0.97"             ,
    # "INTEGRIS HEALTH PONCA CITY HOSPITA" , "ALLIANCEHEALTH PONCA CITY"                                          , "OK"   , "INTPOOK"                , "0.76"             ,
    # "GEISINGER MEDICAL CENTER"           , "GEISINGER MEDICAL CENTER"                                           , "PA"   , "GSMCDPA"                , "1"                ,
    # "GEISINGER MEDICAL CENTER"           , "GEISINGER MEDICAL CENTER MUNCY"                                     , "PA"   , "GSMCDPA"                , "0.76"             ,
    # "GEISINGER MEDICAL CENTER"           , "GEISINGER MEDICAL CENTER - TRANSPLANT CENTER"                       , "PA"   , "GSMCDPA"                , "0.91"             ,
    "LEHIGH VALLEY HOSPITAL - POCONO"    , "LEHIGH VALLEY HOSPITAL"                                             , "PA"   , "LVYHOPA"                , "0.82"             ,
    "LEHIGH VALLEY HOSPITAL - POCONO"    , "LEHIGH VALLEY HOSPITAL - POCONO"                                    , "PA"   , "LVYHOPA"                , "1"                ,
    "MILTON S. HERSHEY MEDICAL CENTER"   , "MILTON S HERSHEY MEDICAL CENTER"                                    , "PA"   , "MHRHOPA"                , "1"                ,
    "MILTON S. HERSHEY MEDICAL CENTER"   , "THE MILTON S. HERSHEY MEDICAL CENTER"                               , "PA"   , "MHRHOPA"                , "0.94"             ,
    "MOUNT NITTANY MEDICAL CENTER"       , "MOUNT NITTANY MEDICAL CENTER"                                       , "PA"   , "MTNITPA"                , "1"                ,
    "READING HOSPITAL"                   , "READING HOSPITAL"                                                   , "PA"   , "RDHOSPA"                , "1"                ,
    # "UPMC PRESBYTERIAN HOSPITAL"         , "MAGEE WOMENS HOSPITAL OF UPMC HEALTH SYSTEM"                        , "PA"   , "UPMCPPA"                , "0.77"             ,
    # "UPMC PRESBYTERIAN HOSPITAL"         , "UPMC PRESBYTERIAN SHADYSIDE"                                        , "PA"   , "UPMCPPA"                , "0.85"             ,
    # "UPMC PRESBYTERIAN HOSPITAL"         , "UPMC PRESBYTERIAN SHADYSIDE - TRANSPLANT CENTER"                    , "PA"   , "UPMCPPA"                , "0.91"             ,
    # "BAYAMON HOSPITAL"                   , "BAYAMON MEDICAL CENTER"                                             , "PR"   , "BAYAMPR"                , "0.82"             ,
    # "BAYAMON HOSPITAL"                   , "HOSPITAL REGIONAL BAYAMON"                                          , "PR"   , "BAYAMPR"                , "0.8"              ,
    # "HOSPITAL PAVIA-SANTURCE"            , "HOSPITAL PAVIA SANTURCE"                                            , "PR"   , "HOSPSPR"                , "1"                ,
    # "HOSPITAL PAVIA-SANTURCE"            , "HOSPITAL PAVIA HATO REY, INC"                                       , "PR"   , "HOSPSPR"                , "0.91"             , # delete bc other two already exist
    # "PAVIA HOSPITAL - HATO REY"          , "HOSPITAL PAVIA HATO REY, INC"                                       , "PR"   , "PAVHRPR"                , "0.9"              ,
    "MIRIAM HOSPITAL"                    , "THE MIRIAM HOSPITAL"                                                , "RI"   , "MIRHPRI"                , "0.84"             , # https://www.brownhealth.org/locations/miriam-hospital
    # "RHODE ISLAND HOSPITAL"              , "RHODE ISLAND HOSPITAL"                                              , "RI"   , "RIHPRRI"                , "1"                ,
    # "RHODE ISLAND HOSPITAL"              , "MEMORIAL HOSPITAL OF RHODE ISLAND"                                  , "RI"   , "RIHPRRI"                , "0.87"             ,
    # "RHODE ISLAND HOSPITAL"              , "REHABILITATION HOSPITAL OF RHODE ISLAND"                            , "RI"   , "RIHPRRI"                , "0.85"             ,
    # "RHODE ISLAND HOSPITAL"              , "SAINT JOSEPH HEALTH SERVICES OF RHODE ISLAND"                       , "RI"   , "RIHPRRI"                , "0.77"             ,
    "ANSON GENERAL HOSPITAL"             , "ANSON GENERAL HOSPITAL"                                             , "TX"   , "ANSGHTX"                , "1"                ,
    # "ASCENSION SETON WILLIAMS HOSP"      , "ASCENSION SETON MEDICAL CENTER AUSTIN"                              , "TX"   , "ASWHRTX"                , "0.82"             ,
    # "ASCENSION SETON WILLIAMS HOSP"      , "ASCENSION SETON WILLIAMSON"                                         , "TX"   , "ASWHRTX"                , "0.91"             ,
    # "BAPTIST MEDICAL CENTER"             , "BAPTIST MEDICAL CENTER"                                             , "TX"   , "BMCSATX"                , "1"                ,
    # "BAPTIST MEDICAL CENTER"             , "FIRST BAPTIST MEDICAL CENTER"                                       , "TX"   , "BMCSATX"                , "0.92"             ,
    # "BAPTIST MEDICAL CENTER"             , "NORTH CENTRAL BAPTIST HOSPITAL"                                     , "TX"   , "BMCSATX"                , "0.8"              ,
    # "BAPTIST MEDICAL CENTER"             , "NORTHEAST BAPTIST HOSPITAL"                                         , "TX"   , "BMCSATX"                , "0.79"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER - TEMPLE"                       , "TX"   , "BSWRRTX"                , "0.88"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER AT IRVING"                      , "TX"   , "BSWRRTX"                , "0.83"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE ALL SAINTS MEDICAL CENTER"                   , "TX"   , "BSWRRTX"                , "0.78"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER- WAXAHACHIE"                    , "TX"   , "BSWRRTX"                , "0.78"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER UPTOWN"                       , "TX"   , "BSWRRTX"                , "0.78"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - UPTOWN"                     , "TX"   , "BSWRRTX"                , "0.78"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER  GRAPEVINE"                     , "TX"   , "BSWRRTX"                , "0.83"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER LAKE POINTE"                  , "TX"   , "BSWRRTX"                , "0.76"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - LAKE POINTE"                , "TX"   , "BSWRRTX"                , "0.76"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT &  WHITE MEDICAL CENTER - CENTENNIAL"                  , "TX"   , "BSWRRTX"                , "0.87"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER PLANO"                          , "TX"   , "BSWRRTX"                , "0.85"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER -TAYLOR"                        , "TX"   , "BSWRRTX"                , "0.87"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER - ROUND ROCK"                   , "TX"   , "BSWRRTX"                , "0.91"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER SUNNYVALE"                    , "TX"   , "BSWRRTX"                , "0.76"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - SUNNYVALE"                  , "TX"   , "BSWRRTX"                , "0.76"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE  MEDICAL CENTER  MCKINNEY"                   , "TX"   , "BSWRRTX"                , "0.89"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - MCKINNEY"                   , "TX"   , "BSWRRTX"                , "0.89"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER- COLLEGE STATI"                 , "TX"   , "BSWRRTX"                , "0.92"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER - MARBLE FALLS"                 , "TX"   , "BSWRRTX"                , "0.85"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER PFLUGERVILLE"                   , "TX"   , "BSWRRTX"                , "0.78"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT & WHITE MEDICAL CENTER- AUSTIN"                        , "TX"   , "BSWRRTX"                , "0.93"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE INSTITUTE FOR REHABILITATION - FRISCO"       , "TX"   , "BSWRRTX"                , "0.86"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE INSTITUTE FOR REHABILITATION - LAKEWAY"      , "TX"   , "BSWRRTX"                , "0.83"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - AUSTIN"                     , "TX"   , "BSWRRTX"                , "0.93"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - BRENHAM"                    , "TX"   , "BSWRRTX"                , "0.93"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - CENTENNIAL"                 , "TX"   , "BSWRRTX"                , "0.77"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - FRISCO"                     , "TX"   , "BSWRRTX"                , "0.79"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - HILLCREST"                  , "TX"   , "BSWRRTX"                , "0.94"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - IRVING"                     , "TX"   , "BSWRRTX"                , "0.76"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - LAKEWAY"                    , "TX"   , "BSWRRTX"                , "0.92"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - MARBLE FALLS"               , "TX"   , "BSWRRTX"                , "0.84"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - PLANO"                      , "TX"   , "BSWRRTX"                , "0.92"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - ROUND ROCK"                 , "TX"   , "BSWRRTX"                , "0.81"             ,
    # "BAYLOR SCOTT WHITE MED RR"          , "BAYLOR SCOTT AND WHITE MEDICAL CENTER - TAYLOR"                     , "TX"   , "BSWRRTX"                , "0.88"             ,
    "CEDAR CREST HOSP TREAT"             , "CEDAR CREST HOSPITAL"                                               , "TX"   , "CCHRTTX"                , "0.85"             ,
    "CHI ST LUKE'S HEALTH MEM"           , "CHI ST LUKES HEALTH BAYLOR COLLEGE OF MEDICINE MEDICAL CENTER"      , "TX"   , "CHISLTX"                , "0.88"             ,
    # "CHRISTUS SANTA ROSA HOSPITAL - MED" , "CHRISTUS SANTA ROSA MEDICAL CENTER"                                 , "TX"   , "CHSRHTX"                , "0.94"             ,
    # "CHRISTUS SANTA ROSA HOSPITAL - MED" , "CHRISTUS SANTA ROSA HOSPITAL - NEW BRAUNFELS"                       , "TX"   , "CHSRHTX"                , "0.83"             ,
    # "CHRISTUS SANTA ROSA HOSPITAL - MED" , "CHRISTUS SANTA ROSA HOSPITAL - WESTOVER HILLS"                      , "TX"   , "CHSRHTX"                , "0.83"             ,
    # "CHRISTUS SPOHN HOSP CC - SOUTH"     , "CHRISTUS SPOHN HOSPITAL CORPUS CHRISTI"                             , "TX"   , "CCCSSTX"                , "0.9"              ,
    # "CHRISTUS SPOHN HOSP CC - SOUTH"     , "CHRISTUS SPOHN HOSPITAL CORPUS CHRISTI SHORELINE"                   , "TX"   , "CCCSSTX"                , "0.82"             ,
    # "CHRISTUS SPOHN HOSP CC - SOUTH"     , "CHRISTUS SPOHN HOSPITAL CORPUS CHRISTI SOUTH"                       , "TX"   , "CCCSSTX"                , "0.96"             ,
    "CONNALLY MEMORIAL MEDICAL CENTER"   , "CONNALLY MEMORIAL MEDICAL CENTER"                                   , "TX"   , "CONHOTX"                , "1"                ,
    "CROSS CREEK HOSPITAL"               , "CROSS CREEK HOSPITAL"                                               , "TX"   , "CCHAUTX"                , "1"                ,
    "DIMMIT REGIONAL HOSPITAL"           , "DIMMIT REGIONAL HOSPITAL"                                           , "TX"   , "DRHCSTX"                , "1"                ,
    "EL PASO BEHAVIORAL HEALTH SYSTEM"   , "EL PASO BEHAVIORAL HEALTH SYSTEM"                                   , "TX"   , "EPBHSTX"                , "1"                ,
    # "EL PASO LTAC HOSPITAL"              , "EL PASO LTAC HOSPITAL"                                              , "TX"   , "EPLTATX"                , "1"                ,
    # "EL PASO LTAC HOSPITAL"              , "EL PASO SPECIALTY HOSPITAL"                                         , "TX"   , "EPLTATX"                , "0.84"             ,
    "FRIO REGIONAL HOSPITAL"             , "FRIO REGIONAL HOSPITAL"                                             , "TX"   , "FRIRHTX"                , "1"                ,
    # "GEORGETOWN BEHAVIORAL HOSPITAL"     , "GEORGETOWN BEHAVIORAL HEALTH INSTITUTE"                             , "TX"   , "GRGBVTX"                , "0.95"             ,
    "HARLINGEN MEDICAL CENTER"           , "HARLINGEN MEDICAL CENTER"                                           , "TX"   , "HMCHOTX"                , "1"                ,
    "HARRIS HEALTH BEN TAUB HOSPITAL"    , "HARRIS HEALTH SYSTEM BEN TAUB HOSPITAL"                             , "TX"   , "HHBTHTX"                , "0.96"             ,
    "HARRIS HEALTH LBJ HOSPITAL"         , "HARRIS HEALTH SYSTEM LYNDON B JOHNSON HOSPITAL"                     , "TX"   , "LBJHSTX"                , "0.93"             ,
    "HASKELL MEMORIAL HOSPITAL"          , "HASKELL MEMORIAL HOSPITAL"                                          , "TX"   , "HASMHTX"                , "1"                ,
    "HCA HOUSTON HC CONROE"              , "HCA HOUSTON HEALTHCARE CONROE"                                      , "TX"   , "HCAHCTX"                , "0.93"             ,
    # "HENDRICK MEDICAL CENTER"            , "HENDRICK MEDICAL CENTER"                                            , "TX"   , "HNDMCTX"                , "1"                ,
    # "HENDRICK MEDICAL CENTER"            , "HENDRICK MEDICAL CENTER BROWNWOOD"                                  , "TX"   , "HNDMCTX"                , "0.79"             ,
    # "HENDRICK MEDICAL CENTER"            , "CONTINUECARE HOSPITAL AT HENDRICK MEDICAL CENTER"                   , "TX"   , "HNDMCTX"                , "0.87"             ,
    # "HENDRICK MEDICAL CENTER"            , "HENDRICK MEDICAL CENTER SOUTH"                                      , "TX"   , "HNDMCTX"                , "0.85"             ,
    # "HOUSTON HEALTHCARE HOSP"            , "HCA HOUSTON HEALTHCARE SOUTHEAST"                                   , "TX"   , "HHCHKTX"                , "0.89"             ,
    # "HOUSTON HEALTHCARE HOSP"            , "HCA HOUSTON HEALTHCARE NORTHWEST"                                   , "TX"   , "HHCHKTX"                , "0.86"             ,
    # "HOUSTON HEALTHCARE HOSP"            , "HCA HOUSTON HEALTHCARE MEDICAL CENTER"                              , "TX"   , "HHCHKTX"                , "0.75"             ,
    # "HOUSTON HEALTHCARE HOSP"            , "HCA HOUSTON HEALTHCARE NORTH CYPRESS"                               , "TX"   , "HHCHKTX"                , "0.76"             ,
    "HUNTSVILLE MEMORIAL HOSPITAL"       , "HUNTSVILLE MEMORIAL HOSPITAL"                                       , "TX"   , "HUNMHTX"                , "1"                ,
    "JOHN PETER SMITH HOSPITAL"          , "JOHN PETER SMITH HOSPITAL"                                          , "TX"   , "JOPSHTX"                , "1"                ,
    "KNOX COUNTY HOSPITAL"               , "KNOX COUNTY HOSPITAL"                                               , "TX"   , "KNXCHTX"                , "1"                ,
    # "LAREDO MEDICAL CENTER"              , "LAREDO MEDICAL CENTER"                                              , "TX"   , "LRDMCTX"                , "1"                ,
    # "LAREDO MEDICAL CENTER"              , "DOCTORS HOSPITAL OF LAREDO"                                         , "TX"   , "LRDMCTX"                , "0.79"             ,
    "LIMESTON MEDICAL CENTER"            , "LIMESTONE MEDICAL CENTER"                                           , "TX"   , "LIMMCTX"                , "0.99"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN HOSPITAL SYSTEM"                                   , "TX"   , "MHNHHTX"                , "0.81"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN MEMORIAL CITY HOSPITAL"                            , "TX"   , "MHNHHTX"                , "0.83"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN NORTHEAST HOSPITAL"                                , "TX"   , "MHNHHTX"                , "0.82"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN GREATER HEIGHTS HOSPITAL"                          , "TX"   , "MHNHHTX"                , "0.86"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN SOUTHEAST HOSPITAL"                                , "TX"   , "MHNHHTX"                , "0.79"             ,
    # "MEMORIAL HERMANN NE HOSP"           , "MEMORIAL HERMANN SOUTHWEST HOSPITAL"                                , "TX"   , "MHNHHTX"                , "0.75"             ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL ATASCOSA"                                        , "TX"   , "METHOTX"                , "0.75"             ,
    # "METHODIST HOSPITAL"                 , "HOUSTON METHODIST HOSPITAL"                                         , "TX"   , "METHOTX"                , "0.76"             ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL"                                                 , "TX"   , "METHOTX"                , "1"                ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL  STONE OAK"                                      , "TX"   , "METHOTX"                , "0.9"              ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL - STONE OAK"                                     , "TX"   , "METHOTX"                , "0.9"              ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL FOR SURGERY"                                     , "TX"   , "METHOTX"                , "0.84"             ,
    # "METHODIST HOSPITAL"                 , "METHODIST HOSPITAL SOUTH"                                           , "TX"   , "METHOTX"                , "0.98"             ,
    # "METHODIST HOSPITAL"                 , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "METHOTX"                , "0.88"             ,
    # "METHODIST HOSPITAL"                 , "METROPOLITAN METHODIST HOSPITAL"                                    , "TX"   , "METHOTX"                , "0.88"             ,
    # "METHODIST HOSPITAL"                 , "NORTHEAST METHODIST HOSPITAL"                                       , "TX"   , "METHOTX"                , "0.82"             ,
    "METHODIST HOSPITAL METROPOLITAN"    , "METROPOLITAN METHODIST HOSPITAL"                                    , "TX"   , "MHMSATX"                , "0.95"             ,
    # "METHODIST HOSPITAL NORTHEAST"       , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "MHNSATX"                , "0.84"             ,
    # "METHODIST HOSPITAL NORTHEAST"       , "METROPOLITAN METHODIST HOSPITAL"                                    , "TX"   , "MHNSATX"                , "0.76"             ,
    # "METHODIST HOSPITAL NORTHEAST"       , "NORTHEAST METHODIST HOSPITAL"                                       , "TX"   , "MHNSATX"                , "0.95"             ,
    # "METHODIST HOSPITAL SOUTH"           , "METHODIST HOSPITAL"                                                 , "TX"   , "MTHOSTX"                , "0.98"             ,
    # "METHODIST HOSPITAL SOUTH"           , "METHODIST SOUTHLAKE MEDICAL CENTER"                                 , "TX"   , "MTHOSTX"                , "0.89"             ,
    # "METHODIST HOSPITAL SOUTH"           , "METHODIST HOSPITAL SOUTH"                                           , "TX"   , "MTHOSTX"                , "1"                ,
    # "METHODIST HOSPITAL SOUTH"           , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "MTHOSTX"                , "0.82"             ,
    # "METHODIST HOSPITAL STONE OAK"       , "METHODIST HOSPITAL"                                                 , "TX"   , "MHSOSTX"                , "0.9"              ,
    # "METHODIST HOSPITAL STONE OAK"       , "METHODIST HOSPITAL  STONE OAK"                                      , "TX"   , "MHSOSTX"                , "1"                ,
    # "METHODIST HOSPITAL STONE OAK"       , "METHODIST HOSPITAL - STONE OAK"                                     , "TX"   , "MHSOSTX"                , "1"                ,
    # "METHODIST HOSPITAL STONE OAK"       , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "MHSOSTX"                , "0.84"             ,
    "METHODIST HOSPITAL TEXSAN"          , "METHODIST HOSPITAL"                                                 , "TX"   , "MHTSATX"                , "0.92"             ,
    "METHODIST HOSPITAL TEXSAN"          , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "MHTSATX"                , "0.93"             ,
    "METHODIST SPECIALTY TRANSPLANT HOS" , "METHODIST SPECIALTY AND TRANSPLANT HOSPITAL"                        , "TX"   , "MSTHSTX"                , "0.94"             ,
    "MIDLAND MEMORIAL HOSPITAL"          , "MIDLAND MEMORIAL HOSPITAL"                                          , "TX"   , "MIDMHTX"                , "1"                ,
    # "NORTH CENTRAL BAPTIST HOSPITAL"     , "BAPTIST MEDICAL CENTER"                                             , "TX"   , "NCBHOTX"                , "0.8"              ,
    # "NORTH CENTRAL BAPTIST HOSPITAL"     , "NORTH CENTRAL BAPTIST HOSPITAL"                                     , "TX"   , "NCBHOTX"                , "1"                ,
    # "NORTHEAST BAPTIST HOSPITAL"         , "BAPTIST MEDICAL CENTER"                                             , "TX"   , "NEBHSTX"                , "0.79"             ,
    # "NORTHEAST BAPTIST HOSPITAL"         , "NORTHEAST BAPTIST HOSPITAL"                                         , "TX"   , "NEBHSTX"                , "1"                ,
    "OTTO KAISER MEMORIAL HOSPITAL"      , "OTTO KAISER MEMORIAL HOSPITAL"                                      , "TX"   , "OKMHKTX"                , "1"                ,
    "PALMS BEHAVIORAL HEALTH"            , "PALMS BEHAVIORAL HEALTH"                                            , "TX"   , "PLMBHTX"                , "1"                ,
    "PAM HEALTH SPECIALTY HOSPITAL 1"    , "PAM SPECIALTY HOSPITAL OF SAN ANTONIO MEDICAL CENTER"               , "TX"   , "PAMSATX"                , "0.85"             ,
    "PARKLAND HOSPITAL"                  , "PARKLAND MEMORIAL HOSPITAL"                                         , "TX"   , "PKLDHTX"                , "0.9"              ,
    "RED RIVER HOSPITAL"                 , "RED RIVER HOSPITAL"                                                 , "TX"   , "RDRVHTX"                , "1"                ,
    "RED RIVER HOSPITAL"                 , "RED RIVER ER AND HOSPITAL"                                          , "TX"   , "RDRVHTX"                , "0.89"             ,
    "RIO GRANDE ST.HOSPITAL"             , "RIO GRANDE REGIONAL HOSPITAL"                                       , "TX"   , "RGHOSTX"                , "0.9"              ,
    "RIO VISTA BEHV HLTH HSP"            , "RIO VISTA BEHAVIORAL HEALTH"                                        , "TX"   , "RVBHHTX"                , "0.79"             ,
    # "SHANNON MEDICAL CENTER"             , "SHANNON MEDICAL CENTER"                                             , "TX"   , "SHNMCTX"                , "1"                ,
    # "SHANNON MEDICAL CENTER"             , "SHANNON MEDICAL CENTER SOUTH"                                       , "TX"   , "SHNMCTX"                , "0.99"             ,
    # "SHANNON MEDICAL CENTER"             , "SHANNON MEDICAL CENTER ST JOHNS CAMPUS"                             , "TX"   , "SHNMCTX"                , "0.86"             ,
    # "SOUTH TEXAS HOSPITAL"               , "SOUTH TEXAS HEALTH SYSTEM"                                          , "TX"   , "STHOSTX"                , "0.78"             ,
    # "SOUTH TEXAS HOSPITAL"               , "METHODIST TEXSAN HOSPITAL - A METHODIST HOSPITAL FACILITY"          , "TX"   , "STHOSTX"                , "0.88"             ,
    # "SOUTH TEXAS HOSPITAL"               , "SOUTH TEXAS HEALTH SYSTEM MCALLEN"                                  , "TX"   , "STHOSTX"                , "0.78"             ,
    "ST LUKE'S BAPTIST HOSPITAL"         , "ST LUKES BAPTIST HOSPITAL"                                          , "TX"   , "SLBHSTX"                , "1"                ,
    # "ST LUKE'S HEALTH - THE WOODLANDS"   , "ST LUKE'S THE WOODLANDS HOSPITAL"                                   , "TX"   , "SLHTWTX"                , "0.94"             ,
    # "ST LUKE'S HEALTH - THE WOODLANDS"   , "CHI ST LUKES HEALTH - SPRINGWOODS VILLAGE"                          , "TX"   , "SLHTWTX"                , "0.75"             ,
    # "ST LUKE'S HEALTH - THE WOODLANDS"   , "CHI ST LUKES HEALTH BAYLOR COLLEGE OF MEDICINE MEDICAL CENTER"      , "TX"   , "SLHTWTX"                , "0.84"             ,
    # "ST LUKE'S HEALTH - THE WOODLANDS"   , "ST LUKES THE WOODLANDS HOSPITAL"                                    , "TX"   , "SLHTWTX"                , "0.94"             ,
    # "TEX HEALTH HUGULEY HOSP"            , "TEXAS HEALTH HUGULEY HOSPITAL FORT WORTH SOUTH"                     , "TX"   , "THHHBTX"                , "0.79"             ,
    # "TEX HEALTH HUGULEY HOSP"            , "TEXAS HEALTH HUGULEY HOSPITAL"                                      , "TX"   , "THHHBTX"                , "0.86"             ,
    "TEXAS VISTA MEDICAL CENTER"         , "TEXAS VISTA MEDICAL CENTER"                                         , "TX"   , "TVMSATX"                , "1"                ,
    "UNIVERSITY HOSPITAL"                , "UNIVERSITY HOSPITAL"                                                , "TX"   , "UVHSATX"                , "1"                ,
    "UNIVERSITY MED CENTER OF EL PASO"   , "UNIVERSITY MEDICAL CENTER OF EL PASO"                               , "TX"   , "UMCEPTX"                , "0.99"             ,
    # "VALLEY BAPTIST HOSPITAL"            , "VALLEY BAPTIST MEDICAL CENTER- BROWNSVILLE"                         , "TX"   , "VBHOSTX"                , "0.91"             ,
    # "VALLEY BAPTIST HOSPITAL"            , "VALLEY BAPTIST MEDICAL CENTER - BROWNSVILLE"                        , "TX"   , "VBHOSTX"                , "0.91"             ,
    # "VALLEY BAPTIST HOSPITAL"            , "VALLEY BAPTIST MEDICAL CENTER"                                      , "TX"   , "VBHOSTX"                , "0.94"             ,
    "WEST OAKS HOSPITAL"                 , "WEST OAKS HOSPITAL"                                                 , "TX"   , "WOAKSTX"                , "1"                ,
    # "CENTRA HEALTH"                      , "CENTRA HEALTH -  LYNCHBURG GEN HOSPITAL"                            , "VA"   , "CENTRVA"                , "0.79"             ,
    # "CENTRA HEALTH"                      , "CENTRA HEALTH"                                                      , "VA"   , "CENTRVA"                , "1"                ,
    "CENTRA SOUTHSIDE COMM HOSP"         , "CENTRA SOUTHSIDE COMMUNITY HOSPITAL"                                , "VA"   , "CSSCHVA"                , "0.96"             ,
    "CHIPPENHAM HOSPITAL - RICHMOND"     , "CHIPPENHAM HOSPITAL"                                                , "VA"   , "CHPHOVA"                , "0.92"             ,
    "JOHNSTON MEMORIAL HOSPITAL"         , "JOHNSTON MEMORIAL HOSPITAL"                                         , "VA"   , "JMTHOVA"                , "1"                ,
    "MARY WASHINGTON HOSPITAL"           , "MARY WASHINGTON HOSPITAL"                                           , "VA"   , "MWASHVA"                , "1"                ,
    "SENTARA CAREPLEX HOSPITAL"          , "SENTARA CAREPLEX HOSPITAL"                                          , "VA"   , "STCHOVA"                , "1"                ,
    "SPOTSYLVANIA REG MED CTR"           , "SPOTSYLVANIA REGIONAL MEDICAL CENTER"                               , "VA"   , "SPOTSVA"                , "0.96"             ,
    "VCU HEALTH TAPPAHANNOCK HOSPITAL"   , "VCU HEALTH TAPPAHANNOCK HOSPITAL"                                   , "VA"   , "VCUHOVA"                , "1"                ,
    "CENTRAL WASHINGTON HOSPITAL"        , "CONFLUENCE HEALTH - CENTRAL WASHINGTON HOSPITAL"                    , "WA"   , "CNTRHWA"                , "0.8"              ,
    "HARBORVIEW MEDICAL CENTER"          , "HARBORVIEW MEDICAL CENTER"                                          , "WA"   , "HAVMCWA"                , "1"                ,
    "HARBORVIEW MEDICAL CENTER"          , "UW MEDICINE - HARBORVIEW MEDICAL CENTER"                            , "WA"   , "HAVMCWA"                , "0.95"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "ST JOSEPH HOSPITAL"                                                 , "WA"   , "STJMCWA"                , "0.89"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "PEACEHEALTH ST JOHN MEDICAL CENTER"                                 , "WA"   , "STJMCWA"                , "0.81"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "PEACEHEALTH ST. JOHN MEDICAL CENTER"                                , "WA"   , "STJMCWA"                , "0.81"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "ST JOSEPH MEDICAL CENTER"                                           , "WA"   , "STJMCWA"                , "1"                ,
    # "ST JOSEPH MEDICAL CENTER"           , "PROVIDENCE ST JOSEPH HOSPITAL"                                      , "WA"   , "STJMCWA"                , "0.87"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "FRANCISCAN HEALTH ST JOSEPH MEDICAL CENTER"                         , "WA"   , "STJMCWA"                , "0.9"              ,
    # "ST JOSEPH MEDICAL CENTER"           , "PROVIDENCE ST. JOSEPH'S HOSPITAL"                                   , "WA"   , "STJMCWA"                , "0.86"             ,
    # "ST JOSEPH MEDICAL CENTER"           , "SAINT JOSEPH HOSPITAL - SOUTH CAMPUS"                               , "WA"   , "STJMCWA"                , "0.87"             ,
    # "TACOMA GENERAL HOSPITAL"            , "TACOMA GENERAL ALLENMORE HOSPITAL"                                  , "WA"   , "TACGHWA"                , "0.87"             ,
    # "TACOMA GENERAL HOSPITAL"            , "MULTICARE TACOMA GENERAL HOSPITAL"                                  , "WA"   , "TACGHWA"                , "0.93"             ,
    "WATERTOWN REGIONAL MEDICAL CENTER"  , "WATERTOWN REGIONAL MEDICAL CENTER"                                  , "WI"   , "WRMEDWI"                , "1"
  )
