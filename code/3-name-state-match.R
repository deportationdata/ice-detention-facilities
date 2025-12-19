library(tidyverse)
library(tidylog)

facility_attributes <- arrow::read_feather(
  "data/facilities-attributes-raw.feather"
)

facilities_from_detentions <- arrow::read_feather(
  "data/facilities-from-detentions.feather"
)

hold_rooms_all <-
  facility_attributes |>
  filter(
    str_detect(str_to_upper(detention_facility_code), "HOLD")
  )

hold_rooms_definitive <-
  hold_rooms_all |>
  filter(
    !is.na(name),
    !is.na(state),
    !is.na(detention_facility_code)
  ) |>
  distinct(detention_facility_code, name, state)

# hold_rooms_all |>
#   anti_join(
#     hold_rooms_definitive,
#     by = "detention_facility_code"
#   ) |>
#   distinct(detention_facility_code, name) |>
#   clipr::write_clip()
# pasted below

# TODO: get verified links
hold_rooms_manual <-
  tribble(
    ~detention_facility_code , ~name                    , ~state ,
    # first three are in vera, commented out for now
    # "DULHOLD"                , "DULUTH HOLD ROOM"       , "MN"   , # https://www.vera.org/ice-detention-trends
    # "RICHOLD"                , "RICHLAND HOLD ROOM"     , "WA"   , # https://www.ice.gov/node/62177
    # "SYRHOLD"                , "SYRACUSE HOLD ROOM"     , "NY"   , # https://www.vera.org/ice-detention-trends
    # "CBPHOLD"                , "BUFFALO USBP HOLD ROOM" , "NY" # https://www.cbp.gov/border-security/along-us-borders/border-patrol-sectors/buffalo-sector-new-york and confirmed arrests are in Buffalo NY via matching to arrests table
  )

hold_rooms_final <-
  bind_rows(
    hold_rooms_definitive,
    hold_rooms_manual
  )

# need to treat hold rooms differently

code_name_state_guesses <-
  facilities_from_detentions |>
  as_tibble() |>
  distinct(detention_facility_code, name = detention_facility) |>
  filter(!str_detect(str_to_upper(detention_facility_code), "HOLD")) |>
  arrange(detention_facility_code, name) |>
  mutate(
    state_guess_end = str_sub(detention_facility_code, -2),
    state_guess_start = str_sub(detention_facility_code, 1, 2),
    state = case_when(
      str_sub(detention_facility_code, 1, 3) == "BOP" ~ NA_character_,
      nchar(detention_facility_code) < 7 ~ NA_character_,
      state_guess_start %in%
        c(state.abb, "PR") &
        state_guess_end %in%
          c(state.abb, "PR") &
        state_guess_start == state_guess_end ~ state_guess_start,
      state_guess_start %in%
        c(state.abb, "PR") &
        state_guess_end %in%
          c(state.abb, "PR") ~ NA_character_,
      state_guess_start %in%
        c(state.abb, "PR") ~ state_guess_start,
      state_guess_end %in%
        c(state.abb, "PR") ~ state_guess_end,
      TRUE ~ NA_character_
    )
  )

code_name_state_definitive <-
  code_name_state_guesses |>
  filter(!is.na(state)) |>
  select(-state_guess_start, -state_guess_end)

# code_name_state_guesses |>
#   filter(nchar(detention_facility_code) < 7) |>
#   select(detention_facility_code, name) |>
#   clipr::write_clip()
# pasted below

# need better verification of the non-BOP facilities
# TODO: fill in the missing ones
code_name_state_short <-
  tribble(
    ~detention_facility_code , ~name                                , ~state ,
    "AGC"                    , "AGUADILLA SPC"                      , "PR"   , # https://www.ice.gov/node/63512
    "BOPATL"                 , "ATLANTA U.S. PEN."                  , "GA"   , # https://www.bop.gov/locations/institutions/atl/
    "BOPBER"                 , "FCI BERLIN"                         , "NH"   , # https://www.bop.gov/locations/institutions/ber/
    "BOPBRO"                 , "BROOKLYN MDC"                       , "NY"   , # https://www.bop.gov/locations/institutions/bro/
    "BOPCAA"                 , "USP CANAAN"                         , "PA"   , # https://www.bop.gov/locations/institutions/caa/
    "BOPCNV"                 , "NASHVILLE COMM. CORR."              , "TN"   , # https://www.bop.gov/locations/ccm/cnv/
    "BOPDEV"                 , "DEVENS FMC"                         , "MA"   , # https://www.bop.gov/locations/institutions/dev/
    "BOPGUA"                 , "GUAYNABO MDC (SAN JUAN)"            , "PR"   , # https://www.bop.gov/locations/institutions/gua/
    "BOPHON"                 , "HONOLULU FEDERAL DETENTION CENTER"  , "HI"   , # https://www.bop.gov/locations/institutions/hon/
    "BOPLOF"                 , "LOMPOC FCI"                         , "CA"   , # https://www.bop.gov/locations/institutions/lom/
    "BOPLOM"                 , "LOMPOC USP"                         , "CA"   , # https://www.bop.gov/locations/institutions/lom/
    "BOPLOS"                 , "LOS ANGELES MET.DET.CTR."           , "CA"   , # https://www.bop.gov/locations/institutions/los/
    "BOPLVN"                 , "LEAVENWORTH USP"                    , "KS"   , # https://www.bop.gov/locations/institutions/lvn/
    "BOPMIM"                 , "MIAMI FED.DET.CENTER"               , "FL"   , # https://www.bop.gov/locations/institutions/mim/
    "BOPMRG"                 , "MORGANTOWN FED.CORR.INST."          , "WV"   , # https://www.bop.gov/locations/institutions/mrg/
    "BOPOAD"                 , "OAKDALE FED.DET.CENTER"             , "LA"   , # https://www.bop.gov/locations/institutions/oad/
    "BOPPET"                 , "PETERSBURG FED.CORR.INST."          , "VA"   , # https://www.bop.gov/locations/institutions/pet/
    "BOPPHL"                 , "FDC PHILADELPHIA"                   , "PA"   , # https://www.bop.gov/locations/institutions/phl/phl_prea.pdf?v=1.0.2
    "BOPPHX"                 , "PHOENIX FED.CORR.INST."             , "AZ"   , # https://www.bop.gov/locations/institutions/phx/
    "BOPRAE"                 , "MCRAE CORRECTIONAL FACILITY, CCA"   , "GA"   , # https://www.bop.gov/news/20221201_ends_use_of_privately_owned_prisons.jsp
    "BOPSET"                 , "SEATAC FED.DET.CENTER"              , "WA"   , # https://www.bop.gov/locations/institutions/set/
    "BOPSPG"                 , "SPRINGFIELD FED.MED.CNTR"           , "MO"   , # https://www.bop.gov/locations/institutions/spg/
    "BOPTCN"                 , "TUCSON FED.CORR.INST."              , "AZ"   , # https://www.bop.gov/locations/institutions/tcn/
    "BOPVIM"                 , "FCI VICTORVILLE"                    , "CA"   , # https://www.bop.gov/locations/institutions/vim/
    "BPC"                    , "BOSTON SPC"                         , "MA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/502/boston-spc-coast-guard-support-facility
    "BTV"                    , "BUFFALO SPC"                        , "NY"   , # https://www.ice.gov/detain/detention-facilities/buffalo-batavia-service-processing-center
    "DHDFNJ"                 , "DELANEY HALL DETENTION FACILITY"    , "NJ"   , # https://www.ice.gov/detain/detention-facilities/delaney-hall-detention-facility
    "EAZ"                    , "ELOY FED CTR FACILITY (CORE CIVIC)" , "AZ"   , # https://www.ice.gov/detain/detention-facilities/eloy-detention-center
    "EPC"                    , "EL PASO SPC"                        , "TX"   , # https://www.ice.gov/detain/detention-facilities/el-paso-service-processing-center
    "EROFCB"                 , "ERO EL PASO CAMP EAST MONTANA"      , "TX"   , # https://www.ice.gov/detain/detention-facilities/camp-east-montana
    "FLO"                    , "FLORENCE SPC"                       , "AZ"   , # https://www.ice.gov/detain/detention-facilities/florence-spc
    "FSF"                    , "FLORENCE STAGING FACILITY"          , "AZ"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1451/florence-staging-facility
    "KRO"                    , "KROME NORTH SPC"                    , "FL"   , # https://www.ice.gov/detain/detention-facilities/krome-north-service-processing-center
    "MSF"                    , "MIAMI STAGING FACILITY"             , "FL"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1493/miami-staging-facility
    "PIC"                    , "PORT ISABEL SPC"                    , "TX"   , # https://www.ice.gov/detain/detention-facilities/port-isabel-service-processing-center
    "RGS"                    , "RIO GRANDE VALLEY STAGING"          , "TX"   , # https://www.ice.gov/detain/detention-facilities/rio-grande-detention-center
    "SJS"                    , "SAN JUAN STAGING"                   , "PR"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1283/san-juan-staging-facility
    "STK"                    , "STOCKTON STAGING FACILITY"          , "CA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1553/stockton-staging-facility
    "VRK"                    , "VRK PRCS"                           , "NY"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/678/varick-federal-detention-centre-formerly-varick-street-service-processing-centre

    # new
    "AFRC"                   , "ARTESIA FAMILY RESIDENTIAL CENTER"  , "NM"   , # https://www.detentionwatchnetwork.org/sites/default/files/reports/DWN%20Expose%20and%20Close%20Artesia%20Report.pdf
    "BOPALF"                 , "ALLENWOOD (LOW) FCI"                , "PA"   , # https://www.bop.gov/locations/institutions/alf/
    "BOPALM"                 , "ALLENWOOD (MED) FCI"                , "PA"   , # https://www.bop.gov/locations/institutions/alm/
    "BOPALP"                 , "ALLENWOOD U.S.PEN."                 , "PA"   , # https://www.bop.gov/locations/institutions/alp/
    "BOPASH"                 , "ASHLAND FED.CORR.INST."             , "KY"   , # https://www.bop.gov/locations/institutions/ash/
    "BOPATW"                 , "U.S.P. ATWATER"                     , "CA"   , # https://www.bop.gov/locations/institutions/atw/
    "BOPBAS"                 , "BASTROP FED.CORR.INST."             , "TX"   , # https://www.bop.gov/locations/institutions/bas/
    "BOPBEC"                 , "BECKLEY  FED.CORR.INST."            , "WV"   , # https://www.bop.gov/locations/institutions/bec/
    "BOPBIG"                 , "BIG SPRING FED.CORR.INST."          , "TX"   , # https://www.bop.gov/locations/institutions/big/
    "BOPBMA"                 , "BEAUMONT FED.CORR.CTR"              , "TX"   , # https://www.globaldetentionproject.org/wp-content/uploads/2021/01/US_Department_of_Homeland_Security_2007_1_1.pdf
    "BOPBML"                 , "BEAUMONT FCC, LOW"                  , "TX"   , # https://www.bop.gov/locations/institutions/bml/
    "BOPBMM"                 , "BEAUMONT FCC, MEDIUM"               , "TX"   , # https://www.bop.gov/locations/institutions/bmm/
    "BOPBMP"                 , "BEAUMONT FEDERAL PRISON"            , "TX"   , # https://www.bop.gov/locations/institutions/bmp/
    "BOPBUT"                 , "BUTNER FED.CORR.INST."              , "NC"   , # https://www.bop.gov/locations/institutions/but/
    "BOPCCC"                 , "CHICAGO MCC"                        , "IL"   , # https://www.bop.gov/locations/institutions/ccc/
    "BOPCOL"                 , "COLEMAN FCI"                        , "FL"   , # https://www.bop.gov/locations/institutions/col/
    "BOPCRW"                 , "CARSWELL FED.MED.CTR"               , "TX"   , # https://www.bop.gov/locations/institutions/crw/
    "BOPCUM"                 , "CUMBERLAND FCI"                     , "MD"   , # https://www.bop.gov/locations/institutions/cum/
    "BOPDAL"                 , "DALBY CORRECTIONAL INSTITUTE"       , "TX"   ,
    "BOPDAN"                 , "DANBURY FED.CORR.INST."             , "CT"   , # https://www.bop.gov/locations/institutions/dan/
    "BOPDTH"                 , "DULUTH FED.PRISON CAMP"             , "MN"   , # https://www.bop.gov/locations/institutions/dth/
    "BOPDUB"                 , "DUBLIN FED.CORR.INST."              , "CA"   , # https://www.bop.gov/locations/institutions/dub/
    "BOPEDG"                 , "EDGEFIELD FED.CORR.INST."           , "SC"   , # https://www.bop.gov/locations/institutions/edg/
    "BOPELK"                 , "ELKTON FED.CORR.INST."              , "OH"   , # https://www.bop.gov/locations/institutions/elk/
    "BOPENG"                 , "ENGLEWOOD FED.CORR.INST."           , "CO"   , # https://www.bop.gov/locations/institutions/eng/
    "BOPERE"                 , "EL RENO FED.CORR.INST."             , "OK"   , # https://www.bop.gov/locations/institutions/ere/
    "BOPEST"                 , "ESTILL FED.CORR.INST."              , "SC"   , # https://www.bop.gov/locations/institutions/est/
    "BOPFAI"                 , "FAIRTON FCI"                        , "NJ"   , # https://www.bop.gov/locations/institutions/fai/
    "BOPFLF"                 , "FLORENCE FED.CORR.INST."            , "CO"   , # https://www.bop.gov/locations/institutions/flf/
    "BOPFLM"                 , "FLORENCE ADMAX US PEN."             , "CO"   , # https://www.bop.gov/locations/institutions/flm/
    "BOPFLP"                 , "FLORENCE US PEN"                    , "CO"   , # https://www.bop.gov/locations/institutions/flp/
    "BOPFOR"                 , "FORREST CITY FED.CORR.INST"         , "AR"   , # https://www.bop.gov/locations/institutions/for/
    "BOPFTD"                 , "FORT DIX FED.CORR.INST."            , "NJ"   , # https://www.bop.gov/locations/institutions/ftd/
    "BOPFTW"                 , "FORT WORTH FED.MED.CENTER"          , "TX"   , # https://www.bop.gov/locations/institutions/ftw/
    "BOPGIL"                 , "FCI GILMER"                         , "WV"   , # https://www.bop.gov/locations/institutions/gil/
    "BOPGRE"                 , "GREENVILLE FCI"                     , "IL"   , # https://www.bop.gov/locations/institutions/gre/
    "BOPJES"                 , "JESUP FED.CORR.INST."               , "GA"   , # https://www.bop.gov/locations/institutions/jes/
    "BOPLEW"                 , "LEWISBURG U.S. PEN."                , "PA"   , # https://www.bop.gov/locations/institutions/lew/
    "BOPLEX"                 , "LEXINTON FED.MED CENTER"            , "KY"   , # https://www.bop.gov/locations/institutions/lex/
    "BOPLOR"                 , "LORETTO FED.CORR.INST."             , "PA"   , # https://www.bop.gov/locations/institutions/lor/
    "BOPLTL"                 , "LA TUNA FEDERAL SATTELITE, LOW"     , ""     ,
    "BOPMAN"                 , "MANCHESTER FED CORR INSTR"          , "KY"   , # https://www.bop.gov/locations/institutions/man/
    "BOPMAR"                 , "MARION USP"                         , "IL"   , # https://www.bop.gov/locations/institutions/mar/
    "BOPMCK"                 , "MCKEAN FED.CORR.INST."              , "PA"   , # https://www.bop.gov/locations/institutions/mck/
    "BOPMCR"                 , "MCCREARY USP"                       , "KY"   , # https://www.bop.gov/locations/institutions/mcr/
    "BOPMEM"                 , "MEMPHIS FED.CORR.INST."             , "TN"   , # https://www.bop.gov/locations/institutions/mem/
    "BOPMIA"                 , "MIAMI FCI (FORMER MCC)"             , "FL"   , # https://www.bop.gov/locations/institutions/mia/
    "BOPMIL"                 , "MILAN FED.CORR.INST."               , "MI"   , # https://www.bop.gov/locations/institutions/mil/
    "BOPMNA"                 , "MARIANNA FED.CORR.INST."            , "FL"   , # https://www.bop.gov/locations/institutions/mna/
    "BOPMVC"                 , "MOSHANNON VALLEY CORRECTIONAL"      , "PA"   , # https://www.ice.gov/detain/detention-facilities/moshannon-valley-processing-center
    "BOPNEO"                 , "NORTHEAST OHIO CORRECTIONAL CENTER" , "OH"   , # https://www.ice.gov/detain/detention-facilities/northeast-ohio-correctional-center
    "BOPNYM"                 , "NEW YORK MCC"                       , "NY"   , # https://www.bop.gov/locations/institutions/nym/
    "BOPOAK"                 , "OAKDALE FED.CORR.INST."             , "LA"   , # https://www.bop.gov/locations/institutions/oak/
    "BOPOKL"                 , "OKLAHOMA FED.TRANSFER CTR"          , "OK"   , # https://www.bop.gov/locations/institutions/okl/
    "BOPOTV"                 , "OTISVILLE FED.CORR.INST."           , "NY"   , # https://www.bop.gov/locations/institutions/otv/
    "BOPOXF"                 , "OXFORD FED.CORR.INST."              , "WI"   , # https://www.bop.gov/locations/institutions/oxf/
    "BOPPEK"                 , "PEKIN FED.CORR.INST."               , "IL"   , # https://www.bop.gov/locations/institutions/pek/
    "BOPPOL"                 , "POLLOCK USP"                        , "LA"   , # https://www.bop.gov/locations/institutions/pol/
    "BOPRBK"                 , "RAY BROOK FED.CORR.INST."           , "NY"   , # https://www.bop.gov/locations/institutions/rbk/
    "BOPRCH"                 , "ROCHESTER FED.MED.CENTER"           , "MN"   , # https://www.bop.gov/locations/institutions/rch/
    "BOPSAF"                 , "SAFFORD FED.CORR.INST."             , "AZ"   , # https://www.bop.gov/locations/institutions/saf/
    "BOPSCH"                 , "SCHUYLKILL FED.CORR.INST."          , "PA"   , # https://www.bop.gov/locations/institutions/sch/
    "BOPSDC"                 , "SAN DIEGO MCC"                      , "CA"   , # https://www.bop.gov/locations/institutions/sdc/
    "BOPSEA"                 , "SEAGOVILLE FED.CORR.INST."          , "TX"   , # https://www.bop.gov/locations/institutions/sea/
    "BOPSHE"                 , "SHERIDAN FED.CORR.INST."            , "OR"   , # https://www.bop.gov/locations/institutions/she/
    "BOPSST"                 , "SANDSTONE FED.CORR.INST."           , "MN"   , # https://www.bop.gov/locations/institutions/sst/
    "BOPTAL"                 , "TALLAHASSEE FED.CORR.INST"          , "FL"   , # https://www.bop.gov/locations/institutions/tal/
    "BOPTCP"                 , "US PENITENTIARY TUCSON"             , "AZ"   , # https://www.bop.gov/locations/institutions/tcp/
    "BOPTDG"                 , "TALLADEGA FCI"                      , "AL"   , # https://www.bop.gov/locations/institutions/tdg/
    "BOPTEX"                 , "TEXARKANA FED.CORR.INST."           , "TX"   , # https://www.bop.gov/locations/institutions/tex/
    "BOPTHA"                 , "TERRE HAUTE USP"                    , "IN"   , # https://www.bop.gov/locations/institutions/thp/
    "BOPTRV"                 , "THREE RIVERS FCI"                   , "TX"   , # https://www.bop.gov/locations/institutions/trv/
    "BOPWAS"                 , "WASECA FED.CORR.INST."              , "MN"   , # https://www.bop.gov/locations/institutions/was/
    "BOPYAM"                 , "YAZOO CITY MEDIUM FCI"              , "MS"   , # https://www.globaldetentionproject.org/wp-content/uploads/2021/01/US_Department_of_Homeland_Security_2007_1_1.pdf
    "BOPYAZ"                 , "YAZOO CITY FCI"                     , "MS"   , # https://www.globaldetentionproject.org/wp-content/uploads/2021/01/US_Department_of_Homeland_Security_2007_1_1.pdf
    "ECC"                    , "EL CENTRO SPC"                      , "CA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/554/el-centro-detention-facility-service-processing-centre
    "HSF"                    , "HARLINGEN STAGING FACILITY"         , "TX"   , # https://www.ice.gov/node/62072
    "OAK"                    , "OAKDALE FED. DET. CENTER"           , "LA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/682/oakdale-federal-detention-center
    "SPP"                    , "SAN PEDRO SPC"                      , "CA"   , # https://www.globaldetentionproject.org/wp-content/uploads/2021/01/US_Department_of_Homeland_Security_2007_1_1.pdf
    "USMCO"                  , "US MARSHALS, COLORADO"              , "CO"   , # https://www.usmarshals.gov/local-districts
    "USMHI"                  , "US MARSHALS, HAWAII"                , "HI"   , # https://www.usmarshals.gov/local-districts
    "USMMD"                  , "US MARSHALS,MARYLAND"               , "MD"   , # https://www.usmarshals.gov/local-districts
    "USMOR"                  , "US MARSHALS, OREGON"                , "OR"   , # https://www.usmarshals.gov/local-districts
    "USMVT"                  , "US MARSHAL'S, VERMONT"              , "VT" # https://www.usmarshals.gov/local-districts
  ) |>
  filter(!is.na(state), state != "")

# code_name_state_guesses |>
#   filter(is.na(state), nchar(detention_facility_code) == 7) |>
#   select(-state) |>
#   mutate(
#     state = str_c(state_guess_start, " ", state_guess_end),
#     .keep = "unused"
#   ) |>
#   clipr::write_clip()
# pasted below

# coded based on reading the codes and names, and additional research only when ambiguous
code_name_state_manual <-
  tribble(
    ~detention_facility_code , ~name                                , ~state ,
    "ALAMCNC"                , "ALAMANCE CO. DET. FACILITY"         , "NC"   , # https://www.ice.gov/detain/detention-facilities/alamance-county-detention-center
    "ALAMOTX"                , "ALAMO POLICE DEPT."                 , "TX"   ,
    "ALEXAVA"                , "ALEXANDRIA CITY JAIL"               , "VA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/476/alexandria-city-jail
    "ALHAMCA"                , "ALHAMBRA CITY JAIL"                 , "CA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1329/alhambra-city-jail
    "ALLEGNY"                , "ALLEGANY COUNTY JAIL"               , "NY"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1281/allegany-county-jail
    "ARAPACO"                , "ARAPAHOE COUNTY JAIL"               , "CO"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/644/arapahoe-county-jail
    "ARRMCCA"                , "ARROWHEAD REGIONAL MED CENTER"      , "CA"   , # https://www.arrowheadregional.org/
    "CACHEUT"                , "CACHE CO. JAIL"                     , "UT"   , # https://www.ice.gov/node/61445
    "CALHOMI"                , "CALHOMI CALHOUN CO., BATTLE CR,MI"  , "MI"   , # https://www.ice.gov/detain/detention-facilities/calhoun-county-correctional-center
    "CAMBRPA"                , "CAMBRIA COUNTY JAIL, PA"            , "PA"   , # https://www.ice.gov/foia/2021-cambria-county-jail-edensburg-pa-mar-8-12-2021
    "CAMCOWY"                , "CAMPBELL COUNTY JAIL"               , "WY"   , # https://www.ice.gov/detain/detention-facilities/campbell-county-detention-center
    "CAMERTX"                , "CAMERON COUNTY JAIL"                , "TX"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1419/cameron-county-jail
    "CARDFVA"                , "CAROLINE DETENTION FACILITY"        , "VA"   , # https://www.ice.gov/detain/detention-facilities/caroline-detention-facility
    "CASCAMT"                , "CASCADE COUNTY JAIL, MT"            , "MT"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/597/cascade-county-jail-montana
    "CASSCNE"                , "CASS COUNTY JAIL"                   , "NE"   , # https://www.ice.gov/foia/2020-cass-county-jail-plattsmouth-ne-aug-10-13-2020
    "COBBJGA"                , "COBB CO SHERIFFS OFC ADULT DET CTR" , "GA"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1288/cobb-county-jail
    "COLCASC"                , "COLUMBIA CARE CENTER"               , "SC"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1429/columbia-care-center
    "COLLIFL"                , "COLLIER COUNTY SHERIFF"             , "FL"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/1290/collier-county-naples-jail-center
    "COLUMFL"                , "COLUMBIA COUNTY JAIL"               , "FL"   , # https://columbiasheriff.org/detention-facility/
    "CONHOTX"                , "CONNALLY MEMORIAL MEDICAL CENTER"   , "TX"   ,
    "COXMEMO"                , "COXHEALTH MEDICAL CENTER SOUTH"     , "MO"   , # https://www.coxhealth.com/our-locations/cox-medical-center-south/
    "CWHCCMP"                , "COMMONWEALTH HEALTHCARE CORP"       , "MP"   , # https://www.chcc.health/
    "DEAPDMI"                , "DEARBORN POLICE DEPT."              , "MI"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/543/dearborn-police-department
    "DEKALAL"                , "DEKALB COUNTY DETENTION CENTER"     , "AL"   , # https://www.ice.gov/foia/dekalb-county-detention-center-al
    "DENVECO"                , "DENVER COUNTY JAIL"                 , "CO"   , # https://www.denvercriminalattorneylawyer.com/colorado-jail-prison-information/denver-county-jail
    "ELZICDF"                , "ELIZABETH CONTRACT D.F."            , "NJ"   , # https://www.ice.gov/detain/detention-facilities/elizabeth-contract-detention-facility
    "FLORHAZ"                , "FLORENCE HOSPITAL"                  , "AZ"   ,
    "FLOYDGA"                , "FLOYD COUNTY JAIL"                  , "GA"   , # https://www.floydsheriffga.gov/jail-detention
    "GALLAMT"                , "GALLATIN COUNTY JAIL"               , "MT"   , # https://gallatincountysheriff.com/detention-center/
    "GARVIOK"                , "GARVIN COUNTY JAIL"                 , "OK"   , # https://www.ice.gov/node/64728
    "GASTNNC"                , "GASTON COUNTY JAIL"                 , "NC"   , # https://www.globaldetentionproject.org/countries/americas/united-states/detention-centres/589/gaston-county-jail
    "GTMOACU"                , "WINDWARD HOLDING FACILITY"          , "CU"   , # note this is CU = Cuba
    "GTMOBCU"                , "MIGRANT OPS CENTER EAST"            , "CU"   , # note this is CU = Cuba
    "GTMODCU"                , "MIGRANT OPS CENTER MAIN AV622"      , "CU"   , # note this is CU = Cuba
    "GUAMHGU"                , "GUAM MEMORIAL HOSPITAL"             , "GU"   ,
    "GUDOCHG"                , "DEPT OF CORRECTIONS-HAGATNA"        , "GU"   , # https://doc.guam.gov/contact-us/
    "HICOJFL"                , "HILLSBOROUGH COUNTY JAIL"           , "FL"   ,
    "HIDALTX"                , "HIDALGO COUNTY SHERIFF'S JAIL"      , "TX"   ,
    "HOUICDF"                , "HOUSTON CONTRACT DET.FAC."          , "TX"   , # ICE website
    "INMIAMI"                , "MIAMI CORRECTIONAL CENTER"          , "IN"   , # https://www.ice.gov/detain/detention-facilities/miami-correctional-facility-mcf
    "INTEGOK"                , "INTEGRIS HEALTH BAPTIST MEDICAL"    , "OK"   ,
    "INTPOOK"                , "INTEGRIS HEALTH PONCA CITY HOSPITA" , "OK"   ,
    "LACKAPA"                , "LACKAWANA CNTY JAIL, PA"            , "PA"   ,
    "LAKCOFL"                , "LAKE COUNTY JAIL"                   , "FL"   ,
    "LAKMCFL"                , "LAKESIDE MEDICAL CENTER"            , "FL"   ,
    "LAPAZAZ"                , "LA PAZ COUNTY ADULT DET. FAC."      , "AZ"   ,
    "LARCOWY"                , "LARAMIE COUNTY JAIL"                , "WY"   ,
    "LARELKY"                , "LAUREL COUNTY CORRECTIONS"          , "KY"   ,
    "LASALTX"                , "LASALLE COUNTY JAIL"                , "TX"   ,
    "LAWRESD"                , "LAWRENCE CO. JAIL, SD"              , "SD"   ,
    "LRDICDF"                , "LAREDO PROCESSING CENTER"           , "TX"   , # https://www.ice.gov/detain/detention-facilities/laredo-detention-center
    "MADISMS"                , "MADISON CO. JAIL, MS."              , "MS"   ,
    "MAHONOH"                , "MAHONING COUNTY JAIL"               , "OH"   ,
    "MANATFL"                , "MANATEE COUNTY DETENTION-ANNEX"     , "FL"   ,
    "MARICOR"                , "MARION CO JAIL"                     , "OR"   ,
    "MARIOFL"                , "MARION COUNTY JAIL"                 , "FL"   ,
    "MARIOIN"                , "MARION COUNTY JAIL"                 , "IN"   ,
    "MARTIFL"                , "MARTIN COUNTY JAIL"                 , "FL"   ,
    "MAVERTX"                , "MAVERICK COUNTY JAIL"               , "TX"   ,
    "MDLNDNE"                , "CHI HEALTH MIDLANDS HOSPITAL"       , "NE"   ,
    "MEHOSCA"                , "MERCY HOSP DOWNTOWN BAKERSFIELD"    , "CA"   ,
    "MEMCRCA"                , "MERCY MEDICAL CTR REDDING"          , "CA"   ,
    "MEMHOCA"                , "MEMORIAL HOSPITAL BAKERSFIELD"      , "CA"   ,
    "MEMRHFL"                , "MEMORIAL REGIONAL HOSP."            , "FL"   ,
    "MERCYFL"                , "MERCY HOSPITAL"                     , "FL"   ,
    "MESJACO"                , "MESA COUNTY JAIL"                   , "CO"   ,
    "METHOTX"                , "METHODIST HOSPITAL"                 , "TX"   ,
    "METROFL"                , "METRO DADE JAIL"                    , "FL"   ,
    "MEYTHCT"                , "MAINE YOUTH CENTER - LONG CREEK"    , "ME"   ,
    "MIDLATX"                , "MIDLAND DETENTION CENTER"           , "TX"   ,
    "MIDMHTX"                , "MIDLAND MEMORIAL HOSPITAL"          , "TX"   ,
    "MILLRAR"                , "MILLER COUNTY JAIL"                 , "AR"   ,
    "MINICID"                , "MINICASSIA DET. CENTER"             , "ID"   ,
    "MINNESD"                , "MINNEHAHA COUNTY JAIL"              , "SD"   ,
    "MIRHPRI"                , "MIRIAM HOSPITAL"                    , "RI"   ,
    "MNROEMI"                , "MONROE COUNTY SHERIFF/JAIL-MAIN"    , "MI"   ,
    "MNTGMAL"                , "MONTGOMERY CITY JAIL"               , "AL"   ,
    "MOFFECO"                , "MOFFAT COUNTY JAIL"                 , "CO"   ,
    "MONMONJ"                , "MONMOUTH COUNTY JAIL"               , "NJ"   ,
    "MONROFL"                , "MONROE COUNTY JAIL"                 , "FL"   ,
    "MONROMI"                , "MONROE COUNTY DETENTION-DORM"       , "MI"   ,
    "MONTGAL"                , "MONTGOMERY CO MAC SIM BUTLER D.F."  , "AL"   ,
    "MONTGNY"                , "MONTGOMERY COUNTY JAIL"             , "NY"   ,
    "MONTGTX"                , "MONTGOMERY COUNTY JAIL"             , "TX"   ,
    "MPSIPAN"                , "SAIPAN DEPARTMENT OF CORRECTIONS"   , "MP"   ,
    "MSTHSTX"                , "METHODIST SPECIALTY TRANSPLANT HOS" , "TX"   ,
    "MSVPCPA"                , "MOSHANNON VALLEY PROCESSING CENTER" , "PA"   ,
    "MTGPCTX"                , "MONTGOMERY PROCESSING CTR"          , "TX"   ,
    "MTHOSTX"                , "METHODIST HOSPITAL SOUTH"           , "TX"   ,
    "MTNITPA"                , "MOUNT NITTANY MEDICAL CENTER"       , "PA"   ,
    "NEBHSTX"                , "NORTHEAST BAPTIST HOSPITAL"         , "TX"   ,
    "NMCOMNE"                , "NEBRASKA MED CTR OMAHA"             , "NE"   ,
    "NYEPANV"                , "NYE COUNTY SHERIFF-PAHRUMP"         , "NV"   ,
    "OKMHKTX"                , "OTTO KAISER MEMORIAL HOSPITAL"      , "TX"   ,
    "ORANGFL"                , "ORANGE COUNTY JAIL"                 , "FL"   ,
    "ORANGNY"                , "ORANGE COUNTY JAIL"                 , "NY"   ,
    "PALMBFL"                , "PALM BEACH COUNTY JAIL"             , "FL"   ,
    "PAMSATX"                , "PAM HEALTH SPECIALTY HOSPITAL 1"    , "TX"   ,
    "PRESBNY"                , "NEW YORK PRESBYTERIAN HOSPITAL"     , "NY"   ,
    "PRLDCTX"                , "PRAIRIELAND DETENTION CENTER"       , "TX"   ,
    "PRSHANM"                , "PRESBYTERIAN HOSP ABQ"              , "NM"   ,
    "PRVMDKS"                , "PROVIDENCE MEDICAL GROUP"           , "KS"   ,
    "SCOTTIA"                , "SCOTT COUNTY JAIL"                  , "IA"   ,
    "SCOTTNE"                , "SCOTTS BLUFF CO JAIL"               , "NE"   ,
    "SCVMCCA"                , "SHARP CHULA VISTA MEDICAL CENTER"   , "CA"   ,
    "VALVETX"                , "VAL VERDE DETENTION CENTER"         , "TX"   ,
    "WAJAIMN"                , "WASHINGTON COUNTY JAIL"             , "MN"   ,
    "WAKULFL"                , "WAKULLA COUNTY JAIL"                , "FL"   ,
    "WALTNFL"                , "WALTON COUNTY D.O.C."               , "FL"   ,
    "WARDCND"                , "WARD COUNTY JAIL"                   , "ND"   ,
    "WASHCUT"                , "WASHINGTON COUNTY JAIL"             , "UT"   ,
    "WASHIAR"                , "WASHINGTON COUNTY JAIL"             , "AR"   ,
    "WASHIMD"                , "WASHINGTON CO. DETENTION"           , "MD"   ,
    "WASHONV"                , "WASHOE COUNTY JAIL"                 , "NV"   ,
    "WAUKEWI"                , "WAUKESHA COUNTY JAIL"               , "WI"   ,
    "WILSOTX"                , "WILSON COUNTY JAIL"                 , "TX"   ,
    "WYATTRI"                , "WYATT DETENTION CENTER"             , "RI"   ,

    # new
    "AKANVIL"                , "ANVIL MOUNTAIN CC, NOME,"           , "IL"   ,
    "AKCCANC"                , "STATE COR CTR ANNEX ANCHO"          , "AK"   ,
    "AKPSYNJ"                , "ANNE KLEIN PHSYCHIATRIC INSTITUTE"  , "NJ"   ,
    "ALAMOCO"                , "ALAMOSA COUNTY JAIL"                , "CO"   ,
    "ALBANWY"                , "ALBANY COUNTY JAIL"                 , "WY"   ,
    "ALBCONY"                , "ALBANY COUNTY JAIL"                 , "NY"   ,
    "ALBEMVA"                , "ALBEMARLE-CHARLOTTESVILLE"          , "VA"   ,
    "ALESSAZ"                , "STES ON SCOTTSDALE CASA DE ALEGRIA" , "AZ"   ,
    "ALLEGPA"                , "ALLEGHENY CO. JAIL"                 , "PA"   ,
    "ALLENIN"                , "ALLEN COUNTY JAIL"                  , "IN"   ,
    "ARCHUCO"                , "ARCHULETA COUNTY JAIL"              , "CO"   ,
    "ARLCOVA"                , "ARLINGTON CO JAIL"                  , "VA"   ,
    "AROOSME"                , "AROOSTOOK COUNTY JAIL"              , "ME"   ,
    "ARTESNM"                , "ARTESIA CITY JAIL"                  , "NM"   ,
    "ARTSINM"                , "ARTESIA LAW ENFORCEMENT CENTER"     , "NM"   ,
    "CACFBAK"                , "BAKER CCF"                          , "CA"   ,
    "CADCCAL"                , "CDC CALIPATRIA IHP"                 , "CA"   , # https://www.cdcr.ca.gov/facility-locator/cal/
    "CAJUVMN"                , "CARVER CO. JUVY DETENTION"          , "MN"   , # https://www.carvercountymn.gov/departments/court-services-probation/juvenile-division
    "CALCALA"                , "CALCASIEU PARISH PRISON"            , "LA"   ,
    "CALDWMO"                , "CALDWELL COUNTY JAIL"               , "MO"   ,
    "CALDWTX"                , "CALDWELL COUNTY JAIL"               , "TX"   ,
    "CALHOTX"                , "CALHOUN COUNTY JAIL"                , "TX"   ,
    "CALLAMO"                , "CALLAWAY COUNTY JAIL"               , "MO"   ,
    "CAMDENJ"                , "CAMDEN COUNTY JAIL"                 , "NJ"   ,
    "CAMEDAZ"                , "CENTRAL ARIZONA MEDICAL"            , "AZ"   ,
    "CANADOK"                , "CANADIAN COUNTY, EL RENO,"          , "OK"   ,
    "CANYNID"                , "CANYON COUNTY JAIL"                 , "ID"   ,
    "CARBOPA"                , "CARBON COUNTY CORRECTIONL"          , "PA"   ,
    "CARBOWY"                , "CARBON COUNTY JAIL"                 , "WY"   ,
    "CARJAMN"                , "CARVER COUNTY JAIL"                 , "MN"   ,
    "CARROIA"                , "CARROL COUNTY JAIL"                 , "IA"   ,
    "CARROMD"                , "CARROLL COUNTY DETENTION CENTER"    , "MD"   ,
    "CARTEOK"                , "CARTER COUNTY, ARDMORE OK"          , "OK"   ,
    "CASPCAL"                , "CALIPATRIA STATE PRISON"            , "AL"   ,
    "CASSCIA"                , "CASS CO, IA, CORR FACILIT"          , "IA"   ,
    "CASSCND"                , "CASS COUNTY JAIL"                   , "ND"   ,
    "CATAHLA"                , "CATAHOULA CORRECTIONAL CENTER"      , "LA"   ,
    "CATTANY"                , "CATTARAUGUS COUNTY JAIL"            , "NY"   ,
    "CAYUGNY"                , "CAYUGA COUNTY JAIL"                 , "NY"   ,
    "COCHIAZ"                , "COCHISE CO JAIL BISBEE AZ"          , "AZ"   ,
    "COCONAZ"                , "COCONINO CO DETENTION FACILITY"     , "AZ"   ,
    "CODINSD"                , "CODINGTON CO. JAIL, SD"             , "SD"   ,
    "COJUVAZ"                , "COCHISE COUNTY CHILD CENT"          , "AZ"   ,
    "COLESIL"                , "COLES COUNTY JAIL"                  , "IL"   ,
    "COLFANE"                , "COLFAX CO JAIL"                     , "NE"   ,
    "COLOMNY"                , "COLOMBIA COUNTY JAIL"               , "NY"   ,
    "COLQUGA"                , "COLQUITT COUNTY JAIL"               , "GA"   ,
    "COLUMOR"                , "COLUMBIA COUNTY JAIL"               , "OR"   ,
    "COLUMPA"                , "COLUMBIA COUNTY JAIL"               , "PA"   ,
    "COMALTX"                , "COMAL CTY JAIL"                     , "TX"   ,
    "COMFTFL"                , "COMFORT SUITES HOTEL"               , "FL"   ,
    "CONCOCA"                , "CONTRA COSTA CO. JAIL"              , "CA"   ,
    "CONCOLA"                , "CONCORDIA PARISH C C"               , "LA"   ,
    "CONVEWY"                , "CONVERSE COUNTY JAIL"               , "WY"   ,
    "CONWECA"                , "CONTRA COSTA CO JAIL/WEST"          , "CA"   ,
    "COOCONH"                , "COOS COUNTY JAIL"                   , "NH"   ,
    "COOKETX"                , "COOKE COUNTY JAIL"                  , "TX"   ,
    "COOSCOR"                , "COOS CO. JAIL"                      , "OR"   ,
    "CORCONY"                , "CORTLAND COUNTY JAIL"               , "NY"   ,
    "CORTYTX"                , "CORYELL COUNTY JAIL"                , "TX"   ,
    "COSTACA"                , "COSTA MESA COLLEGE HOSP"            , "CA"   ,
    "COWJVWA"                , "COWLITZ CO. JUV. DET."              , "WA"   ,
    "COWLIWA"                , "COWLITZ COUNTY JAIL"                , "WA"   ,
    "DCDOCDC"                , "DC DEPT OF CORRECTIONS"             , "DC"   ,
    "DEKABGA"                , "DEKALB COUNTY JAIL"                 , "GA"   ,
    "DEKALGA"                , "DEKALB CO JAIL"                     , "GA"   ,
    "DELAWNY"                , "DELAWARE COUNTY JAIL"               , "NY"   ,
    "DELAWOH"                , "DELAWARE COUNTY JAIL"               , "OH"   ,
    "DELAWPA"                , "DELAWARE CO JAIL"                   , "PA"   ,
    "DELTACO"                , "DELTA COUNTY JAIL"                  , "CO"   ,
    "DENCICO"                , "DENVER JUSTICE CENTER"              , "CO"   ,
    "DENIICO"                , "Denver CDF II"                      , "CO"   ,
    "DENTOTX"                , "DENTON COUNTY JAIL"                 , "TX"   ,
    "DESCACA"                , "DESCANSO DETENTION FAC."            , "CA"   ,
    "DESCHOR"                , "DESCHUTES CO JAIL"                  , "OR"   ,
    "DEVILND"                , "DEVILS LAKE LAW ENF CENTR"          , "ND"   ,
    "DEWITIL"                , "DEWITT COUNTY JAIL"                 , "IL"   ,
    "DEWITTX"                , "DE WITT COUNTY JAIL"                , "TX"   ,
    "EPCJUVI"                , "EL PASO SPC JUVENILE"               , "TX"   , # https://juv.epcounty.com/Units/Detention/Default?id=5
    "FLATHMT"                , "FLATHEAD COUNTY JAIL"               , "MT"   ,
    "FLOYDIN"                , "FLOYD COUNTY JAIL"                  , "IN"   ,
    "GAGCONE"                , "GAGE COUNTY JAIL"                   , "NE"   ,
    "GARDENE"                , "GARDEN CO, NE, JAIL"                , "NE"   ,
    "GARFICO"                , "GARFIELD COUNTY JAIL"               , "CO"   ,
    "HIDALNM"                , "HIDALGO COUNTY DETENTION CENTER"    , "NM"   ,
    "HIELPTX"                , "HOLIDAY INN EXPRESS & SUITES"       , "TX"   ,
    "HIESATX"                , "HOLIDAY INN EXP SAN ANTONIO"        , "TX"   ,
    "HIESPAZ"                , "HOLIDAY INN EXP PHOENIX"            , "AZ"   ,
    "HIEXSTX"                , "HOLIDAY INN EXPR & STES PEARSALL"   , "TX"   ,
    "HILLCMT"                , "HILL COUNTY JUSTICE CENTER"         , "MT"   ,
    "HILLSNH"                , "HILLSBOROUGH COUNTY JAIL"           , "NH"   ,
    "HIPANAZ"                , "HAMPTON INN PHO AIRPORT N"          , "AZ"   ,
    "HIPDBAZ"                , "HOLIDAY INN PHO DWTN BALLPARK"      , "AZ"   ,
    "HIRAYTX"                , "HOLIDAY INN EXPRESS & SUITES"       , "TX"   ,
    "HISAXLA"                , "HAMPTON INN & STES ALEXANDRIA"      , "LA"   ,
    "HISDBCA"                , "HOLIDAY INN SND BAYSIDE"            , "CA"   ,
    "HISEPTX"                , "HAMPTON INN & STES ELP AP"          , "TX"   ,
    "HISMCTX"                , "HAMPTON INN & STES MCALLEN"         , "TX"   ,
    "HISPAAZ"                , "HOLIDAY INN & STE PHO AP"           , "AZ"   ,
    "IAHSFTX"                , "GEORGE BUSH INTERCONTINENTAL"       , "TX"   ,
    "IBPSYUM"                , "YUMA BORDER PATROL"                 , "AZ"   , # https://www.cbp.gov/border-security/along-us-borders/border-patrol-sectors/yuma-sector-arizona/yuma-station
    "IDACOIA"                , "IDA COUNTY JAIL"                    , "IA"   ,
    "INDIAFL"                , "INDIAN RIVER COUNTY JAIL"           , "FL"   ,
    "LAFERTX"                , "LA FERIA POLICE DEPT."              , "TX"   ,
    "LAJUVNE"                , "LANCASTER CO. JUVENILE"             , "NE"   ,
    "LAKCOMT"                , "LAKE COUNTY JAIL"                   , "MT"   ,
    "LAKEJCO"                , "LAKE COUNTY JAIL"                   , "CO"   ,
    "LANCACA"                , "MIRA LOMA DET.CENTER"               , "CA"   , # https://www.ice.gov/foia/2004-mira-loma-detention-center-lancaster-ca
    "LANCANE"                , "LANCASTER COUNTY JAIL"              , "NE"   ,
    "LANECOR"                , "LANE CO JAIL"                       , "OR"   ,
    "LAPEEMI"                , "LAPEER CO., LAPEER. MI."            , "MI"   ,
    "LAPLACO"                , "LA PLATA COUNTY JAIL"               , "CO"   ,
    "LARIMCO"                , "LARIMER COUNTY JAIL"                , "CO"   ,
    "LASANCO"                , "LAS ANIMAS COUNTY JAIL"             , "CO"   ,
    "LASWOCA"                , "LAS COLINAS WOMEN DET FAC"          , "CA"   ,
    "LAURITX"                , "LAUREL RIDGE TREATMENT CTR"         , "TX"   ,
    "LAVACTX"                , "LAVACA COUNTY JAIL"                 , "TX"   ,
    "LAWREPA"                , "LAWRENCE COUNTY JAIL"               , "PA"   ,
    # "LIRSJUV"                , "LUTHERAN IMM & REF SERVICES"        , "MD" , # https://2022.lirs.org/#top
    "MACCOMI"                , "MACOMB CO.MT.CLEMENS,MI."           , "MI"   ,
    "MACISHI"                , "MCI SHIRLEY"                        , "MA"   , # https://www.mass.gov/locations/mci-shirley
    "MADISIA"                , "MADISON COUNTY JAIL"                , "IA"   ,
    "MADISID"                , "MADISON COUNTY JAIL"                , "ID"   ,
    "MADISLA"                , "MADISON PARISH DET CENTER"          , "LA"   ,
    "MADISNE"                , "MADISON COUNTY JAIL"                , "NE"   ,
    "MADISNY"                , "MADISON COUNTY JAIL"                , "NY"   ,
    "MALHCOR"                , "MALHEUR COUNTY JAIL"                , "OR"   ,
    "MANSFTX"                , "MANSFIELD L.E. CENTER"              , "TX"   ,
    "MAPLEOH"                , "MAPLE HEIGHTS CITY JAIL"            , "OH"   ,
    "MARINCA"                , "MARIN CO. JAIL"                     , "CA"   ,
    "MARIOIA"                , "MARION COUNTY JAIL"                 , "IA"   ,
    "MARSHIA"                , "MARSHALL COUNTY JAIL"               , "IA"   ,
    "MARTIWA"                , "MARTIN HALL JUVENILE"               , "WA"   ,
    "MARTNTX"                , "MARTIN COUNTY JAIL"                 , "TX"   ,
    "MASONWA"                , "MASON COUNTY JAIL"                  , "WA"   ,
    "MATEWKS"                , "TEWKSBURY STATE HOSPITAL"           , "KS"   ,
    "MEADESD"                , "MEADE CO. JAIL, SD"                 , "SD"   ,
    "MECCWIN"                , "MAINE CORRECTIONAL CENTER"          , "ME"   , # https://www.maine.gov/corrections/mcc
    "MECKLNC"                , "MECKLENBURG (NC) CO JAIL"           , "NC"   ,
    "MEDINOH"                , "MEDINA COUNTY JAIL"                 , "OH"   ,
    "MEDINTX"                , "MEDINA COUNTY JAIL"                 , "TX"   ,
    "MEJUVTX"                , "MEDINA CNTY JUVENILE CORR"          , "TX"   ,
    "MEMPHTN"                , "MEMPHIS/SHELBY COUNTY JUV"          , "TN"   ,
    "MERCECA"                , "MERCED COUNTY JAIL"                 , "CA"   ,
    "MERCEIL"                , "MERCER COUNTY CORRECTIONS"          , "IL"   ,
    "MERCENJ"                , "MERCER COUNTY JAIL"                 , "NJ"   ,
    "MERCEPA"                , "MERCER CO. JAIL, PA"                , "PA"   ,
    "MERRINH"                , "MERRIMACK COUNTY JAIL"              , "NH"   ,
    "MIDBIMA"                , "MIDDLESEX COUNTY H.O.C.-BILLERICA"  , "MA"   ,
    "MIDDLNJ"                , "MIDDLESEX COUNTY JAIL"              , "NJ"   ,
    "MIJUVSD"                , "MINNEHAHA JUVENILE DET CN"          , "SD"   ,
    "MIJUVTX"                , "ROGER HASHEM JUVENILE JUSTICE CTR." , "TX"   ,
    "MINERMT"                , "MINERAL COUNTY JAIL"                , "MT"   ,
    "MISCOMO"                , "MISSISSIPPI COUNTY DETENTION CENTE" , "MO"   ,
    "MISSOMT"                , "MISSOULA COUNTY"                    , "MT"   ,
    "MOHAVAZ"                , "MOHAVE CO CORR CENTER"              , "AZ"   ,
    "MONCJAL"                , "MONTGOMERY CITY JAIL"               , "AL"   ,
    "MONONIA"                , "MONONA COUNTY JAIL, IOWA"           , "IA"   ,
    "MONRONY"                , "MONROE COUNTY JAIL"                 , "NY"   ,
    "MONTECA"                , "MONTEREY CO. JAIL"                  , "CA"   ,
    "MONTECO"                , "MONTEZUMA COUNTY JAIL"              , "CO"   ,
    "MONTGIA"                , "MONTGOMERY COUNTY JAIL"             , "IA"   ,
    "MONTGMD"                , "MONTGOMERY COUNTY DET."             , "MD"   ,
    "MONTGPA"                , "MONTGOMERY CNTY JAIL, PA"           , "PA"   ,
    "MONTRCO"                , "MONTROSE COUNTY JAIL"               , "CO"   ,
    "MORGACO"                , "MORGAN COUNTY JAIL"                 , "CO"   ,
    "MOROWOH"                , "MORROW CO. CORRECTIONAL FACILITY"   , "OH"   ,
    "MORRINJ"                , "MORRIS COUNTY JAIL"                 , "NJ"   ,
    "MOUNTND"                , "MOUNTRAIL CO. JAIL, ND"             , "ND"   ,
    "MOYOUMI"                , "MONROE COUNTY,YOUTHCENTER"          , "MI"   ,
    "MSKGNMI"                , "MUSKEGON COUNTY JAIL"               , "MI"   ,
    "MTPPDCA"                , "MONTEREY PARK POLICE DEPT"          , "CA"   ,
    "NCBHOTX"                , "NORTH CENTRAL BAPTIST HOSPITAL"     , "TX"   ,
    "NEWTOMO"                , "NEWTON COUNTY JAIL"                 , "MO"   ,
    "NEWTOTX"                , "NEWTON COUNTY CORR. CTR."           , "TX"   ,
    "NVJDCVA"                , "NO. VA. JUVENILE DET - ICE"         , "VA"   ,
    "NYCLINC"                , "CLINTON C.F. DANNEMMORA"            , "NY"   , # https://doccs.ny.gov/location/clinton-correctional-facility
    "NYFRANC"                , "FRANKLIN C.F."                      , "NY"   , # https://doccs.ny.gov/location/franklin-correctional-facility
    "NYOGDNC"                , "OGDENSBURG CORRECTIONAL"            , "NY"   , # https://doccs.ny.gov/location/riverview-correctional-facility
    "NYORLNC"                , "ORLEANS CORRECTIONAL"               , "NY"   , # https://doccs.ny.gov/location/orleans-correctional-facility
    "OKANOWA"                , "OKANOGAN CO. JAIL"                  , "WA"   ,
    "ORLEANY"                , "ORLEANS COUNTY JAIL"                , "NY"   ,
    "ORLEAVT"                , "ORLEANS COUNTY JAIL"                , "VT"   ,
    "ORPARLA"                , "ORLEANS PARISH SHERIFF"             , "LA"   ,
    "PACIFCA"                , "PACIFIC FURLOUGH FACILITY"          , "CA"   ,
    "PAGCOIA"                , "PAGE COUNTY JAIL"                   , "IA"   ,
    "PAMREVA"                , "PAMUNKEY REG JAIL"                  , "VA"   ,
    "PARKCMT"                , "PARK COUNTY DET CENTER"             , "MT"   ,
    "PARKCWY"                , "PARK COUNTY JAIL"                   , "WY"   ,
    "PARKJCO"                , "PARK COUNTY JAIL"                   , "CO"   ,
    "PASADCA"                , "PASADENA CITY JAIL"                 , "CA"   ,
    "PASSANJ"                , "PASSAIC COUNTY JAIL"                , "NJ"   ,
    "PAULDGA"                , "PAULDING CO GEORGIA"                , "GA"   ,
    "PAULDOH"                , "PAULDING COUNTY JAIL"               , "OH"   ,
    "PAVHRPR"                , "PAVIA HOSPITAL - HATO REY"          , "PR"   , # need to check
    "PRESITX"                , "PRESIDIO COUNTY JAIL"               , "TX"   ,
    "PROWECO"                , "PROWERS COUNTY JAIL"                , "CO"   ,
    "PRVEGAL"                , "VEGA ALTA DET. FACILITY"            , "AL"   ,
    "QUEICDF"                , "INS/WACKENHUT CON/FAC, NY"          , "NY"   , # need to check
    "RICECKS"                , "RICE COUNTY DET CENTER"             , "KS"   ,
    "RICECMN"                , "RICE COUNTY JAIL"                   , "MN"   ,
    "RICHLND"                , "RICHLAND CO. JAIL, ND"              , "ND"   ,
    "RICHMVA"                , "RICHMOND CITY JAIL"                 , "VA"   ,
    "RIKISNY"                , "RIKERS ISLAND, QUEENS, NY"          , "NY"   ,
    "RIOBLCO"                , "RIO BLANCO COUNTY JAIL"             , "CO"   , # need to check
    "RIOCCCA"                , "RIO COSUMNES CORRECTIONS-RCCC"      , "CA"   ,
    "RIOGRCO"                , "RIO GRANDE COUNTY JAIL"             , "CO"   ,
    "RIVERCA"                , "RIVERSIDE COUNTY SHERIFF"           , "CA"   ,
    "SCANITX"                , "SCAN INC."                          , "TX"   ,
    "SCHNENY"                , "SCHNECTADY COUNTY JAIL"             , "NY"   ,
    "SCHOHNY"                , "SCHOHARIE COUNTY JAIL"              , "NY"   ,
    "SCMENCA"                , "SANTA CLARA CO JAIL/ELMWOOD"        , "CA"   ,
    "SCNORCA"                , "SANTA CLARA CO MAIN JAIL"           , "CA"   ,
    "SCOTTKY"                , "SCOTT COUNTY DETENTION CENTER"      , "KY"   ,
    "SCOTTMN"                , "SCOTT COUNTY JAIL"                  , "MN"   ,
    "SCOTTMO"                , "SCOTT COUNTY JAIL"                  , "MO"   ,
    "SCRUZAZ"                , "SANTA CRUZ CO JAIL AZ"              , "AZ"   ,
    "SCRUZCA"                , "SANTA CRUZ CO MAIN JAIL"            , "CA"   ,
    "SCSOUCA"                , "SANTA CLARA COUNTY JAIL"            , "CA"   ,
    "SDHOSCA"                , "SAN DIEGO AREA HOSPITAL"            , "CA"   ,
    "SDPROCA"                , "S.D. COUNTY PROBATION"              , "CA"   ,
    "SDSPRIN"                , "SPRINGFIELD CORR. FAC SD"           , "SD"   , # https://www.doc.sd.gov/adult-corrections/facilities/mike-durfee-state-prison
    "SEAICDF"                , "SEATTLE CONTRACT DET.FAC."          , "WA"   , # need to check
    "STCANVI"                , "ST X ANNA'S HOPE"                   , "VI"   , # https://www.usviodr.com/procurement/annas-hope-emergency-housing-renovation-buildings-9-10/
    "STCGOVI"                , "ST X GOLDEN GROVE"                  , "VI"   , # https://www.justice.gov/d9/goldengrove_mtrrpt10_3-8-16.pdf
    "STTCJVI"                , "ST T CRIMINAL JUST COMPLX"          , "VI"   , # https://www.justice.gov/usao-vi
    "TXDOMIN"                , "TDC-DOMINGUEZ UNIT"                 , "TX"   ,
    "VALCONE"                , "VALLEY COUNTY JAIL"                 , "NE"   ,
    "VANBUMI"                , "VAN BUREN COUNTY JAIL, MI"          , "MI"   ,
    "WACLATX"                , "CENTRAL TEXAS DETENTION FACILITY"   , "TX"   ,
    "WAJUVMN"                , "WASHINGTON CO. JUVENILE C"          , "MN"   ,
    "WAKECNC"                , "WAKE COUNTY SHERIFF DEPT."          , "NC"   ,
    "WALDOME"                , "WALDO COUNTY JAIL"                  , "ME"   ,
    "WALKEGA"                , "WALKER COUNTY JAIL"                 , "GA"   ,
    "WALKETX"                , "WALKER COUNTY JAIL"                 , "TX"   ,
    "WALSHND"                , "WALSH COUNTY JAIL"                  , "ND"   ,
    "WALWOWI"                , "WALWORTH COUNTY JAIL"               , "WI"   ,
    "WAPLOIA"                , "WAPELLO COUNTY JAIL"                , "IA"   ,
    "WARCOTX"                , "WARD CTY JAIL, MONAHANS"            , "TX"   ,
    "WARREIA"                , "WARREN COUNTY JAIL"                 , "IA"   ,
    "WARREKY"                , "WARREN COUNTY REGIONAL"             , "KY"   ,
    "WARREMO"                , "WARREN COUNTY JUSTICE CTR"          , "MO"   ,
    "WARREPA"                , "WARREN COUNTY JAIL"                 , "PA"   ,
    "WASATUT"                , "WASATCH COUNTY JAIL"                , "UT"   ,
    "WASCONY"                , "WASHINGTON COUNTY JAIL"             , "NY"   ,
    "WASCOTN"                , "WASHINGTON COUNTY DET. CTR"         , "TN"   ,
    "WASHAWY"                , "WASHAKIE COUNTY JAIL"               , "WY"   ,
    "WASHCIA"                , "WASHINGTON COUNTY JAIL"             , "IA"   ,
    "WASHICO"                , "WASHINGTON COUNTY JAIL"             , "CO"   ,
    "WASHIME"                , "WASHINGTON COUNTY JAIL"             , "ME"   ,
    "WASHINE"                , "WASHINGTON CO JAIL, BLAIR"          , "NE"   , # https://www.washcountysheriff.org/jail-list/
    "WASHIPA"                , "WASHINGTON CO. JAIL, PA"            , "PA"   ,
    "WASHTMI"                , "WASHTENAW CO, AN ARBOR,MI"          , "MI"   ,
    "WAUKEIL"                , "WAUKEGAN CITY JAIL"                 , "IL"   ,
    "WAYNEMI"                , "WAYNE COUNTY, DETROIT"              , "MI"   ,
    "WAYNENY"                , "WAYNE COUNTY JAIL"                  , "NY"   ,
    "WAYNEOH"                , "WAYNE COUNTY JAIL"                  , "OH"   ,
    "WICOMMD"                , "WICOMICO CO DET"                    , "MD"   ,
    "WILLCTX"                , "WILLACY COUNTY DETENTION CENTER"    , "TX"   ,
    "WILLITN"                , "WILLIAMSON COUNTY JAIL"             , "TN"   ,
    "WILLITX"                , "WILLIAMSON COUNTY JAIL"             , "TX"   ,
    "WINCOTX"                , "WINKLER COUNTY JAIL"                , "TX"   ,
    "WINWYAZ"                , "WINGATE WYNDHAM CASA ESPERANZA"     , "AZ"   ,
    "WISCOTX"                , "WISE COUNTY JAIL"                   , "TX"   ,
    "WYMCONY"                , "WYOMING COUNTY JAIL"                , "NY"   ,
    "WVIRGVA"                , "WESTERN VIRGINIA REGIONAL JAIL"     , "VA" # https://www.wvarj.org/
  )

name_city_state_match <-
  bind_rows(
    code_name_state_definitive,
    code_name_state_short,
    code_name_state_manual
  ) |>
  mutate(source = "detentions")

arrow::write_feather(
  name_city_state_match,
  "data/facilities-name-state-match.feather"
)
