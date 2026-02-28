library(tidyverse)
library(readxl)

metadata <- read_rds("~/github/deportationdata.org/resources/metadata.rds")

files_not_skip6 <- tribble(
  ~name                              , ~id             , ~skip ,
  "FY23_detentionStats.xlsx"         , "1836545074923" ,     5 ,
  "FY23_detentionStats10312023.xlsx" , "1940908029365" ,     5 ,
  "FY24_detentionStats03292024.xlsx" , "1941082281029" ,     3 ,
  "FY24_detentionStats05132024.xlsx" , "1941086522189" ,     5 ,
  "FY24_detentionStats08292024.xlsx" , "1941082436598" ,     5 ,
  "FY24_detentionStats11222023.xlsx" , "1940906505313" ,     5 ,

  ""                                 , "2134352283737" ,     9 ,
  ""                                 , "2122521469710" ,     9 ,
  ""                                 , "2098254904000" ,     9 ,
  ""                                 , "2084169781953" ,     9 ,
  ""                                 , "2068761030753" ,     9
)

detention_management_files <-
  metadata |>
  filter(
    str_detect(name, "detentionStats")
  ) |>
  left_join(files_not_skip6 |> select(-name), by = "id") |>
  mutate(skip = if_else(is.na(skip), 6L, skip)) |>
  mutate(
    url = glue::glue(
      "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_{id}",
    )
  )

facilities <-
  detention_management_files$url |>
  set_names(detention_management_files$name) |>
  map2_dfr(
    detention_management_files$skip,
    ~ {
      # Download to temporary file first
      dl <- tempfile(fileext = ".xlsx")
      download.file(.x, destfile = dl, mode = "wb")

      shts <- excel_sheets(dl)
      sht <- shts[which(str_detect(shts, "Facilities"))]

      read_excel(
        dl,
        sheet = sht,
        col_types = "text",
        skip = .y
      )
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  mutate(file = basename(file)) |>
  mutate(row = row_number(), .after = "file", .by = "file")
facilities_df <-
  facilities |>
  filter(!is.na(name), !is.na(address)) |>
  mutate(
    fy = str_extract(file, "(?<=FY)\\d{2}"),

    # last 6- or 8-digit run anywhere in the filename
    date_str = str_extract_all(file, "\\d{6,8}") |>
      map_chr(~ dplyr::last(.x, default = NA_character_)),

    # parse:
    # - 8 digits: assume MMDDYYYY
    # - 6 digits: if first two <= 12 => MMDDYY, else => YYMMDD
    date_raw = case_when(
      str_length(date_str) == 8 ~ mdy(date_str),

      str_length(date_str) == 6 ~ {
        mm <- suppressWarnings(as.integer(str_sub(date_str, 1, 2)))
        if_else(
          !is.na(mm) & mm <= 12L,
          mdy(date_str), # MMDDYY
          ymd(date_str) # YYMMDD
        )
      },

      TRUE ~ as.Date(NA)
    ),

    date = coalesce(date_raw, as.Date(str_c("20", fy, "-12-31")))
  ) |>
  select(-date_str, -date_raw)

# correct data errors
facilities_df <-
  facilities_df |>
  mutate(
    # a few names have footnotes in them in the excel, remove those numbers
    name = str_remove_all(name, "\\d"),
    # misspelling
    city = case_match(
      city,
      "COTTONWOOD FALL" ~ "COTTONWOOD FALLS",
      "Cottonwood Fall" ~ "COTTONWOOD FALLS",
      .default = city
    ),
    name = case_match(
      name,
      "NORTHWEST ICE PROCESSSING CENTER" ~ "NORTHWEST ICE PROCESSING CENTER",
      "NORTHWEST ICE PROCSESING CENTER" ~ "NORTHWEST ICE PROCESSING CENTER",
      .default = name
    ),
    city = case_match(
      city,
      "BINGHAMPTON" ~ "BINGHAMTON",
      "COTTONWOOD FALL" ~ "COTTONWOOD FALLS",
      "SAULT STE MARIE" ~ "SAULT SAINTE MARIE",
      .default = city
    ),
    address = case_match(
      address,
      "911 PARR BLVD 775 328 3308" ~ "911 PARR BOULEVARD",
      "1001 CENTER WAY" ~ "1001 CENTRE WAY",
      "11866 HASTINGS BRIDGE ROAD" ~ "11866 HASTINGS BRIDGE ROAD P.O. BOX 429",
      "4909 Fm 2826" ~ "4909 Fm (farm To Market) 2826",
      "203 ASPINAL AVE. PO BOX 3236" ~ "203 ASPINALL AVE",
      "3130 OAKLAND ST" ~ "3130 N OAKLAND ST",
      "11866 HASTINGS BRIDGE ROAD P.O. BOX 429" ~ "11866 HASTINGS BRIDGE RD",
      "11866 Hastings Bridge Road P.o. Box 429" ~ "11866 HASTINGS BRIDGE RD",
      "RD. 2, BOX 1" ~ "112 NORTHERN REGIONAL CORRECTIONAL DR", # po box can't be geocoded
      .default = address
    ),
    city = case_when(
      name %in%
        c("RICHWOOD CORRECTIONAL CENTER", "Richwood Correctional Center") &
        city %in% c("Richwood", "RICHWOOD") &
        state == "LA" ~
        "MONROE",
      TRUE ~ city
    ),
    address = case_when(
      address %in%
        c("419 SHOEMAKER ROAD", "419 Shoemaker Road") &
        city %in% c("LOCK HAVEN", "Lock Haven") &
        state == "PA" ~
        "58 PINE MOUNTAIN ROAD",
      TRUE ~ address
    ),
    city = case_when(
      address %in%
        c("58 PINE MOUNTAIN ROAD") &
        city %in% c("LOCK HAVEN", "Lock Haven") &
        state == "PA" ~
        "MCELHATTAN",
      TRUE ~ city
    ),
  )

facilities_df <-
  facilities_df |>
  slice_max(
    order_by = date,
    n = 1,
    with_ties = FALSE,
    by = c("name", "city", "state")
  )

arrow::write_feather(
  facilities_df,
  "data/facilities-detention-management.feather"
)
