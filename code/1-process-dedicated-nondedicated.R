library(tidyverse)
library(readxl)

file_metadata <- read_rds(
  "~/github/deportationdata.org/resources/metadata.rds"
)

detention_management_files <-
  file_metadata |>
  filter(str_starts(path, "ice/facilities_dedicated_nondedicated")) |>
  mutate(
    url = glue::glue(
      "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_{id}",
    )
  ) |>
  distinct(name, .keep_all = TRUE)

facilities <-
  detention_management_files$url |>
  set_names(detention_management_files$name) |>
  map_dfr(
    ~ {
      # Download to temporary file first
      dl <- tempfile(fileext = ".xlsx")
      download.file(.x, destfile = dl, mode = "wb")

      shts <- excel_sheets(dl)

      file_date <-
        readxl::read_excel(
          dl,
          sheet = 1,
          col_types = "text",
          n_max = 3
        ) |>
        slice(3) |>
        select(1) |>
        # set names so it's date_str as the first column
        set_names("date_str") |>
        mutate(
          file_original_date = str_extract(
            date_str,
            "\\d{1,2}/\\d{1,2}/\\d{4}"
          ) |>
            as.Date(format = "%m/%d/%Y"),
          .keep = "unused"
        )

      df <-
        shts |>
        # filter to sheets with Dedicated in the name
        keep(~ str_detect(.x, regex("dedicated", ignore_case = TRUE))) |>
        set_names() |>
        map_dfr(
          ~ {
            skp <-
              readxl::read_excel(
                dl,
                sheet = .x,
                col_types = "text",
                skip = 0,
                n_max = 10
              ) |>
              rowwise() |>
              summarize(
                na_count = sum(!is.na(c_across(everything())))
              ) |>
              # find the first row with greater than 2 non-NA values using mutate so I can extract the row number
              mutate(row_num = row_number()) |>
              filter(na_count >= 5) |>
              slice(1) |>
              pull(row_num)

            readxl::read_excel(dl, sheet = .x, col_types = "text", skip = skp)
          },
          .id = "sheet_original"
        ) |>
        bind_cols(file_date)

      print(colnames(df))

      df
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  mutate(file_original = basename(file), .keep = "unused") |>
  mutate(
    row_original = row_number()
  ) |>
  relocate(
    file_original,
    file_original_date,
    sheet_original,
    row_original
  )

sept522_df <-
  facilities |>
  filter(
    file_original_date == as.Date("2022-09-05"),
    sheet_original == "ICE Non-Dedicated Facilities."
  ) |>
  set_names(
    # move all names to the right by one from name to the end
    c(
      "file_original",
      "file_original_date",
      "sheet_original",
      "row_original",
      "detention_facility_code",
      "name",
      "address",
      "state",
      "zip",
      'aor',
      'type_detailed',
      'male_female',
      'fy24_adp',
      'over_under_72_status',
      'last_inspection_standard',
      'last_inspection_date',
      'type',
      'fy21_adp',
      'number_of_medical_mental_health_personnel',
      'fy22_adp',
      'x15',
      'x16',
      'x17',
      'x18',
      'x19',
      'x20',
      'fy20_adp',
      'type_detailed_2',
      'fy18_adp',
      'fy19_adp',
      'unknown'
    )
  )

facilities_df <-
  facilities |>
  filter(
    !(file_original_date == as.Date("2022-09-05") &
      sheet_original == "ICE Non-Dedicated Facilities.")
  ) |>
  bind_rows(sept522_df) |>
  filter(str_sub(name, 1, 2) != "**" & !is.na(name)) |>
  slice_max(
    order_by = file_original_date,
    n = 1,
    with_ties = FALSE,
    by = c("name", "city", "state")
  ) |>
  rename(
    over_under_72 = over_under_72_status,
    date = file_original_date
  )

arrow::write_feather(
  facilities_df,
  "data/facilities-dedicated-nondedicated.feather"
)
