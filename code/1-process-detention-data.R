library(tidyverse)
library(readxl)
library(tidylog)

detentions_current <- arrow::read_feather(
  # "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
  "~/github/ice/data/detention-stints-latest.feather"
) |>
  mutate(date = as.Date("2025-10-15"))

detentions_2012_2023 <- arrow::read_feather(
  # "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-detentions-2012-2023.feather"
  "~/github/ice/data/ice-detentions-2012-2023.feather"
) |>
  mutate(date = as.Date("2023-11-15"))

date_fmt <- "%m/%d/%y %I:%M %p"
detentions_hrw <-
  read_csv(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/A Costly Move report/foia_10_2554_527_NoIDS.csv",
    col_types = list(
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_datetime(format = date_fmt),
      col_datetime(format = date_fmt),
      col_datetime(format = date_fmt),
      col_character(),
      col_datetime(format = date_fmt),
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_character(),
      col_integer()
    )
  )

detentions_hrw_df <-
  detentions_hrw |>
  janitor::clean_names() |>
  mutate(date = as.Date("2010-01-01"))

detentions_hrw_additional_1 <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/Excel 1 RIF Copy.xlsx",
    skip = 21
  ) |>
  janitor::clean_names() |>
  mutate(date = max(history_intake_date, na.rm = TRUE))

detentions_hrw_additional_2 <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/Excel 2 RIF Copy.xlsx",
    skip = 21
  ) |>
  janitor::clean_names() |>
  mutate(date = max(history_intake_date, na.rm = TRUE))

detentions_hrw_additional_3 <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/Excel 3 RIF Copy.xlsx",
    skip = 21
  ) |>
  janitor::clean_names() |>
  mutate(date = max(history_intake_date, na.rm = TRUE))

detentions_hrw_additional_4 <-
  readxl::read_excel(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/Excel 4 RIF Copy.xlsx",
    skip = 21
  ) |>
  janitor::clean_names() |>
  mutate(date = max(history_intake_date, na.rm = TRUE))

detentions_hrw_additional_5 <-
  read_csv(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/HRW January 2026/From Emily/foia_10_2554_527_NoIDS.csv"
  ) |>
  janitor::clean_names() |>
  mutate(across(contains("date"), ~ mdy_hm(.x) |> as.Date())) |>
  mutate(date = as.Date("2010-05-25"))

col_defs_41855 <-
  c(
    "Arresting Agency" = "text",
    "Arresting Agency Group" = "text",
    "Book-in Criminality" = "text",
    "Processing Disposition" = "text",
    "Detention Facility Type...5" = "text",
    "Initial Book In Date Time" = "date",
    "Book in Date And Time" = "date",
    "Book Out Date Time" = "date",
    "Release Reason" = "text",
    "Final Book Out Date Time" = "date",
    "Final Release Reason" = "text",
    "Detention Facility" = "text",
    "Detention Facility Code" = "text",
    "Detention Facility Type...14" = "text",
    "Detention Facility Type Detailed" = "text",
    "Detention Facility Address" = "text",
    "Detention Facility County" = "text",
    "Detention Facility City" = "text",
    "Detention Facility State Code" = "text",
    "Detention Facility Zip Code" = "text",
    "Detention Facility Male Female" = "text",
    "FSC or Adult Facility" = "text",
    "Detention Facility Over Under 72" = "text",
    "Detention Facility Ice Funded" = "text",
    "Book in DCO" = "text",
    "Gender" = "text",
    "Birth Country" = "text",
    "Citizenship Country" = "text",
    "Race" = "text",
    "Ethnicity" = "text",
    "Birth Date" = "text",
    "Birth Year" = "numeric",
    "Unaccompanied Juvenile Yes No" = "logical",
    "Case Family Status" = "text",
    "Entry Date" = "date",
    "Latest Entry Date" = "date",
    "Entry Status" = "text",
    "Time Illegal In Us" = "text",
    "Latest Entry Status" = "text",
    "Admission Class" = "logical",
    "Visa Abuse Yes No" = "text",
    "Student Violator Yes No" = "logical",
    "Non Immigrant Overstay Yes No" = "logical",
    "Non Immigrant Status Violation Yes No" = "logical",
    "Most Serious Conviction Charge Code" = "text",
    "Most Serious Conviction Charge" = "text",
    "Most Serious Conviction Criminal Charge Status" = "text",
    "Most Serious Conviction Date" = "date",
    "Most Serious Conviction Sentence Years" = "numeric",
    "Most Serious Conviction Sentence Months" = "numeric",
    "Most Serious Conviction Sentence Days" = "numeric",
    "Most Serious Conviction Crime Class" = "text",
    "Most Serious Conviction Charge Date" = "date",
    "Most Serious Criminal Charge" = "text",
    "Most Serious Criminal Charge Code" = "text",
    "Most Serious Criminal Charge Date" = "date",
    "Most Serious Criminal Charge Status" = "text",
    "Most Serious Charge Conviction Date" = "date",
    "Most Serious Sentence Years" = "numeric",
    "Most Serious Sentence Months" = "numeric",
    "Most Serious Sentence Days" = "numeric",
    "Most Serious Crime Class" = "text",
    "Most Serious Pending  Charge" = "text",
    "Most Serious Pending Charge Code" = "text",
    "Most Serious Pending Charge Date" = "date",
    "Most Serious Pending Criminal Charge Status" = "text",
    "Most Serious Pending Conviction Date" = "date",
    "Most Serious Pending Sentence Years" = "logical",
    "Most Serious Pending Sentence Months" = "logical",
    "Most Serious Pending Sentence Days" = "numeric",
    "Most Serious Pending Crime Class" = "text",
    "Apprehension Threat Level" = "text",
    "Mandatory Detention Yes No" = "text",
    "Apprehension Method" = "text",
    "Apprehension Date" = "date",
    "Apprehension AOR" = "text",
    "Apprehension Site" = "text",
    "Lead Source" = "text",
    "Apprehension Landmark" = "text",
    "Detention Stay ID" = "text",
    "Detention ID" = "text",
    "Apprehension ID" = "text",
    "EID Subject ID" = "text",
    "EID Civilian ID" = "text",
    "EID Case ID" = "text",
    "Alien File Number" = "text",
    "Alien Number Unique Identifier" = "text",
    "EID Person ID" = "text"
  )

detentions_41855 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release (ICE FOIA Library)/2024-ICFO-41855/",
    full.names = TRUE
  ) |>
  set_names() |>
  map(
    ~ readxl::read_excel(.x, col_types = col_defs_41855, skip = 4),
    .id = "file"
  ) |>
  bind_rows(.id = "file_original") |>
  janitor::clean_names() |>
  mutate(date = as.Date("2024-01-01"))

detentions_05655 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release (ICE FOIA Library)/2024-ICFO-05655",
    full.names = TRUE
  ) |>
  set_names() |>
  map(
    ~ readxl::read_excel(
      .x, #col_types = col_defs_41855,
      skip = 4
    ),
    .id = "file"
  ) |>
  bind_rows(.id = "file_original") |>
  janitor::clean_names() |>
  mutate(date = as.Date("2024-02-01"))

# facilities_51185 <-
#   read_csv(
#     "inputs/facilities-foia-51185.csv",
#   ) |>
#   janitor::clean_names() |>
#   mutate(row_original = row_number(), .before = 0) |>
#   distinct(name) |>
#   mutate(date = as.Date("2007-11-07"))

detentions_facilities <-
  bind_rows(
    # "-1" = facilities_51185,
    "2010-01-01" = detentions_hrw_df |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date = book_in_date,
        detention_book_out_date = book_out_date,
        gender
      ),
    "2024-01-01" = detentions_41855 |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date = book_in_date_and_time,
        detention_book_out_date = book_out_date_time,
        anonymized_identifier = alien_number_unique_identifier,
        gender,
        birth_year
      ),
    "2024-02-01" = detentions_05655 |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date = initial_book_in_date_time,
        detention_book_out_date = detention_book_out_date_time,
        anonymized_identifier,
        gender,
        birth_year
      ),
    "2023-11-15" = detentions_2012_2023 |>
      select(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date,
        detention_book_out_date,
        anonymized_identifier,
        gender,
        birth_year
      ),
    "2025-10-15" = detentions_current |>
      select(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date = book_in_date_time,
        detention_book_out_date = book_out_date_time,
        anonymized_identifier = unique_identifier,
        gender,
        birth_year
      ),
    "2014-10-15" = detentions_hrw_additional_1 |>
      transmute(
        detention_facility_code = history_detention_facility_code,
        name = history_detention_facility,
        detention_book_in_date = history_intake_date,
        detention_book_out_date = history_book_out_date,
        anonymized_identifier = as.character(unique_person_id)
      ),
    "2014-10-15" = detentions_hrw_additional_2 |>
      transmute(
        detention_facility_code = history_detention_facility_code,
        name = history_detention_facility,
        detention_book_in_date = history_intake_date,
        detention_book_out_date = history_book_out_date,
        anonymized_identifier = as.character(unique_person_id)
      ),
    "2014-10-15" = detentions_hrw_additional_3 |>
      transmute(
        detention_facility_code = history_detention_facility_code,
        name = history_detention_facility,
        detention_book_in_date = history_intake_date,
        detention_book_out_date = history_book_out_date,
        anonymized_identifier = as.character(unique_person_id)
      ),
    "2014-10-15" = detentions_hrw_additional_4 |>
      transmute(
        detention_facility_code = history_detention_facility_code,
        name = history_detention_facility,
        detention_book_in_date = history_intake_date,
        detention_book_out_date = history_book_out_date,
        anonymized_identifier = as.character(unique_person_id)
      ),
    "2010-05-25" = detentions_hrw_additional_5 |>
      transmute(
        detention_facility_code,
        name = detention_facility,
        detention_book_in_date = book_in_date,
        detention_book_out_date = book_out_date
      ),
    .id = "source"
  ) |>
  arrange(detention_facility_code, source) |>
  group_by(detention_facility_code) |>
  summarize(
    name = last(name),
    first_book_in = as.Date(min(
      pmin(detention_book_out_date, detention_book_in_date, na.rm = TRUE),
      na.rm = TRUE
    )),
    last_book_in = as.Date(max(
      pmax(detention_book_out_date, detention_book_in_date, na.rm = TRUE),
      na.rm = TRUE
    )),
    n_stints = n(),
    n_individuals = n_distinct(anonymized_identifier),
    # proportion_male = mean(
    #   gender == "Male" & gender != "Unknown",
    #   na.rm = TRUE
    # ),
    .groups = "drop"
  )

arrow::write_feather(
  detentions_facilities,
  "data/facilities-from-detentions.feather"
)
