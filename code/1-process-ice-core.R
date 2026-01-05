library(tidyverse)
library(readxl)
library(tidylog)

detentions_current <- arrow::read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
)

detentions_2012_2023 <- arrow::read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-detentions-2012-2023.feather"
)

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
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release/2024-ICFO-41855",
    full.names = TRUE
  ) |>
  set_names() |>
  map(
    ~ readxl::read_excel(.x, col_types = col_defs_41855, skip = 4),
    .id = "file"
  ) |>
  bind_rows(.id = "file_original") |>
  janitor::clean_names()

detentions_05655 <-
  list.files(
    "~/Library/CloudStorage/Box-Box/deportationdata/data/ICE/September 2025 Release/2024-ICFO-05655",
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
  janitor::clean_names()

detentions_facilities <-
  bind_rows(
    "1" = detentions_41855 |>
      transmute(
        detention_facility_code,
        detention_facility,
        detention_book_in_date = book_in_date_and_time,
        detention_book_out_date = book_out_date_time,
        anonymized_identifier = alien_number_unique_identifier,
        gender,
        birth_year
      ),
    "2" = detentions_05655 |>
      transmute(
        detention_facility_code,
        detention_facility,
        detention_book_in_date = initial_book_in_date_time,
        detention_book_out_date = detention_book_out_date_time,
        anonymized_identifier,
        gender,
        birth_year
      ),
    "3" = detentions_2012_2023 |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date,
        detention_book_out_date,
        anonymized_identifier,
        gender,
        birth_year
      ),
    "4" = detentions_current |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date = book_in_date_time,
        detention_book_out_date = book_out_date_time,
        anonymized_identifier = unique_identifier,
        gender,
        birth_year
      ),
    .id = "source"
  ) |>
  arrange(detention_facility_code, source) |>
  group_by(detention_facility_code) |>
  summarize(
    detention_facility = last(detention_facility),
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
    # n_stints_2025 = sum(detention_book_in_date >= "2025-01-01", na.rm = TRUE),
    # n_individuals_2025 = sum(
    #   detention_book_in_date >= "2025-01-01",
    #   na.rm = TRUE
    # ),
    proportion_male = mean(
      gender == "Male" & gender != "Unknown",
      na.rm = TRUE
    ),
    .groups = "drop"
  )

arrow::write_feather(
  detentions_facilities,
  "data/facilities-from-detentions.feather"
)
