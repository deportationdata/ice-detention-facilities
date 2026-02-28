library(tidyverse)
library(tidylog)

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
  janitor::clean_names()

detentions_41855_df <-
  detentions_41855 |>
  janitor::clean_names() |>
  rename(
    detention_facility_operator = detention_facility_type_5,
    detention_facility_type = detention_facility_type_14
  ) |>
  mutate(
    detention_facility_operator = case_when(
      detention_facility_operator == "INS Facility" ~ "ICE Facility",
      TRUE ~ detention_facility_operator
    )
  )

detentions_41855_df_summary <-
  detentions_41855_df |>
  arrange(detention_facility_code, book_out_date_time) |>
  group_by(detention_facility_code) |>
  summarize(
    name = list(unique(detention_facility)),
    address = last(detention_facility_address),
    city = last(detention_facility_city),
    state = last(detention_facility_state_code),
    zip = last(detention_facility_zip_code),
    type = last(detention_facility_type),
    type_detailed = last(detention_facility_type_detailed),
    ice_funded = last(detention_facility_ice_funded),
    male_female = last(detention_facility_male_female),
    over_under_72 = last(detention_facility_over_under_72),
    operator = last(detention_facility_operator),
    first_book_in = as.Date(min(
      pmin(
        book_out_date_time,
        book_in_date_and_time,
        na.rm = TRUE
      ),
      na.rm = TRUE
    )),
    last_book_in = as.Date(max(
      pmax(
        book_out_date_time,
        book_in_date_and_time,
        na.rm = TRUE
      ),
      na.rm = TRUE
    )),
    n_stints = n(),
    n_individuals = n_distinct(alien_number_unique_identifier),
    proportion_male = mean(
      gender == "Male" & gender != "Unknown",
      na.rm = TRUE
    )
  ) |>
  ungroup() |>
  mutate(date = as.Date("2024-01-01")) |>
  unnest(name) |>
  mutate(name_n = row_number(), .by = detention_facility_code) |>
  pivot_wider(
    names_from = name_n,
    values_from = name,
    names_prefix = "name_"
  ) |>
  unnest(name) |>
  mutate(name_n = row_number(), .by = detention_facility_code) |>
  pivot_wider(
    names_from = name_n,
    values_from = name,
    names_prefix = "name_"
  ) |>
  rename(name = name_1, name_alt = name_2) |>
  relocate(name, name_alt, .after = detention_facility_code)

arrow::write_feather(
  detentions_41855_df_summary,
  "data/facilities-foia-41855.feather"
)
