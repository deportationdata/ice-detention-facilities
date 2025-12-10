library(tidyverse)
library(readxl)
library(tidylog)

detentions_current <- arrow::read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"
)

detentions_2012_2023 <- arrow::read_feather(
  "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-detentions-2012-2023.feather"
)

# bind detentions to obtain all facilities used since 2012
detentions_facilities <-
  bind_rows(
    detentions_2012_2023 |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date,
        detention_book_out_date,
        anonymized_identifier,
        gender,
        birth_year
      ) |>
      mutate(source = "2012-2023"),
    detentions_current |>
      select(
        detention_facility_code,
        detention_facility,
        detention_book_in_date = book_in_date_time,
        detention_book_out_date = book_out_date_time,
        anonymized_identifier = unique_identifier,
        gender,
        birth_year
      ) |>
      mutate(source = "current")
  ) |>
  group_by(detention_facility_code, detention_facility) |>
  summarize(
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
