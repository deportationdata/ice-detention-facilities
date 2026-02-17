library(tidyverse)
library(tidylog)

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
  distinct(detention_facility_code, name = detention_facility, city, state) |>
  mutate(date = as.Date("2010-01-01"))

arrow::write_feather(
  detentions_hrw_df,
  "data/facilities-foia-hrw.feather"
)
