library(tidyverse)
library(readxl)

f <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1836538055645"
dl <- tempfile(fileext = ".xlsx")
download.file(f, destfile = dl, mode = "wb")

# Download to temp file and read in 2017 data
facilities_2017 <-
  readxl::read_excel(
    dl,
    skip = 8,
    sheet = 2
  ) |>
  janitor::clean_names() |>
  mutate(row = row_number(), .before = 0) |>
  filter(detloc != "Redacted") |>
  readr::type_convert()
# many warnings for redacted rows

facilities_2017 <-
  facilities_2017 |>
  rename(detention_facility_code = detloc) |>
  mutate(zip = as.character(zip)) |>
  mutate(date = as.Date("2017-11-06")) # date in file

arrow::write_feather(
  facilities_2017,
  "data/facilities-2017.feather"
)
