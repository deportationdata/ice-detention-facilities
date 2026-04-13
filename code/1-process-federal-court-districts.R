library(tidyverse)
library(sf)
library(tigris)
library(tidylog)
library(geoarrow)

# must cite this per license
# http://doi.org/10.3886/E240727V2

# run this to correct corrupt geometry
# ogr2ogr -makevalid -nlt MULTIPOLYGON US_District_Court_Jurisdictions-corrected.shp US_District_Court_Jurisdictions.shp

federal_court_districts <- st_read(
  "inputs/us-district-court-jurisdictions-shapefile/US_District_Court_Jurisdictions-corrected.shp"
) |>
  st_transform(crs = 4326)

federal_court_districts |>
  tibble::as_tibble() |>
  arrow::write_parquet("data/federal-court-districts.parquet")

# run this to correct corrupt geometry
# ogr2ogr -makevalid -nlt MULTIPOLYGON US_CourtOfAppealsCircuits-corrected.shp US_CourtOfAppealsCircuits.shp

federal_court_circuits <- st_read(
  "inputs/us-courts-of-appeals-circuits-shapefile/US_CourtOfAppealsCircuits-corrected.shp"
) |>
  st_transform(crs = 4326)

federal_court_circuits |>
  tibble::as_tibble() |>
  arrow::write_parquet("data/federal-court-circuits.parquet")
