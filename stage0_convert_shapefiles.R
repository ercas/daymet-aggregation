#!/usr/bin/env Rscript
#
# Validate TIGER/Line shapefiles and reproject into the Daymet CRS, for use by
# exactextract in aggregate.sh. To convert other shapefiles, simply add to the
# `conversions` list, following the existing pattern.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(raster)
library(sf)

# Pull the Daymet CRS from the first daily file
daymet_crs <- crs(brick(Sys.glob("rawdata/daily/*.nc")[1]))

# A list of conversions to process - `input_file` can be either a single path or
# a glob pattern (see the ZCTA single-path `input_file`s vs tract globs)
conversions <- list(
  # ZCTAs
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2000/zcta5/tl_2010_us_zcta500.shp",
    output_file = "shapefiles/zcta5_2000.shp",
    id_column = "ZCTA5CE00"
  ),
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2010/zcta5/tl_2010_us_zcta510.shp",
    output_file = "shapefiles/zcta5_2010.shp",
    id_column = "GEOID10"
  ),
  
  # Counties
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2000/counties/*.shp",
    output_file = "shapefiles/counties_2000.shp",
    id_column = "CNTYIDFP00"
  ),
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2010/counties/*.shp",
    output_file = "shapefiles/counties_2010.shp",
    id_column = "GEOID10"
  ),
  
  # Tracts
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2000/tracts/*.shp",
    output_file = "shapefiles/tracts_2000.shp",
    id_column = "CTIDFP00"
  ),
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2010/tracts/*.shp",
    output_file = "shapefiles/tracts_2010.shp",
    id_column = "GEOID10"
  ),
  
  # Block groups
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2000/block_groups/*.shp",
    output_file = "shapefiles/block_groups_2000.shp",
    id_column = "BKGPIDFP00"
  ),
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2010/block_groups/*.shp",
    output_file = "shapefiles/block_groups_2010.shp",
    id_column = "GEOID10"
  ),
  
  # Blocks
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2000/blocks/*.shp",
    output_file = "shapefiles/blocks_2000.gpkg",
    id_column = "BLKIDFP00"
  ),
  c(
    input_file = "/media/qnap3/ShapeFiles/Polygons/TIGER2010/blocks/*.shp",
    output_file = "shapefiles/blocks_2010.gpkg",
    id_column = "GEOID10"
  )
)

for (conversion in conversions) {
  input_file <- conversion["input_file"]
  output_file <- conversion["output_file"]
  id_column <- conversion["id_column"]
  if (file.exists(output_file)) {
    message(sprintf("Skipping: %s -> %s", input_file, output_file))
  } else {
    message(sprintf("Generating: %s", output_file))
    
    if (grepl("\\*", input_file)) {
      parts <- Sys.glob(input_file)
      message(sprintf(
        "(1/4) Reading %d parts using matching %s",
        length(parts), input_file
      ))
      result <- do.call(rbind, lapply(parts, st_read, quiet = TRUE))[id_column]
    } else {
      message(sprintf("(1/4) Reading %s", input_file))
      result <- st_read(input_file, quiet = TRUE)[id_column]
    }
    
    message("(2/4) Validating geometries")
    result <- st_make_valid(result)
    
    message("(3/4) Transforming to Daymet CRS")
    result <- st_transform(result, daymet_crs)
    
    message(sprintf("(4/4) Writing to %s", output_file))
    st_write(result, output_file)
  }
}
