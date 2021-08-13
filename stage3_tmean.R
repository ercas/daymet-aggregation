#!/usr/bin/env Rscript
#
# Generate mean temperature, taken as the average of tmax and tmin.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(unglue)

paths <- Sys.glob("output/aggregated-combined/*/*.csv.gz")
parts <- as.data.table(unglue_data(
  paths,
  "output/aggregated-combined/{geography}/{aggregation}_{measure}.csv.gz"
))[, path := paths][aggregation == "mean"]

for (current_geography in unique(parts$geography)) {
  output_directory <- sprintf("output/extra/%s", current_geography)
  dir.create(output_directory, showWarnings = FALSE, recursive = TRUE)
  
  output_path <- sprintf("%s/mean_tmean.csv.gz", output_directory)
  
  if (file.exists(output_path)) {
    message(sprintf("Skipping: %s", current_geography))
  } else if (
    nrow(parts[geography == current_geography & measure == "tmax"]) > 0 &
    nrow(parts[geography == current_geography & measure == "tmin"]) > 0
  ) {
    message(sprintf("Processing: %s", current_geography))
    
    message("(1/5) Reading tmax data")
    tmax <- fread(parts[geography == current_geography & measure == "tmax"]$path)
    setnames(tmax, "value", "tmax")
    
    message("(2/5) Reading tmin data")
    tmin <- fread(parts[geography == current_geography & measure == "tmin"]$path)
    setnames(tmin, "value", "tmin")
    
    message("(3/5) Joining tmax and tmin")
    join_columns <- intersect(names(tmax), names(tmin))
    result <- tmax[tmin, on = join_columns]
    
    rm(tmax, tmin) # Memory management
    
    message("(4/5) Calculating tmean")
    result[, tmean := (tmax + tmin) / 2]
    
    message("(5/5) Writing tmean")
    output_columns <- c(join_columns, "tmean")
    fwrite(result[, ..output_columns], output_path)
    
  }
}
