#!/usr/bin/env Rscript
#
# Generate percentiles 0-100 of tmax and tmin files.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(pbapply)
library(unglue)

pboptions(type = "timer")

paths <- Sys.glob("output/aggregated-combined/*/*.csv.gz")
parts <- as.data.table(unglue_data(
  paths,
  "output/aggregated-combined/{geography}/{aggregation}_{measure}.csv.gz"
))[, path := paths][aggregation == "mean"]

parts <- parts[!grepl("block_group", path)]

PERCENTILES <- 0:100 / 100

for (current_geography in unique(parts$geography)) {
  output_directory <- sprintf("output/extra/%s", current_geography)
  dir.create(output_directory, showWarnings = FALSE, recursive = TRUE)
  
  output_tmax_quantiles <- sprintf("%s/tmax_quantiles.csv.gz", output_directory)
  output_tmin_quantiles <- sprintf("%s/tmin_quantiles.csv.gz", output_directory)
  
  message(sprintf("Processing: %s", current_geography))
  
  if (file.exists(output_tmax_quantiles)) {
    message("(1/8) Already done")
    message("(2/8) Already done")
    message("(3/8) Already done")
    message("(4/8) Already done")
  } else {
    message("(1/8) Reading and transforming tmax data")
    tmax <- fread(parts[geography == current_geography & measure == "tmax"]$path)
    
    # Hacky: do some math to extract the year, month, and day from ISO8601 dates
    # with no delimiter
    tmax[, year := floor(date / 1e4)]
    tmax[, month := floor((date / 1e4 - year) * 1e2)]
    tmax[, day := date - (floor(date / 1e2) * 1e2)]
    
    # Guess the ID column (should be the first column) and rename it (syntactically
    # inconvenient to use variables in data.table `by` argument)
    id_column <- names(tmax)[1]
    setnames(tmax, id_column, "id")
    
    # Generate quantiles ----
    message("(2/8) Calculating tmax percentiles")
    tmax_agg <- pblapply(
      PERCENTILES,
      function(percentile) {
        result <- tmax[, list(temp = quantile(value, percentile, na.rm = TRUE)),
                       by = list(id, year)]
        setnames(result, "temp", sprintf("pctile%02.f", percentile * 100))
      }
    )
    
    # Merge and write out ----
    message("(3/8) Merging tmax percentiles")
    tmax_agg <- Reduce(function(left, right) cbind(left, right[, 3]), tmax_agg)
    setnames(tmax_agg, "id", id_column)
    
    message("(4/8) Writing tmax percentiles")
    fwrite(tmax_agg, output_tmax_quantiles)
    
    rm(tmax)
    rm(tmax_agg)
    gc()
  }
  
  # Repeat for tmin
  if (file.exists(output_tmin_quantiles)) {
    message("(5/8) Already done")
    message("(6/8) Already done")
    message("(7/8) Already done")
    message("(8/8) Already done")
  } else {
    message("(5/8) Reading and transforming tmin data")
    tmin <- fread(parts[geography == current_geography & measure == "tmin"]$path)
    
    # Hacky: do some math to extract the year, month, and day from ISO8601 dates
    # with no delimiter
    tmin[, year := floor(date / 1e4)]
    tmin[, month := floor((date / 1e4 - year) * 1e2)]
    tmin[, day := date - (floor(date / 1e2) * 1e2)]
    
    # Guess the ID column (should be the first column) and rename it (syntactically
    # inconvenient to use variables in data.table `by` argument)
    id_column <- names(tmin)[1]
    setnames(tmin, id_column, "id")
    
    # Generate quantiles ----
    message("(6/8) Calculating tmin percentiles")
    tmin_agg <- pblapply(
      PERCENTILES,
      function(percentile) {
        result <- tmin[, list(temp = quantile(value, percentile, na.rm = TRUE)),
                       by = list(id, year)]
        setnames(result, "temp", sprintf("pctile%02.f", percentile * 100))
      }
    )
    
    # Merge and write out ----
    message("(7/8) Merging tmin percentiles")
    tmin_agg <- Reduce(function(left, right) cbind(left, right[, 3]), tmin_agg)
    setnames(tmin_agg, "id", id_column)
    
    message("(8/8) Writing tmin percentiles")
    fwrite(tmin_agg, output_tmin_quantiles)
    
    rm(tmin)
    rm(tmin_agg)
    gc()
  }
}
