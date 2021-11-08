#!/usr/bin/env Rscript
#
# Combine daily DAYMET data aggregated into yearly files into a single,
# long-format file.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(unglue)

# Finished aggregations
paths <- Sys.glob("output/aggregated/*/*.csv.gz")
parts <- as.data.table(unglue_data(
  paths,
  "output/aggregated/{geography}/{measure}_{year}.csv.gz"
))[, path := paths][order(geography, measure, year)]

# Unfinished aggregations will have uncompressed CSV files in them - we need to
# remove these directories from `parts` so as to not generate incomplete
# combined data
# unfinished_paths <- Sys.glob("output/aggregated/*/*.csv")
# unfinished <- as.data.table(unglue_data(
#   unfinished_paths,
#   "output/aggregated/{geography}/{measure}_{year}.csv"
# ))[, list(geography, measure)][, unfinished := TRUE]
# 
# parts <- unfinished[parts, on = list(geography, measure), allow.cartesian = TRUE][is.na(unfinished)]
parts <- parts[geography != "block_groups_2010"]
parts <- parts[geography != "block_groups_2000"]

# Read and transform output from aggregate.sh
read_part <- function(path, subset_to_aggregation = NA, verbose = TRUE) {
  # Read data
  if (verbose) message(sprintf("Reading: %s", path))
  data <- fread(path)
  
  # Guess the ID column - should be the only column not starting with a digit
  id_column <- Filter(
    function(column) return(! substr(column, 1, 1) %in% c(1:9)),
    names(data)
  )
  output_columns <- c(id_column, "aggregation", "date", "value")
  
  # Subset to one aggregation if specified
  if (!is.na(subset_to_aggregation)) {
    # Columns that end with _{subset_to_aggregation}
    all_columns <- names(data)
    subset_columns <- c(
      id_column,
      all_columns[grep(sprintf("_%s$", subset_to_aggregation), all_columns)]
    )
    
    if (verbose) message(sprintf("> Subsetting to aggregation: %s", subset_to_aggregation))
    data <- data[, ..subset_columns]
    
    # `aggregation` column is now extraneous
    output_columns <- c(id_column, "date", "value")
  }
  
  # Transform wide to long
  if (verbose) message("> Pivoting data from wide -> long")
  data <- melt(data, id_column)
  
  # Split variable=2000101_min" -> "2001", "min"
  if (verbose) message("> Splitting metadata")
  data <- data[, c("date", "aggregation") := tstrsplit(variable, "_", fixed = TRUE)]
  
  # Column cleanup
  if (verbose) message("> Reordering columns")
  data <- data[, ..output_columns]
  
  # Remove NAs (areas with no Daymet data)
  if (verbose) message("> Removing NAs")
  data <- data[!is.na(value)]
  
  return(data)
}

for (current_geography in unique(parts$geography)) {
  for (current_measure in unique(parts$measure)) {
    for (current_aggregation in c("min", "max", "mean")) {
      output_directory <- sprintf(
        "output/aggregated-combined/%s", current_geography
      )
      dir.create(output_directory, showWarnings = FALSE, recursive = TRUE)
      output_file <- sprintf(
        "%s/%s_%s.csv.gz",
        output_directory, current_aggregation, current_measure
      )
      if (!file.exists(output_file)) {
        message(sprintf(
          "Combining: %s %s %s",
          current_geography, current_aggregation, current_measure
        ))
        data <- lapply(
          parts[
            geography == current_geography & measure == current_measure
          ]$path,
          function(path) {
            read_part(path, current_aggregation)
          }
        )
        
        message("Merging parts")
        data <- rbindlist(data)
        
        message(sprintf("Writing to %s", output_file))
        if (nrow(data) > 1) {
          fwrite(data, output_file)
        }
      }
    }
  }
}
