#!/usr/bin/env Rscript
#
# Harvard Dataverse only allows files less than 2.5 GB in size; this script
# splits compressed CSV files to meet this limit, preserving their structure and
# compression in the process.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

library(data.table)
library(glue)
library(unglue)

# Maximum size of a split - added 1% buffer room to be safe
default_max_split_size <- 2.5e9 * 0.99

# Where to save splits to
output_directory <- "output/aggregated-combined-distrib/"

# Split a large CSV file into different splits. It's hard to guess the size of a
# compressed output from the size of the compressed input alone, so we sample
# random lines from the input, write to temporary compressed outputs, and
# predict the number of lines in each split via OLS regression. This makes the
# assumption that the data is fairly rectangular in terms of bytes per row.
split_csv <- function(path,
                      output_template, # Should be of form "xxx_{split_number}.csv.gz"
                      max_split_size = default_max_split_size,
                      sample_min_lines = 1e3,
                      sample_max_lines = 1e6,
                      sample_n = 10,
                      sample_temp_path = "split_test.csv.gz"
                      ) {
  first_file <- glue(output_template, split_number = "01")
  if (file.exists(first_file)) {
    message(sprintf("ERROR: found existing file %s", first_file))
  } else if (file.size(path) < max_split_size) {
    message("Input path is already less than max split size")
    system(sprintf("ln \"%s\" \"%s\"", path, first_file))
    message(sprintf("* Created hard link %s -> %s", first_file, path))
  } else {
    message(sprintf("Splitting: %s -> %s", path, output_template))
    message("(1/3) Reading data")
    data <- fread(path)
    
    message("(2/3) Determining split size")
    actual_sample_max_lines <- min(sample_max_lines, nrow(data))
    sample_lines <- seq(
      sample_min_lines, actual_sample_max_lines,
      by = (actual_sample_max_lines - sample_min_lines) / sample_n
    )
    
    message("* Sampling random lines: ", appendLF = FALSE)
    sample_sizes <- sapply(
      sample_lines,
      function(lines) {
        message(sprintf("%d ... ", lines), appendLF = FALSE)
        fwrite(data[sample(nrow(data), lines),], sample_temp_path)
        result <- file.size(sample_temp_path)
        unlink(sample_temp_path)
        return(result)
      }
    )
    lines_per_split <- as.integer(predict(
      lm(sample_lines ~ sample_sizes),
      newdata = data.frame(sample_sizes = max_split_size)
    ))
    message(sprintf("\n* Split size: %d lines", lines_per_split))
    
    split_points <- c(seq(1, nrow(data), lines_per_split), nrow(data))
    n_splits <- length(split_points) - 1
    message(sprintf("(3/3) Splitting into %d parts", length(split_points) - 1))
    for (i in 1:n_splits) {
      output_path <- glue(output_template, split_number = sprintf("%02d", i))
      start <- split_points[i]
      end <- split_points[i + 1] - 1
      
      if (i == n_splits) {
        end <- end + 1
      }
      
      message(sprintf(
        "* Writing part %d/%d: %s (lines %d-%d)",
        i, n_splits, output_path, start, end
      ))
      
      fwrite(data[start:end,], output_path)
    }
  }
}

paths <- Sys.glob("output/aggregated-combined/*/*.csv.gz")
parts <- as.data.table(unglue_data(
  paths,
  "output/aggregated-combined/{geography}/{aggregation}_{measure}.csv.gz"
))[, path := paths]

dir.create(output_directory, showWarnings = FALSE, recursive = TRUE)
    
for (i in 1:nrow(parts)) {
  part <- parts[i,]
  split_csv(
    part$path,
    file.path(
      output_directory,
      sprintf(
        "%s_%s_%s_part{split_number}.csv.gz",
        part$geography, part$aggregation, part$measure
      )
    )
  )
}