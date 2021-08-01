#!/usr/bin/env Rscript
#
# Generate extreme temperature indicators in accordance with: Schwartz, J.
# (2005). Who is Sensitive to Extremes of Temperature?: A Case-Only Analysis.
# Epidemiology, 16(1), 67–72. https://doi.org/10.1097/01.ede.0000147114.25957.71
# Schwartz gives the following definitions for extreme temperature days:
#
# * Hot: >= 99th pctile of tmin
# * Cold: <= 1st pctile of tmax
#
# Slight modification: percentiles have been calculated on a yearly basis to
# take shifts of percentiles over time due to climate change into account -
# otherwise cold days may be overrepresented in older years and hot days
# overrepresented in more recent years
#
# This script also generates heat waves and cold waves, defined as two or more
# consecutive days with extreme heat/cold. Each day in the hot/cold wave will be
# marked as a hot/cold wave day.
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
  
  output_tmax_quantiles <- sprintf("%s/tmax_quantiles.csv.gz", output_directory)
  output_tmin_quantiles <- sprintf("%s/tmin_quantiles.csv.gz", output_directory)
  output_extreme_temps <- sprintf("%s/extreme_temp_days.csv.gz", output_directory)
  output_extreme_temp_waves <- sprintf("%s/extreme_temp_wave_days.csv.gz", output_directory)
  
  if (all(sapply(
    list(
      output_tmax_quantiles, output_tmin_quantiles,
      output_extreme_temps, output_extreme_temp_waves
    ),
    file.exists
  ))) {
    message(sprintf("Skipping: %s", current_geography))
  } else {
    message(sprintf("Processing: %s", current_geography))
    
    # Read data ----
  
    message("( 1/10) Reading and transforming tmax data")
    tmax <- fread(parts[geography == current_geography & measure == "tmax"]$path)
    
    # Hacky: do some math to extract the year, month, and day from ISO8601 dates
    # with no delimiter
    tmax[, year := floor(date / 1e4)]
    tmax[, month := floor((date / 1e4 - year) * 1e2)]
    tmax[, day := (date / 100 - floor(date / 100)) * 100]
    
    # Guess the ID column (should be the first column) and rename it (syntactically
    # inconvenient to use variables in data.table `by` argument)
    id_column <- names(tmax)[1]
    setnames(tmax, id_column, "id")
    
    # Same as above - refer to above for comments
    message("( 2/10) Reading and transforming tmin data")
    tmin <- fread(parts[geography == current_geography & measure == "tmax"]$path)
    tmin[, year := floor(date / 1e4)]
    tmin[, month := floor((date / 1e4 - year) * 1e2)]
    tmin[, day := (date / 1e2 - floor(date / 1e2)) * 1e2]
    setnames(tmin, id_column, "id") # Should be same ID column
    
    # Generate quantiles ----
    
    if (file.exists(output_tmax_quantiles)) {
      message("( 3/10) Skipped")
      tmax_agg <- fread(output_tmax_quantiles)
    } else {
      message("( 3/10) Calculating tmax 1st and 99th percentiles")
      tmax_agg <- tmax[, .(tmax_pctile01 = quantile(value, 0.01, na.rm = TRUE),
                           tmax_pctile99 = quantile(value, 0.99, na.rm = TRUE)),
                       by = list(id, year)]
      setnames(tmax_agg, "id", id_column)
      fwrite(tmax_agg, output_tmax_quantiles)
    }
    
    if (file.exists(output_tmin_quantiles)) {
      message("( 4/10 Skipped")
      tmin_agg <- fread(output_tmin_quantiles)
    } else {
      message("( 4/10) Calculating tmin 1st and 99th percentiles")
      tmin_agg <- tmin[, .(tmin_pctile01 = quantile(value, 0.01, na.rm = TRUE),
                           tmin_pctile99 = quantile(value, 0.99, na.rm = TRUE)),
                       by = list(id, year)]
      setnames(tmin_agg, "id", id_column)
      fwrite(tmax_agg, output_tmin_quantiles)
    }
    
    # Back to "id" for joining later
    setnames(tmax_agg, id_column, "id")
    setnames(tmin_agg, id_column, "id")
    
    # Hot and cold weather indicators ----
    
    if (file.exists(output_extreme_temps)) {
      message("( 5/10 Skipped")
      message("( 6/10 Skipped")
      message("( 7/10 Skipped")
    } else {
      message("( 5/10) Generating extreme cold indicators")
      cold <- tmax[tmax_agg, on = list(id, year)
                   ][value <= tmax_pctile01
                    ][, list(id, year, month, day)
                     ][, extreme := "cold"]
      setnames(cold, "id", id_column)
      
      message("( 6/10) Generating extreme heat indicators")
      hot <- tmin[tmin_agg, on = list(id, year)
                  ][value >= tmin_pctile99
                   ][, list(id, year, month, day)
                    ][, extreme := "hot"]
      setnames(hot, "id", id_column)
      
      message("( 7/10) Writing extreme weather indicators")
      fwrite(rbindlist(list(cold, hot)), output_extreme_temps)
    }
    
    # Hot and cold wave indicators ----
    
    if (file.exists(output_extreme_temp_waves)) {
      message("( 8/10 Skipped")
      message("( 9/10 Skipped")
      message("(10/10 Skipped")
    } else {
      message("( 8/10) Generating cold wave indicators")
      cold_waves <- tmax[tmax_agg, on = list(id, year)
                         ][, `:=` (this_day_cold = value <= tmax_pctile01,
                                   next_day_cold = shift(value, 1) <= tmax_pctile01,
                                   prev_day_cold = shift(value, -1) <= tmax_pctile01)
                          ][(this_day_cold == TRUE & next_day_cold == TRUE) | (this_day_cold == TRUE & prev_day_cold == TRUE)
                           ][, list(id, year, month, day)
                            ][, extreme_wave := "cold"]
      setnames(cold_waves, "id", id_column)
      
      message("( 9/10) Generating heat wave indicators")
      heat_waves <- tmin[tmin_agg, on = list(id, year)
                         ][, `:=` (this_day_hot = value >= tmin_pctile99,
                                   next_day_hot = shift(value, 1) >= tmin_pctile99,
                                   prev_day_hot = shift(value, -1) >= tmin_pctile99)
                          ][(this_day_hot == TRUE & next_day_hot == TRUE) | (this_day_hot == TRUE & prev_day_hot == TRUE)
                           ][, list(id, year, month, day)
                            ][, extreme_wave := "hot"]
      setnames(heat_waves, "id", id_column)
                
      message("(10/10) Writing heat wave indicators")
      fwrite(rbindlist(list(cold_waves, heat_waves)), output_extreme_temp_waves)
    }
  }
}