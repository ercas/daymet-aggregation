#!/usr/bin/env bash
#
# This script mimics the behaviour of `deprecated/aggregate.R` but uses a C++
# program that interfaces directly with the `exactextract` library rather than
# through a wrapper like `extractractr` in R. As a result, this program runs a
# lot faster than `aggregate.R`. Thanks Alex for discovering this tool!
#
# Note that to reconstruct all major TIGER/Line geographies, we only need ZCTA,
# block, and block group. ZCTAs are separate; counties and tracts can be
# aggregated up from blocks and block groups using areal weighting of GEOIDs
# (see https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html).
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

# Shapefile containing geographies to aggregate Daymet data to. Does not have to
# strictly be an ESRI ShapeFile; can also be GeoPackage etc. - anything
# supported by `exactextract` (i.e. anything supported by OGR)
shapefile=shapefiles/block_groups_2010.shp

# Layer of the shapefile to read feature information from - not explicitly
# needed by exactextract but is required for the progress bar. Can find using
# `ogrinfo /path/to/data.shp` without any other arguments. Leave blank to guess
# from the shapefile path name (recommended, as most shapefiles behave this way)
shapefile_layer=

# Field to label output with
shapefile_feature_id=GEOID10

# Subdirectory to save data to
output_dir=output/aggregated/block_groups_2010

########## Setup

cd "$(dirname "$0")"
mkdir -p "$output_dir"

# Daymet reference date: 1950-01-01@T00:00:00 - Daymet time units are in days
# since this date
reference_time=$(date --date 1950-01-01 +%s)

# Guess shapefile_layer name = shapefile basename without `.shp` if blank
if [ -z "$shapefile_layer" ]
then
	shapefile_layer=$(basename "$shapefile" .shp)
fi

# Determine number of features in the provided shapefile
n_features=$(
	ogrinfo $shapefile $shapefile_layer |
		grep -m 1 "Feature Count" |
		grep -oE "[0-9]+"
)

# Check if Daymet is still using the same reference date
check_date() {
	gdalinfo "$1" 2> /dev/null |
		grep -q "time#units=days since 1950-01-01 00:00:00" && true ||
		echo "Error: time units are different" && false
		
}

process_daymet_nc() {
	daymet_file="$1"
	echo -e "\nAggregating: $daymet_file"

	year=$(grep -oE "[0-9][0-9][0-9][0-9]" <<< "$daymet_file")
	daymet_variable="$(basename "$daymet_file" | cut -d _ -f 5)"
	
	# Temporary path to stream data to (uncompressed)
	temp_file="${output_dir}/${daymet_variable}_${year}.csv"
	
	# Final path (compressed)
	output_file="${temp_file}.gz"

	if [ -f "$output_file" ]
	then
		echo "* ERROR: Found existing output file $output_file"
		return
	fi

	echo "* Verifying reference datetime == $reference_time"
	if ! gdalinfo "$daymet_file" 2> /dev/null |
		grep -q "time#units=days since 1950-01-01 00:00:00"
	then
		echo "Error: time units are different"
		return
	fi

	echo "* Verifying number of bands == 365"
	n_bands=$(gdalinfo "$daymet_file" | grep Band | wc -l)
	if ! [ "$n_bands" -eq 365 ]
	then
		echo "Error: incorrent number of bands (${n_bands} != 365)"
		return
	fi

	echo "* Calculating band timestamps"
	timestamps=$(
		gdalinfo "$daymet_file" |
			grep NETCDF_DIM_time_VALUES |
			grep -oE "[0-9\.]+" |
			while read days_since_reference
			do
				echo "$days_since_reference * 86400 + $reference_time" |
					bc
			done
	)

  # Generate Bash code for `exactextract`
  #
	# Example code that needs to be generated:
	#
	#     exactextract \
	#     	-p shapefiles/zcta5_2000.shp -f ZCTA5CE00 \
	#     	-r var1:rawdata/daily/daymet_v4_daily_na_tmin_2000.nc[1] \
	#       -s "20000101=mean(var1)" \
	#     	-r var2:rawdata/daily/daymet_v4_daily_na_tmin_2000.nc[2] \
	#       -s "20000102=mean(var2)" \
	#     	[...etc...] \
	#     	-o temp_2020-01-02.csv --progress | tqdm --total 32038 > /dev/null
	echo "* Building command"
	i=1
	extractions=$(
		echo "$timestamps" |
			while read timestamp
			do
				ext_var="var${i}" # Temporary exactextract variable
				ymd=$(date -d@${timestamp} +%Y%m%d)
				echo "-r \"${ext_var}:$daymet_file[$i]\" -s \"${ymd}_min=min(${ext_var})\" -s \"${ymd}_max=max(${ext_var})\" -s \"${ymd}_mean=mean(${ext_var})\""
				i=$[$i+1]
			done
	)

	extraction_command="exactextract -p $shapefile -f $shapefile_feature_id -o $temp_file --progress $(tr "\n" " " <<< "$extractions") | tqdm --desc \"* Extracting bands\" --total $n_features > /dev/null"

	echo "* Running extraction; writing to $temp_file"
	#echo "* Command: $extraction_command"
	echo "$extraction_command" | bash

	echo "* Compressing to $output_file (background operation)"
	gzip -9 "$temp_file" 2> /dev/null &
}

########## Run

echo "Aggregating Daymet to:  $shapefile:$shapefile_layer"
echo "Saving output to:       $output_dir"

if ! [ -z "$@" ]
then
	echo "$@" | xargs -n 1 echo
else
	find rawdata/daily -type f -name '*_na_*' | sort
fi |
		while read f
		do
			process_daymet_nc "$f"
		done
