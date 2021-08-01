#!/usr/bin/env bash
#
# Bulk download DAYMET data from THREDDS, resuming incomplete downloads and
# ignoring existing files.
#
# It is preferred to run this script in `tmux` (or `nohup`, or the RStudio
# terminal) so that the download can persist after logout. For faster speeds,
# run in an SSH session directly on the storage server to avoid double traversal
# of the data over the network + I/O bottlenecks with network mounts.
#
# Usage: bash download.sh [ORNL DAAC THREDDS server root] [output directory name, e.g. "annual"]
#
# Exmaple, to download all the annual data:
#
#    bash download.sh https://thredds.daac.ornl.gov/thredds/catalog/ornldaac/1852/catalog.html annual
#
# Monthly:
#
#    bash download.sh https://thredds.daac.ornl.gov/thredds/catalog/ornldaac/1855/catalog.html monthly
#
# Daily:
#
#    bash download.sh https://thredds.daac.ornl.gov/thredds/catalog/ornldaac/1840/catalog.html daily
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

url="$1"
output_directory="rawdata/$2"

html="$(curl "$1")"

mkdir -p "$output_directory"

# Example line: <a href='catalog.html?dataset=1855/daymet_v4_prcp_monttl_hi_1981.nc'><tt>daymet_v4_prcp_monttl_hi_1981.nc</tt></a></td>
# Need to extract the `href`
grep -Eo "catalog.html[^']*.nc" <<< "$html" | while read url
do
	# Example $url: catalog.html?dataset=1855/daymet_v4_vp_monavg_pr_2016.nc
	# We need to parse out `daymet_v4_vp_monavg_pr_2016.nc` and `1855` to build a
	# request to THREDDS
	filename=$(grep -Eo "[^/]+$" <<< "$url")
	dataset=$(grep -Eo "dataset=[0-9]+" <<< "$url" | cut -d = -f 2)
	output="$output_directory/$filename"
	temp="${output}.part"
	if ! [ -f "$output" ]
	then
		wget -O "$temp" "https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/${dataset}/${filename}"
		mv -v "$temp" "$output"
	else
		echo "skipping already-downloaded file $output"
	fi
done
