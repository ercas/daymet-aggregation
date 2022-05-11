#!/usr/bin/env python3
#
# Generate mean temperature, taken as the average of tmax and tmin. Low-memory
# version.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

import csv
import gzip
import os

import tqdm

DEFAULT_TMAX_FILENAME = "mean_tmax.csv.gz"
DEFAULT_TMIN_FILENAME = "mean_tmin.csv.gz"
DEFAULT_TMEAN_FILENAME = "mean_tmean.csv.gz"


def generate_tmean(tmin_path: str, tmax_path: str, output_path: str):
    temp_path = "{}.part".format(output_path)

    with gzip.open(tmin_path, "rt") as tmin_fp, \
            gzip.open(tmax_path, "rt") as tmax_fp, \
            gzip.open(temp_path, "wt") as output_fp:
        tmax_reader = csv.DictReader(tmax_fp)
        tmin_reader = csv.DictReader(tmin_fp)

        id_column = tmax_reader.fieldnames[0]

        tmean_writer = csv.DictWriter(output_fp, fieldnames=[id_column, "date", "tmean"])
        tmean_writer.writeheader()

        for tmax_row in tqdm.tqdm(tmax_reader):
            tmin_row = next(tmin_reader)

            if not tmax_row:
                break

            # assert tmax_row[id_column] == tmin_row[id_column]
            # assert tmax_row["date"] == tmin_row["date"]

            tmean_writer.writerow({
                id_column: tmax_row[id_column],
                "date": tmax_row["date"],
                "tmean": (float(tmax_row["value"]) + float(tmin_row["value"])) / 2
            })

    os.rename(temp_path, output_path)


if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--tmin-file", default=None)
    parser.add_argument("-T", "--tmax-file", default=None)
    parser.add_argument("-o", "--output", default=None)
    args = parser.parse_args()

    # Input given: convert the given file
    if all([args.tmin_file, args.tmax_file, args.output]):
        generate_tmean(args.tmin_file, args.tmax_file, args.output)

    # No input given: determine what files need to be converted by looking for
    # missing files in the expected daymet-aggregation output directory
    # hierarchy
    else:
        for aggregated_directory in glob.glob("output/aggregated-combined/*"):
            extra_directory = aggregated_directory.replace("aggregated-combined", "extra")

            tmax_path = os.path.join(aggregated_directory, DEFAULT_TMAX_FILENAME)
            tmin_path = os.path.join(aggregated_directory, DEFAULT_TMIN_FILENAME)
            tmean_path = os.path.join(extra_directory, DEFAULT_TMEAN_FILENAME)

            for path in [tmax_path, tmin_path]:
                if not os.path.isfile(path):
                    raise Exception("ERROR: {} does not exist".format(path))

            if not os.path.isdir(extra_directory):
                os.makedirs(extra_directory)

            if os.path.isfile(tmean_path):
                print("Skipping {}".format(tmean_path))
            else:
                print("Generating {}".format(tmean_path))
                generate_tmean(tmin_path, tmax_path, tmean_path)
