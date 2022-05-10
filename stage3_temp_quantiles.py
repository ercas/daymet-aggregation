#!/usr/bin/env python3
#
# Generate percentiles 0-100 of tmax and tmin files. Low-memory verison.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

import csv
import collections
import os
import gzip
import typing

import numpy
import tqdm

DEFAULT_TMAX_FILENAME = "mean_tmax.csv.gz"
DEFAULT_TMIN_FILENAME = "mean_tmin.csv.gz"
DEFAULT_TMAX_QUANTILES_FILENAME = "tmax_quantiles.csv.gz"
DEFAULT_TMIN_QUANTILES_FILENAME = "tmin_quantiles.csv.gz"

DEFAULT_PERCENTILES = range(101)

LAST_YEAR = 2020  # For updating the progress bar


def dump_binned_values(values_by_id,
                       current_year,
                       writer,
                       percentiles: typing.Iterable[int] = DEFAULT_PERCENTILES):
    for id_, values in tqdm.tqdm(
            values_by_id.items(),
            position=1,
            desc="Generating pctiles for {}".format(current_year)
    ):
        writer.writerow(
            [id_, current_year] + list(numpy.percentile(
                values, percentiles
            ))
        )


def extract_quantiles(input_path: str, output_path: str):
    with gzip.open(input_path, "rt") as input_fp:
        reader = csv.DictReader(input_fp)
        id_column = reader.fieldnames[0]

        current_year = None
        values_by_id = collections.defaultdict(list)

        temp_path = "{}.part".format(output_path)

        with gzip.open(temp_path, "wt") as output_fp:
            writer = csv.writer(output_fp)
            writer.writerow([id_column, "year"] + ["pctile{:02d}".format(i) for i in range(101)])

            iterator = tqdm.tqdm(
                reader,
                position=0,
                desc="Reading data",
                smoothing=0
            )

            for row in iterator:
                year = row["date"][:4]

                if year != current_year:
                    if current_year is not None:

                        dump_binned_values(values_by_id, current_year, writer)

                        # If not initialized: update the iterator so we can have ETA
                        if iterator.total is None:
                            total_rows = (
                                    len(values_by_id)
                                    * (LAST_YEAR - 1980 + 1)
                                    * 365
                            )
                            tqdm.tqdm.write("Estimated {} total rows".format(total_rows))
                            iterator.total = total_rows

                    # Reset bins
                    values_by_id = collections.defaultdict(list)
                    current_year = year

                values_by_id[row[id_column]].append(float(row["value"]))

            # Dump last year
            dump_binned_values(values_by_id, current_year, writer)

        os.rename(temp_path, output_path)

if __name__ == "__main__":
    import argparse
    import glob

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", default=None)
    parser.add_argument("-o", "--output", default=None)
    args = parser.parse_args()

    # Input given: convert the given file
    if all([args.input, args.output]):
        extract_quantiles(args.input, args.output)

    # No input given: determine what files need to be converted by looking for
    # missing files in the expected daymet-aggregation output directory
    # hierarchy
    else:
        for aggregated_directory in glob.glob("output/aggregated-combined/*"):
            extra_directory = aggregated_directory.replace("aggregated-combined", "extra")

            tmax_path = os.path.join(aggregated_directory, DEFAULT_TMAX_FILENAME)
            tmin_path = os.path.join(aggregated_directory, DEFAULT_TMIN_FILENAME)
            tmax_quantiles_path = os.path.join(extra_directory, DEFAULT_TMAX_QUANTILES_FILENAME)
            tmin_quantiles_path = os.path.join(extra_directory, DEFAULT_TMIN_QUANTILES_FILENAME)

            for path in [tmax_path, tmin_path]:
                if not os.path.isfile(path):
                    raise Exception("ERROR: {} does not exist".format(path))

            if not os.path.isdir(extra_directory):
                os.makedirs(extra_directory)

            if os.path.isfile(tmax_quantiles_path):
                print("Skipping {}".format(tmax_quantiles_path))
            else:
                print("Generating {}".format(tmax_quantiles_path))
                extract_quantiles(tmax_path, tmax_quantiles_path)

            if os.path.isfile(tmin_quantiles_path):
                print("Skipping {}".format(tmin_quantiles_path))
            else:
                print("Generating {}".format(tmin_quantiles_path))
                extract_quantiles(tmin_path, tmin_quantiles_path)
