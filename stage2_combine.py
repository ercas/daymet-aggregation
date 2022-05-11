#!/usr/bin/env python3
#
# Low-memory Python version of stage2_combine.R that loops over rows in each
# files instead of trying to load the entirety of each file at a time
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

import collections
import csv
import glob
import gzip
import os
import re
import typing

import tqdm

# Finished aggregations
paths = glob.glob("output/aggregated/*/*.csv.gz")
parts = collections.defaultdict(lambda: collections.defaultdict(list))
for path in paths:
    (geography, measure, year) = re.search(
        "([^/]+)/([a-z]+)_([0-9]+).csv.gz",
        path
    ).groups()
    parts[geography][measure].append(path)

def transform_part(input_path: str,
                   aggregation: str,
                   output_path: str,
                   verbose: bool = True):
    write_header = not os.path.isfile(output_path)
    with gzip.open(input_path, "rt") as input_fp, \
         gzip.open(output_path, "at") as output_fp:
        reader = csv.reader(input_fp)
        writer = csv.writer(output_fp)
        columns = next(reader)

        # Guess the position of the ID column - should be the only column
        # that doesn't start with a number
        id_idx, id_name = next(iter(
            (idx, column)
            for idx, column in enumerate(columns)
            if not column[0].isdigit()
        ))

        # List of tuples where the first item is the position of a column and
        # the second item is the date in YYYYMMDD format
        columns_to_extract = [
            (idx, re.search("[0-9]+", column).group(0))
            for idx, column in enumerate(columns)
            if aggregation in column
        ]

        output_columns = [id_name, "date", "value"]

        # Write header if it does not exist yet
        if write_header:
            writer.writerow(output_columns)

        for row in tqdm.tqdm(reader, position=1, desc=input_path):
            id_ = row[id_idx]
            for idx, date in columns_to_extract:
                writer.writerow([id_, date, row[idx]])

#transform_part("output/aggregated/esri19uszip5/tmin_2010.csv.gz", "min", "test.csv.gz")

for geography, measures in parts.items():
    for measure, paths in measures.items():

        output_directory = "output/aggregated-combined/{}".format(geography)
        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)

        for aggregation in ["min", "max", "mean"]:

            output_file = "{}/{}_{}.csv.gz".format(output_directory, aggregation, measure)

            if not os.path.isfile(output_file):
                print("Combining: {} {} {}".format(geography, aggregation, measure))

                temp_file = "{}.part".format(output_file)
                if os.path.isfile(temp_file):
                    os.remove(temp_file)

                for path in tqdm.tqdm(paths, position=0, desc="Reading parts"):
                    transform_part(path, aggregation, temp_file)

                os.rename(temp_file, output_file)
                print("Wrote to {}".format(output_file))
