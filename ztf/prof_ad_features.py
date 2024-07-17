# Copyright 2024 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Profile AD features for ZTF"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import time
import logging
import argparse
from codecarbon import EmissionsTracker
import pandas as pd

from fink_utils.spark.utils import concat_col
from fink_science.ad_features.processor import extract_features_ad

from ztf.log_format import apply_logger_conf

_LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    apply_logger_conf("INFO")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-datafolder",
        type=str,
        default="",
        help="Path to parquet data (folder name)",
    )
    args = parser.parse_args(None)

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.format('parquet').load(args.datafolder)

    # Required alert columns, concatenated with historical data
    what = ['magpsf', 'jd', 'sigmapsf', 'fid', 'distnr', 'magnr', 'sigmagnr', 'isdiffpos']

    prefix = 'c'
    what_prefix = [prefix + i for i in what]
    for colname in what:
        df = concat_col(df, colname, prefix=prefix)

    cols = ['cmagpsf', 'cjd', 'csigmapsf', 'cfid', 'objectId', 'cdistnr', 'cmagnr', 'csigmagnr', 'cisdiffpos']
    pdf = df.select(cols).toPandas()

    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        for index, row in pdf.iterrows():
            out = extract_features_ad.__wrapped__(*[row[k] for k in pdf.columns])

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

