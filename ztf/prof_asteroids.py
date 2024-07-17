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
"""Profile roid_catcher for ZTF"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import time
import logging
import argparse
from codecarbon import EmissionsTracker

from fink_utils.spark.utils import concat_col
from fink_science.asteroids.processor import roid_catcher

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
    what = ['jd', 'magpsf'] 

    prefix = 'c'
    what_prefix = [prefix + i for i in what]
    for colname in what:
        df = concat_col(df, colname, prefix=prefix)

    cols = ['cjd', 'cmagpsf', 'candidate.ndethist', 'candidate.sgscore1', 'candidate.ssdistnr', 'candidate.distpsnr1']
    pdf = df.select(cols).toPandas()

    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = roid_catcher.__wrapped__(*[pdf[col] for col in pdf.columns])

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

