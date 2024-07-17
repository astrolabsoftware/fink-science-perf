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
"""Profile Early SN Ia for ZTF"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import time
import logging
import argparse
from codecarbon import EmissionsTracker

from fink_utils.spark.utils import concat_col
from fink_science.random_forest_snia.processor import rfscore_sigmoid_full

_LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-datafolder",
        type=str,
        default="",
        help="Path to parquet data (folder name)",
    )
    args = parser.parse_args(None)

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format('parquet').load(args.datafolder)

    # Required alert columns, concatenated with historical data
    what = ['jd', 'fid', 'magpsf', 'sigmapsf']

    prefix = 'c'
    what_prefix = [prefix + i for i in what]
    for colname in what:
        df = concat_col(df, colname, prefix=prefix)

    cols = [F.col(i) for i in what_prefix]
    cols += [F.col('cdsxmatch'), F.col('candidate.ndethist')]

    pdf = df.select(cols).toPandas()

    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = rfscore_sigmoid_full.__wrapped__(
            pdf['cjd'], 
            pdf['cfid'], 
            pdf['cmagpsf'], 
            pdf['csigmapsf'], 
            pdf['cdsxmatch'], 
            pdf['ndethist']
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

        # In this case, a zero probability means the
        # code did not run fully (quality cuts). So we
        # want to know the proportion of alerts fully classified (effective throughput)
        _LOG.info("{:.6f}% objects with p > 0".format(len(out[out > 0]) / len(out) * 100))

