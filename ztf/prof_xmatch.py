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
"""Profile xmatch for ZTF"""
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

import time
import logging
import argparse
from codecarbon import EmissionsTracker
import pandas as pd

from fink_science.xmatch.processor import cdsxmatch, crossmatch_other_catalog, crossmatch_mangrove

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

    cols = ['candidate.candid', 'candidate.ra', 'candidate.dec']
    pdf = df.select(cols).toPandas()

    _LOG.info("###### SIMBAD ######")
    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = cdsxmatch.__wrapped__(
            *[pdf[col] for col in pdf.columns], 
            pd.Series([1.0]), 
            pd.Series(["simbad"]), 
            pd.Series(["main_type"])
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

    _LOG.info("###### GAIA DR3 ######")
    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = cdsxmatch.__wrapped__(
            *[pdf[col] for col in pdf.columns],
            pd.Series([1.0]),
            pd.Series(["vizier:I/355/gaiadr3"]),
            pd.Series(["DR3Name,Plx,e_Plx"])
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

    _LOG.info("###### VSX ######")
    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = cdsxmatch.__wrapped__(
            *[pdf[col] for col in pdf.columns],
            pd.Series([1.5]),
            pd.Series(["vizier:B/vsx/vsx"]),
            pd.Series(["Type"])
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

    _LOG.info("###### SPICY ######")
    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = cdsxmatch.__wrapped__(
            *[pdf[col] for col in pdf.columns],
            pd.Series([1.2]),
            pd.Series(["vizier:J/ApJS/254/33/table1"]),
            pd.Series(["SPICY,class"])
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

    _LOG.info("###### MANGROVE ######")
    with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
        t0 = time.time()
        out = crossmatch_mangrove.__wrapped__(
            *[pdf[col] for col in pdf.columns],
            pd.Series([60.0]),
        )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))

    for radius, catalog in zip([1.5, 60.0, 60.0], ["gcvs", "3hsp", "4lac"]):
        _LOG.info("###### {} ######".format(catalog.upper()))
        with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
            t0 = time.time()
            out = crossmatch_other_catalog.__wrapped__(
                *[pdf[col] for col in pdf.columns],
                pd.Series([catalog]),
                pd.Series([radius]),
            )

        # Raw throughput (single core)
        _LOG.info("Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0)))


