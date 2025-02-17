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
"""Profile science modules for ZTF"""

from pyspark.sql import SparkSession
from fink_science import __version__

import time
import logging
import argparse

from ztf.science_modules import load_ztf_modules
from ztf.utils import concat

from ztf.log_format import apply_logger_conf

_LOG = logging.getLogger(__name__)

if __name__ == "__main__":
    apply_logger_conf("INFO")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-datafolder",
        type=str,
        required=True,
        help="Path to parquet data (folder name). Required.",
    )
    parser.add_argument(
        "-module_name",
        type=str,
        default="",
        help="Name of the module to perform. See ztf/science_modules for available names. Default is empty string, meaning all modues will be profiled.",
    )
    args = parser.parse_args(None)

    _LOG.info("Fink Science version: {}".format(__version__))

    spark = SparkSession.builder.master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    modules = load_ztf_modules(module_name=args.module_name)

    if "SSOFT" in modules and len(modules.keys()) > 1:
        # SSOFT cannot be processed with others
        # Remove it from the list
        modules.pop("SSOFT")

    for module_name, module_prop in modules.items():
        _LOG.info("Profiling {}".format(module_name))

        df = spark.read.format("parquet").load(args.datafolder)

        if module_name != "SSOFT":
            # SSOFT data is different from data transfer
            # It uses aggregated data in
            # fink-science/fink_science/data/alerts/sso_aggregated_2024.09_test_sample.parquet
            df = concat(df)

        # Recompute lc_features for anomaly
        if module_name == "Anomaly":
            df = df.withColumn(
                modules["Feature extraction"]["colname"],
                modules["Feature extraction"]["processor"](
                    *modules["Feature extraction"]["cols"]
                ),
            )

        pdf = df.select(module_prop["cols"]).toPandas()
        t0 = time.time()
        if module_name == "Feature extraction":
            # standard UDF
            for _, row in pdf.iterrows():
                out = module_prop["processor"].__wrapped__(*[
                    row[k] for k in pdf.columns
                ])
        else:
            out = module_prop["processor"].__wrapped__(*[
                pdf[col] for col in pdf.columns
            ])

        # Raw throughput (single core)
        _LOG.info(
            "Throughput: {:.1f} alert/second".format(len(pdf) / (time.time() - t0))
        )

        # In this case, a zero probability means the
        # code did not run fully (quality cuts). So we
        # want to know the proportion of alerts fully classified (effective throughput)
        # _LOG.info("{:.6f}% objects with p > 0".format(len(out[out > 0]) / len(out) * 100))

    spark.stop()
