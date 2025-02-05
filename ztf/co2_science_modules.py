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
"""Science modules performance using Apache Spark for ZTF"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.functions as F

from fink_science import __version__
from fink_science.microlensing.processor import mulens
from fink_science.asteroids.processor import roid_catcher
from fink_science.snn.processor import snn_ia
from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.xmatch.processor import cdsxmatch, crossmatch_other_catalog
from fink_science.kilonova.processor import knscore
from fink_science.fast_transient_rate.processor import magnitude_rate
from fink_science.ad_features.processor import extract_features_ad

from fink_utils.spark.utils import concat_col

import time
import logging
import argparse
import numpy as np
import pandas as pd
from codecarbon import EmissionsTracker

from ztf.log_format import apply_logger_conf

import matplotlib.pyplot as plt
import seaborn as sns

sns.set_context("poster")

_LOG = logging.getLogger(__name__)


def load_configuration() -> dict:
    """Configuration with all science modules."""
    modules = {
        "CDS xmatch (SIMBAD)": {
            "processor": cdsxmatch,
            "cols": [
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit(1.0).alias("radius"),
                F.lit("simbad"),
                F.lit("main_type"),
            ],
            "type": "xmatch",
            "colname": "cdsxmatch",
        },
        "CDS xmatch (vizier)": {
            "processor": cdsxmatch,
            "cols": [
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit(1.0).alias("radius"),
                F.lit("vizier:I/355/gaiadr3"),
                F.lit("DR3Name,Plx,e_Plx"),
            ],
            "type": "xmatch",
            "colname": "gaia",
        },
        "Local xmatch": {
            "processor": crossmatch_other_catalog,
            "cols": [
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit("gcvs"),
                F.lit(1.5).alias("radius"),
            ],
            "type": "xmatch",
            "colname": "gcvs",
        },
        "Kilonova": {
            "processor": knscore,
            "cols": [
                "cjd",
                "cfid",
                "cmagpsf",
                "csigmapsf",
                F.col("candidate.jdstarthist"),
                F.col("cdsxmatch"),
                F.col("candidate.ndethist"),
            ],
            "type": "ml",
            "colname": "rf_kn_vs_nonkn",
        },
        # 'Anomaly': {
        #     'processor': anomaly_score,
        #     'cols': ["lc_features"]
        # },
        "Fast transient": {
            "processor": magnitude_rate,
            "cols": [
                "candidate.magpsf",
                "candidate.sigmapsf",
                "candidate.jd",
                "candidate.jdstarthist",
                "candidate.fid",
                "cmagpsf",
                "csigmapsf",
                "cjd",
                "cfid",
                "cdiffmaglim",
                F.lit(1000).alias("N"),
                F.lit(None).alias("seed"),
            ],
            "type": "feature",
            "colname": "fast_transient",
        },
        "Feature extraction": {
            "processor": extract_features_ad,
            "cols": [
                "cmagpsf",
                "cjd",
                "csigmapsf",
                "cfid",
                "objectId",
                "cdistnr",
                "cmagnr",
                "csigmagnr",
                "cisdiffpos",
            ],
            "type": "feature",
            "colname": "lc_features",
        },
        "Microlensing": {
            "processor": mulens,
            "cols": [
                "cfid",
                "cmagpsf",
                "csigmapsf",
                "cmagnr",
                "csigmagnr",
                "cisdiffpos",
                "candidate.ndethist",
            ],
            "type": "ml",
            "colname": "mulens",
        },
        "Asteroid": {
            "processor": roid_catcher,
            "cols": [
                "cjd",
                "cmagpsf",
                "candidate.ndethist",
                "candidate.sgscore1",
                "candidate.ssdistnr",
                "candidate.distpsnr1",
            ],
            "type": "feature",
            "colname": "roid",
        },
        "SuperNNova": {
            "processor": snn_ia,
            "cols": [
                "candid",
                "cjd",
                "cfid",
                "cmagpsf",
                "csigmapsf",
                "roid",
                "cdsxmatch",
                "candidate.jdstarthist",
                F.lit("snn_snia_vs_nonia"),
            ],
            "type": "ml",
            "colname": "snn_snia_vs_nonia",
        },
        "Early SN Ia": {
            "processor": rfscore_sigmoid_full,
            "cols": [
                "cjd",
                "cfid",
                "cmagpsf",
                "csigmapsf",
                "cdsxmatch",
                F.col("candidate.ndethist"),
            ],
            "type": "ml",
            "colname": "rf_snia_vs_nonia",
        },
    }

    return modules


def concat(df):
    """Retrieve time-series information."""
    # should include all necessary aggregation
    what = [
        "jd",
        "magpsf",
        "sigmapsf",
        "fid",
        "magnr",
        "sigmagnr",
        "isdiffpos",
        "diffmaglim",
        "distnr",
    ]

    prefix = "c"
    for colname in what:
        df = concat_col(df, colname, prefix=prefix)
    return df


def plot_histogram(modules, kind="ztf"):
    """ """
    fig = plt.figure(figsize=(12, 10))
    fig.add_subplot(111)

    colors = ["C{}".format(i) for i in range(len(modules))]

    # assuming run with the same GB/core to get the average
    plt.bar(
        modules.keys(),
        [np.mean(val["throughput"]) for val in modules.values()],
        yerr=[np.std(val["throughput"]) for val in modules.values()],
        color=colors,
        edgecolor=colors,
        ecolor="black",
        linewidth=3,
        error_kw={"capsize": 10},
        alpha=0.3,
        fill=True,
    )
    plt.grid(alpha=0.5)

    plt.ylabel("alerts/second/core")

    plt.title('"on-sky" single core performance (version {})'.format(__version__))

    if kind == "lsst":
        # r'@ LSST scale @ 16 CPU'
        plt.axhline(1000 / 16, ls="--", color="grey", label="1 sec @ 16 cores")
        plt.axhline(
            1000 / 16 / 2, ls="--", color="grey", alpha=0.5, label=r"2 sec @ 16 cores"
        )
        plt.legend(loc="upper left", title="Per LSST exposure")

    elif kind == "ztf":
        # r'@ ZTF scale @ 8 CPU' 600 alerts/exposure
        plt.axhline(600 / 8, ls="--", color="grey", label="1 sec @ 8 cores")
        plt.axhline(
            600 / 8 / 2, ls="--", color="grey", alpha=0.5, label=r"2 sec @ 8 cores"
        )
        plt.legend(loc="upper left", title="Per ZTF exposure")

    plt.yscale("log")
    plt.xticks(rotation=90)
    plt.ylim(10, None)
    plt.tight_layout()
    plt.savefig("perf_fink_science_{}_pandas.png".format(__version__))
    plt.show()


def plot_histogram_co2(modules, kind="ztf"):
    """ """
    fig = plt.figure(figsize=(12, 10))
    fig.add_subplot(111)

    colors = ["C{}".format(i) for i in range(len(modules))]

    # 8h per day, 365 day
    coeff = 8 * 3600 * 365

    # assuming run with the same GB/core to get the average
    plt.bar(
        modules.keys(),
        [np.mean(val["co2"]) * coeff for val in modules.values()],
        yerr=[np.std(val["co2"]) * coeff for val in modules.values()],
        color=colors,
        edgecolor=colors,
        ecolor="black",
        linewidth=3,
        error_kw={"capsize": 10},
        alpha=0.3,
        fill=True,
    )
    plt.grid(alpha=0.5)

    plt.ylabel("CO₂eq (kg/year)")

    plt.title("Emissions as CO₂-equivalents (version {})".format(__version__))

    plt.xticks(rotation=90)
    plt.ylim(0, None)
    plt.tight_layout()
    plt.savefig("co2_fink_science_{}_pandas.png".format(__version__))
    plt.show()


def save_on_disk(
    modules, total_memory, gb_per_executor, core_per_executor, night, total_alerts
):
    """ """
    for k_ in ["throughput", "co2"]:
        modules_ = {k: modules[k][k_] for k in modules.keys()}

        nloops = len(list(modules.values())[0]["throughput"])
        conf_line = "total_memory={}, gb_per_executor={}, core_per_executor={}, night={}, total_alerts={}".format(
            total_memory, gb_per_executor, core_per_executor, night, total_alerts
        )

        modules_["config"] = [conf_line] * nloops

        pdf = pd.DataFrame(modules_)
        pdf.to_parquet(
            "perf_science_modules_{}_{}_pandas.parquet".format(k_, __version__),
            index=False,
        )


def load_spark_session(n_cpu, gb_per_cpu):
    """ """
    props = {
        "spark.mesos.principal": "lsst",
        "spark.mesos.secret": "secret",
        "spark.mesos.role": "lsst",
        "spark.executorEnv.HOME": "/home/julien.peloton",
        "spark.sql.execution.arrow.pyspark.enabled": True,
        "spark.sql.execution.arrow.maxRecordsPerBatch": 1000000,
        "spark.cores.max": "{}".format(int(n_cpu)),
        "spark.executor.cores": "1",
        "spark.executor.memory": "{}G".format(gb_per_cpu),
        "spark.kryoserializer.buffer.max": "512m",
    }

    conf = (
        SparkConf()
        .setAppName("Performance_{}".format(__version__))
        .setMaster("mesos://vm-75063.lal.in2p3.fr:5050")
    )

    for k, v in props.items():
        conf = conf.set(k, v)

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    return spark


if __name__ == "__main__":
    apply_logger_conf("INFO")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-night",
        type=str,
        default="20240716",
        help="Night in the form YYYYMMDD. Default is 20240716 (200k alerts)",
    )
    parser.add_argument(
        "-total_memory",
        type=int,
        default=16,
        help="Total RAM for the job in GB. Default is 16GB.",
    )
    parser.add_argument(
        "-gb_per_executor",
        type=int,
        default=2,
        help="Total RAM per executor. Default is 2GB.",
    )
    parser.add_argument(
        "-core_per_executor",
        type=int,
        default=1,
        help="Number of core per executor. Default is 1.",
    )
    parser.add_argument(
        "-nloops",
        type=int,
        default=2,
        help="Number of times to run the performance test. Default is 2.",
    )

    args = parser.parse_args(None)

    _LOG.info("Fink Science version: {}".format(__version__))

    # setup
    gb_per_cpus = [args.gb_per_executor] * args.nloops
    n_cpus = [args.total_memory / i for i in gb_per_cpus]

    _LOG.info("Benchmark will be done using {} cores total".format(n_cpus[0]))

    # You need the spark session active
    # to load modules
    spark = load_spark_session(n_cpu=1, gb_per_cpu=2)
    modules = load_configuration()
    spark.stop()

    for module_name, module_prop in modules.items():
        _LOG.info(module_name)
        throughput = []
        co2 = []
        for n_cpu, gb_per_cpu in zip(n_cpus, gb_per_cpus):
            spark = load_spark_session(n_cpu=n_cpu, gb_per_cpu=gb_per_cpu)

            df = spark.read.format("parquet").load(
                "archive/science/year={}/month={}/day={}".format(
                    args.night[:4], args.night[4:6], args.night[6:8]
                )
            )

            df = concat(df)

            pdf = df.select(module_prop["cols"]).toPandas()

            with EmissionsTracker(tracking_mode="process", pue=1.25) as tracker:
                t0 = time.time()
                if module_name == "Feature extraction":
                    # scalar udf
                    for _, row in pdf.iterrows():
                        out = module_prop["processor"].__wrapped__(*[
                            row[k] for k in pdf.columns
                        ])
                else:
                    # pandas udf (vectorised)
                    out = module_prop["processor"].__wrapped__(*[
                        pdf[col] for col in pdf.columns
                    ])

                # Raw throughput (single core)
                throughput.append(len(pdf) / (time.time() - t0))

            raw_emission = tracker.final_emissions_data.values["emissions_rate"]

            # monitor the platform for the same time
            with EmissionsTracker(tracking_mode="process", pue=1.25) as tracker_:
                time.sleep(tracker.final_emissions_data.values["duration"])

            baseline_emission = tracker_.final_emissions_data.values["emissions_rate"]

            _LOG.info("RAW: {:.2f}".format(raw_emission * 8 * 3600 * 365))
            _LOG.info("BAS: {:.2f}".format(baseline_emission * 8 * 3600 * 365))
            _LOG.info(
                "DIF: {:.2f}".format(
                    (raw_emission - baseline_emission) * 8 * 3600 * 365
                )
            )
            co2.append(raw_emission - baseline_emission)

            spark.stop()

        modules[module_name]["throughput"] = throughput
        modules[module_name]["co2"] = co2

    # plot
    plot_histogram(modules, kind="ztf")

    plot_histogram_co2(modules, kind="ztf")

    # save parquet
    save_on_disk(
        modules,
        args.total_memory,
        args.gb_per_executor,
        args.core_per_executor,
        args.night,
        len(pdf),
    )
