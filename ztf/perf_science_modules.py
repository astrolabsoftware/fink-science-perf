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
from fink_science import __version__
from ztf.science_modules import load_ztf_modules
from ztf.utils import load_spark_session
from ztf.utils import plot_histogram
from ztf.utils import save_on_disk
from ztf.utils import concat

from ztf.log_format import apply_logger_conf

import time
import logging
import argparse

_LOG = logging.getLogger(__name__)


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

    _LOG.info('Fink Science version: {}'.format(__version__))

    # setup
    gb_per_cpus = [args.gb_per_executor] * args.nloops
    n_cpus = [args.total_memory / i for i in gb_per_cpus]

    _LOG.info('Benchmark will be done using {} cores total'.format(n_cpus[0]))

    # You need the spark session active
    # to load modules
    spark = load_spark_session(n_cpu=1, gb_per_cpu=2)
    modules = load_ztf_modules()
    spark.stop()

    for module_name, module_prop in modules.items():
        _LOG.info(module_name)
        throughput = []
        for n_cpu, gb_per_cpu in zip(n_cpus, gb_per_cpus):

            spark = load_spark_session(n_cpu=n_cpu, gb_per_cpu=gb_per_cpu)


            df = spark.read.format('parquet').load(
                'archive/science/year={}/month={}/day={}'.format(
                    args.night[:4], args.night[4:6], args.night[6:8]
                )
            )

            df = concat(df)

            # Recompute lc_features for anomaly
            if module_name == "Anomaly":
                df = df.withColumn(
                    modules["Feature extraction"]["colname"],
                    modules["Feature extraction"]["processor"](*modules["Feature extraction"]["cols"])
                )

            df = df.select(module_prop["cols"]).repartition(numPartitions=int(n_cpu))

            # Cache to reduce I/O perturbations
            # Make sure you have enough memory
            df = df.cache()

            total_alerts = df.count()

            _LOG.info('Number of alerts: {:,}'.format(total_alerts))

            # TODO: define a condition for each module to
            # know how many alerts are really processed

            t0 = time.time()
            pdf = df.withColumn(
                'tmp',
                module_prop["processor"](*df.columns)
            ).select('tmp').toPandas()
            t_lapse = time.time() - t0
            throughput.append(total_alerts/t_lapse/n_cpu)
            spark.stop()

        modules[module_name]["result"] = throughput

    # plot
    plot_histogram(modules, kind='ztf')

    # save parquet
    save_on_disk(
        modules,
        args.total_memory,
        args.gb_per_executor,
        args.core_per_executor,
        args.night,
        total_alerts
    )




