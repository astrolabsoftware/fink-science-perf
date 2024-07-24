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
"""Utilities for profiling and performance"""
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import numpy as np
import pandas as pd

from fink_science import __version__


def plot_histogram(modules, kind='ztf'):
    """ """
    import matplotlib.pyplot as plt
    import seaborn as sns
    sns.set_context('poster')

    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(111)

    colors = ['C{}'.format(i) for i in range(len(modules))]

    # assuming run with the same GB/core to get the average
    plt.bar(
        modules.keys(),
        [np.mean(val["result"]) for val in modules.values()],
        yerr=[np.std(val["result"]) for val in modules.values()],
        color=colors, edgecolor=colors, ecolor='black',
        linewidth=3, error_kw={'capsize': 10},
        alpha=0.3, fill=True
    )
    plt.grid(alpha=0.5)

    plt.ylabel('alerts/second/core')

    plt.title('"on-sky" single core performance (version {})'.format(__version__))

    if kind == 'lsst':
        # r'@ LSST scale @ 16 CPU'
        plt.axhline(1000/16, ls='--', color='grey', label="1 sec @ 16 cores")
        plt.axhline(1000/16/2, ls='--', color='grey', alpha=0.5, label=r'2 sec @ 16 cores')
        plt.legend(loc='upper left', title='Per LSST exposure')

    elif kind == 'ztf':
        # r'@ ZTF scale @ 8 CPU' 600 alerts/exposure
        plt.axhline(600/8, ls='--', color='grey', label="1 sec @ 8 cores")
        plt.axhline(600/8/2, ls='--', color='grey', alpha=0.5, label=r'2 sec @ 8 cores')
        plt.legend(loc='upper left', title='Per ZTF exposure')

    plt.yscale('log')
    plt.xticks(rotation=90)
    plt.ylim(10, None)
    plt.tight_layout()
    plt.savefig('perf_fink_science_{}.png'.format(__version__))
    plt.show()

def save_on_disk(modules, total_memory, gb_per_executor, core_per_executor, night, total_alerts):
    """ """
    modules_ = {k: modules[k]['result'] for k in modules.keys()}

    nloops = len(list(modules.values())[0]['result'])
    conf_line = "total_memory={}, gb_per_executor={}, core_per_executor={}, night={}, total_alerts={}".format(
        total_memory, gb_per_executor, core_per_executor, night, total_alerts
    )

    modules_['config'] = [conf_line] * nloops

    pdf = pd.DataFrame(modules_)
    pdf.to_parquet('perf_science_modules_{}.parquet'.format(__version__), index=False)

def load_spark_session(n_cpu, gb_per_cpu, principal="lsst", secret="secret", role="lsst"):
    """
    """
    props = {
        "spark.mesos.principal": principal,
        "spark.mesos.secret": secret,
        "spark.mesos.role": role,
        "spark.sql.execution.arrow.pyspark.enabled": True,
        "spark.sql.execution.arrow.maxRecordsPerBatch": 1000000,
        "spark.cores.max": "{}".format(int(n_cpu)),
        "spark.executor.cores": "1",
        "spark.executor.memory": "{}G".format(gb_per_cpu),
        "spark.kryoserializer.buffer.max": "512m"
    }

    conf = SparkConf()\
        .setAppName("Performance_{}".format(__version__))\
        .setMaster("mesos://vm-75063.lal.in2p3.fr:5050")

    for k, v in props.items():
        conf = conf.set(k, v)

    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    return spark