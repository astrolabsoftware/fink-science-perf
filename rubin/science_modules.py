# Copyright 2024-2026 AstroLab Software
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
"""Science modules in Fink/LSST"""

import pyspark.sql.functions as F

from fink_science.rubin.cats.processor import predict_nn
from fink_science.rubin.snn.processor import snn_ia_elasticc
from fink_science.rubin.random_forest_snia.processor import (
    rfscore_rainbow_elasticc_nometa,
)
from fink_science.rubin.hostless_detection.processor import run_potential_hostless

from ztf.utils import ScienceModule

import logging

_LOG = logging.getLogger(__name__)

MODULE_NAMES = ["hostless", "snnSnVsOthers", "earlySNIa", "CATS"]


def load_rubin_modules(module_name="") -> dict:
    """Configuration with all science modules."""
    modules = {
        "hostless": ScienceModule(
            processor=run_potential_hostless,
            cols=[
                F.col("cutoutScience"),
                F.col("cutoutTemplate"),
                F.col("ssSource.ssObjectId"),
            ],
            kind="feature",
            colname="hostless",
        ),
        "snnSnVsOthers": ScienceModule(
            processor=snn_ia_elasticc,
            cols=[
                "diaSource.diaSourceId",
                "cmidpointMjdTai",
                "cband",
                "cpsfFlux",
                "cpsfFluxErr",
                F.lit("elasticc_binary_broad/SN_vs_other"),
            ],
            kind="ML",
            colname="snnSnVsOthers_score",
        ),
        "earlySNIa": ScienceModule(
            processor=rfscore_rainbow_elasticc_nometa,
            cols=[
                "cmidpointMjdTai",
                "cband",
                "cpsfFlux",
                "cpsfFluxErr",
            ],
            kind="ML",
            colname="earlySNIa_score",
        ),
        "CATS": ScienceModule(
            processor=predict_nn,
            cols=[
                "cmidpointMjdTai",
                "cpsfFlux",
                "cpsfFluxErr",
                "cband",
            ],
            kind="ML",
            colname="cats_broad_array_prob",
        ),
    }

    if module_name != "":
        out = {k: v for k, v in modules.items() if k == module_name}
        if len(out) == 0:
            _LOG.error(
                "The module name {} is not correct. Choose between: {}".format(
                    module_name, modules.keys()
                )
            )
        return out

    return modules
