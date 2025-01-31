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
"""Science modules in Fink"""

import pyspark.sql.functions as F

from fink_science.slsn.processor import slsn_elasticc_with_md
from fink_science.slsn.processor import slsn_elasticc_no_md
from fink_science.cats.processor import predict_nn

import logging


_LOG = logging.getLogger(__name__)


def load_rubin_modules(module_name="") -> dict:
    """Configuration with all science modules."""
    modules = {
        "SLSN (with metadata)": {
            "processor": slsn_elasticc_with_md,
            "cols": [
                "diaObject.diaObjectId",
                "cmidPointTai",
                "cpsFlux",
                "cpsFluxErr",
                "cfilterName",
                "diaSource.ra",
                "diaSource.decl",
                "diaObject.hostgal_zphot",
                "diaObject.hostgal_zphot_err",
                "diaObject.hostgal_snsep"
            ],
            "type": "ML",
            "colname": "slsn_with_md",
        },
        "SLSN (no metadata)": {
            "processor": slsn_elasticc_no_md,
            "cols": [
                "diaObject.diaObjectId",
                "cmidPointTai",
                "cpsFlux",
                "cpsFluxErr",
                "cfilterName",
                "diaSource.ra",
                "diaSource.decl",
            ],
            "type": "ML",
            "colname": "slsn_no_md",
        },
        "CATS_md": {
            "processor": predict_nn,
            "cols": [
                "cmidPointTai",
                "cpsFlux",
                "cpsFluxErr",
                "cfilterName",
                "diaObject.mwebv",
                "diaObject.z_final",
                "diaObject.z_final_err",
                "diaObject.hostgal_zphot",
                "diaObject.hostgal_zphot_err"
            ],
            "type": "ML",
            "colname": "cats_preds_md"
        },
        "CATS_nomd": {
            "processor": predict_nn,
            "cols": [
                "cmidPointTai",
                "cpsFlux",
                "cpsFluxErr",
                "cfilterName",
            ],
            "type": "ML",
            "colname": "cats_preds_nomd"
        }
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
