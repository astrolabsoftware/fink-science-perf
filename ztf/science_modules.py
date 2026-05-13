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
from ztf.utils import ScienceModule
import logging

try:
    from fink_science.ztf.microlensing.processor import mulens
    from fink_science.ztf.asteroids.processor import roid_catcher
    from fink_science.ztf.snn.processor import snn_ia
    from fink_science.ztf.random_forest_snia.processor import rfscore_sigmoid_full
    from fink_science.ztf.xmatch.processor import cdsxmatch, crossmatch_other_catalog
    from fink_science.ztf.kilonova.processor import knscore

    # from fink_science.ztf.anomaly_detection.processor import anomaly_score
    from fink_science.ztf.fast_transient_rate.processor import magnitude_rate
    from fink_science.ztf.ad_features.processor import extract_features_ad

    # from fink_science.ztf.hostless_detection.processor import run_potential_hostless
    from fink_science.ztf.ssoft.processor import extract_ssoft_parameters
except ImportError as e:
    _LOG = logging.getLogger(__name__)
    _LOG.warning(e)

_LOG = logging.getLogger(__name__)

MODULE_NAMES = [
    "CDS xmatch (SIMBAD)",
    "CDS xmatch (vizier)",
    "Local xmatch",
    "Kilonova",
    "Fast transient",
    "Feature extraction",
    "Microlensing",
    "Asteroid",
    "SuperNNova",
    "Early SN Ia",
    "SSOFT",
]


def load_ztf_modules(module_name="") -> dict:
    """Configuration with all science modules."""
    modules = {
        "CDS xmatch (SIMBAD)": ScienceModule(
            processor=cdsxmatch,
            cols=[
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit(1.0).alias("radius"),
                F.lit("simbad"),
                F.lit("main_type"),
            ],
            kind="xmatch",
            colname="cdsxmatch",
        ),
        "CDS xmatch (vizier)": ScienceModule(
            processor=cdsxmatch,
            cols=[
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit(1.0).alias("radius"),
                F.lit("vizier:I/355/gaiadr3"),
                F.lit("DR3Name,Plx,e_Plx"),
            ],
            kind="xmatch",
            colname="gaia",
        ),
        "Local xmatch": ScienceModule(
            processor=crossmatch_other_catalog,
            cols=[
                "candidate.candid",
                "candidate.ra",
                "candidate.dec",
                F.lit("gcvs"),
                F.lit(1.5).alias("radius"),
            ],
            kind="xmatch",
            colname="gcvs",
        ),
        "Kilonova": ScienceModule(
            processor=knscore,
            cols=[
                "cjd",
                "cfid",
                "cmagpsf",
                "csigmapsf",
                F.col("candidate.jdstarthist"),
                F.col("cdsxmatch"),
                F.col("candidate.ndethist"),
            ],
            kind="ml",
            colname="rf_kn_vs_nonkn",
        ),
        # "Anomaly": ScienceModule(processor=anomaly_score, cols=["lc_features"], ...),
        "Fast transient": ScienceModule(
            processor=magnitude_rate,
            cols=[
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
            kind="feature",
            colname="fast_transient",
        ),
        "Feature extraction": ScienceModule(
            processor=extract_features_ad,
            cols=[
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
            kind="feature",
            colname="lc_features",
            scalar=True,
        ),
        "Microlensing": ScienceModule(
            processor=mulens,
            cols=[
                "cfid",
                "cmagpsf",
                "csigmapsf",
                "cmagnr",
                "csigmagnr",
                "cisdiffpos",
                "candidate.ndethist",
            ],
            kind="ml",
            colname="mulens",
        ),
        "Asteroid": ScienceModule(
            processor=roid_catcher,
            cols=[
                "cjd",
                "cmagpsf",
                "candidate.ndethist",
                "candidate.sgscore1",
                "candidate.ssdistnr",
                "candidate.distpsnr1",
            ],
            kind="feature",
            colname="roid",
        ),
        "SuperNNova": ScienceModule(
            processor=snn_ia,
            cols=[
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
            kind="ml",
            colname="snn_snia_vs_nonia",
        ),
        "Early SN Ia": ScienceModule(
            processor=rfscore_sigmoid_full,
            cols=[
                "cjd",
                "cfid",
                "cmagpsf",
                "csigmapsf",
                "cdsxmatch",
                F.col("candidate.ndethist"),
            ],
            kind="ml",
            colname="rf_snia_vs_nonia",
        ),
        "SSOFT": ScienceModule(
            processor=extract_ssoft_parameters,
            cols=[
                "ssnamenr",
                "cmagpsf",
                "csigmapsf",
                "cjd",
                "cfid",
                "cra",
                "cdec",
                "RA",
                "DEC",
                "Phase",
                "Dobs",
                "Dhelio",
                F.lit("nifty"),
                F.lit("SOCCA").alias("model"),
            ],
            kind="agg",
            colname="ssoft_params",
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
