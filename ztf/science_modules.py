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

from fink_science.microlensing.processor import mulens
from fink_science.asteroids.processor import roid_catcher
from fink_science.snn.processor import snn_ia
from fink_science.random_forest_snia.processor import rfscore_sigmoid_full
from fink_science.xmatch.processor import cdsxmatch, crossmatch_other_catalog
from fink_science.kilonova.processor import knscore
from fink_science.anomaly_detection.processor import anomaly_score
from fink_science.fast_transient_rate.processor import magnitude_rate
from fink_science.ad_features.processor import extract_features_ad

import logging


_LOG = logging.getLogger(__name__)

def load_ztf_modules(module_name="") -> dict:
    """Configuration with all science modules."""
    modules = {
        'CDS xmatch (SIMBAD)': {
            'processor': cdsxmatch,
            'cols': ['candidate.candid', 'candidate.ra', 'candidate.dec', F.lit(1.0).alias("radius"), F.lit("simbad"), F.lit("main_type")],
            'type': 'xmatch',
            'colname': 'cdsxmatch'
        },
        'CDS xmatch (vizier)': {
            'processor': cdsxmatch,
            'cols': ['candidate.candid', 'candidate.ra', 'candidate.dec', F.lit(1.0).alias("radius"), F.lit("vizier:I/355/gaiadr3"), F.lit("DR3Name,Plx,e_Plx")],
            'type': 'xmatch',
            'colname': 'gaia'
        },
        'Local xmatch': {
            'processor': crossmatch_other_catalog,
            'cols': ['candidate.candid', 'candidate.ra', 'candidate.dec', F.lit("gcvs"), F.lit(1.5).alias("radius")],
            'type': 'xmatch',
            'colname': 'gcvs'
        },
        'Kilonova': {
            'processor': knscore,
            'cols': ['cjd', 'cfid', 'cmagpsf', 'csigmapsf', F.col('candidate.jdstarthist'), F.col('cdsxmatch'), F.col('candidate.ndethist')],
            'type': 'ml',
            'colname': 'rf_kn_vs_nonkn'
        },
        'Anomaly': {
            'processor': anomaly_score,
            'cols': ["lc_features"]
        },
        'Fast transient': {
            'processor': magnitude_rate,
            'cols': ['candidate.magpsf', 'candidate.sigmapsf', 'candidate.jd', 'candidate.jdstarthist', 'candidate.fid', 'cmagpsf', 'csigmapsf', 'cjd', 'cfid', 'cdiffmaglim', F.lit(1000).alias("N"), F.lit(None).alias("seed")],
            'type': 'feature',
            'colname': 'fast_transient'
        },
        'Feature extraction': {
            'processor': extract_features_ad,
            'cols': ['cmagpsf', 'cjd', 'csigmapsf', 'cfid', 'objectId', 'cdistnr', 'cmagnr', 'csigmagnr', 'cisdiffpos'],
            'type': 'feature',
            'colname': 'lc_features'
        },
        'Microlensing': {
            'processor': mulens,
            'cols': ['cfid', 'cmagpsf', 'csigmapsf', 'cmagnr', 'csigmagnr', 'cisdiffpos', 'candidate.ndethist'],
            'type': 'ml',
            'colname': 'mulens'
        },
        'Asteroid': {
            'processor': roid_catcher,
            'cols': ['cjd', 'cmagpsf', 'candidate.ndethist', 'candidate.sgscore1', 'candidate.ssdistnr', 'candidate.distpsnr1'],
            'type': 'feature',
            'colname': 'roid'
        },
        'SuperNNova': {
            'processor': snn_ia,
            'cols': ['candid', 'cjd', 'cfid', 'cmagpsf', 'csigmapsf', 'roid', 'cdsxmatch', 'candidate.jdstarthist', F.lit('snn_snia_vs_nonia')],
            'type': 'ml',
            'colname': 'snn_snia_vs_nonia'
        },
        'Early SN Ia': {
            'processor': rfscore_sigmoid_full,
            'cols': ['cjd', 'cfid', 'cmagpsf', 'csigmapsf', 'cdsxmatch', F.col('candidate.ndethist')],
            'type': 'ml',
            'colname': 'rf_snia_vs_nonia'
        },
    }

    if module_name != "":
        out = {k:v for k, v in modules.items() if k == module_name}
        if len(out) == 0:
            _LOG.error("The module name {} is not correct. Choose between: {}".format(module_name, modules.keys()))
        return out

    return modules
