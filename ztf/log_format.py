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
import logging

def apply_logger_conf(level: str) -> None:
    """Apply logging formatting

    Parameters
    ----------
    level: str
        Level name: ERROR, WARN, INFO, DEBUG
    """
    # Set logging level
    numeric_level = getattr(logging, level, None)
    if not isinstance(numeric_level, int):
        raise ValueError("Invalid log level: {level}")
    logging.basicConfig(
        level=numeric_level,
        format="[profiling  %(levelname)s @ %(asctime)s] %(message)s",
        datefmt="%I:%M:%S",
    )
