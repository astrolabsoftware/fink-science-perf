# Profiling & performance for fink-science

This repository contains scripts to perform the profiling of [fink-science](https://github.com/astrolabsoftware/fink-science) modules.

The goal is... Note simple profiling... More information with perf...  

## Environment

### Docker

Fire a docker container with all Fink dependencies installed:

```bash
# 2.3GB compressed
docker pull julienpeloton/fink-ci:latest

docker run -t -i --rm julienpeloton/fink-ci:latest bash
```

### Data

The best is to use the [Data Transfer](https://fink-portal.org/download) service to get tailored data for your test.
Make sure you have an account to use the [fink-client](https://github.com/astrolabsoftware/fink-client). Install it
and register your credentials on the container:

```bash
# Install the client
pip install fink-client

# register
fink_client_register -username julien -group_id julien -mytopics nil -servers 134.158.74.95:24499
```

Trigger a job on the Data Transfer service and download data in your container:

```bash
# Change accordingly
TOPIC=ftransfer_ztf_2024-07-16_682277

mkdir -p /data/$TOPIC
fink_datatransfer \
            -topic $TOPIC \
            -outdir /data/$TOPIC \
            -partitionby finkclass \
            --verbose
```

## Profiling

### Default

By default, the latest version of fink-science is installed in the container. 
Walk to the folder ztf, and launch ... TODO: script to test individual module, or all.

### Profiling a new PR

In case a user opens a new PR and you want to profile the new code, you first need to
remove the fink-science dependency in the container:

```bash
pip uninstall fink-science
```

and clone the targeted version:

```bash
# e.g. modified version of fink-science
# corresponding to PR https://github.com/astrolabsoftware/fink-science/pull/396
git clone https://github.com/utthishtastro/fink-science.git
git checkout hostless_detection
```

In case the code is not instrumented, add necessary decorators:

```python
from line_profiler import profile

@profile
def the_function_that_needs_to_be_profiled(...)
```

and finally add a new script to test the module:

```python
import time
from codecarbon import EmissionsTracker

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from fink_utils.spark.utils import concat_col
from fink_science.hostless_detection.processor import run_potential_hostless

# Spark is only used to pre-process data
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Change the path accordingly
df = spark.read.format('parquet').load('/data/ftransfer_ztf_2024-07-16_682277')

# Required for this module
df = concat_col(df, 'magpsf', prefix='c')

# Cast data into a Pandas DataFrame
# Limit to 1000 is not necessary
pdf = df.limit(1000).select(
    [
        "cmagpsf", 
        F.col("cutoutScience.stampData").alias("cutoutScience"), 
        F.col("cutoutTemplate.stampData").alias("cutoutTemplate"), 
        "objectId"
    ]
).toPandas()

# Change accordingly the PUE
with EmissionsTracker(tracking_mode='process', pue=1.25) as tracker:
    t0 = time.time()

    # call the processor without the Spark decorator
    out = run_potential_hostless.__wrapped__(
        pdf["cmagpsf"], 
        pdf["cutoutScience"], 
        pdf["cutoutTemplate"], 
        pdf["objectId"]
    )

    # Raw throughput (single core)
    print("{:.2f} alert/second".format(len(pdf) / (time.time() - t0)))

    # In this case, a negative probability means the 
    # code did not run fully (quality cuts). So we 
    # want to know the proportion of alerts fully classified (effective throughput)
    print("{:.0f}% objects with p > 0".format(len(out[out > 0]) / len(out) * 100))
```

and run the code with:

```bash
#!/bin/bash

kernprof -l profile_code.py
python -m line_profiler profile_code.py.lprof
```

Dependending on how many decorators you put in the code,
you will see a more or less details in the form:

```python
File: /home/libs/fink-science/fink_science/hostless_detection/run_pipeline.py
Function: process_candidate_fink at line 31

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    31                                               @profile
    32                                               def process_candidate_fink(self, science_stamp: Dict,
    33                                                                          template_stamp: Dict, objectId: str):
    34                                                   """
    35                                                   Processes each candidate
    36                                           
    37                                                   Parameters
    38                                                   ----------
    39                                                   science_stamp
    40                                                      science stamp data
    41                                                   template_stamp
    42                                                      template stamp data
    43                                                   """
    44      1000    1217571.0   1217.6      1.8          science_stamp = read_bytes_image(science_stamp)
    45      1000    1026124.8   1026.1      1.5          template_stamp = read_bytes_image(template_stamp)
    46      1000       1110.8      1.1      0.0          if not ((science_stamp.shape == (63, 63)) and (template_stamp.shape == (63, 63))):
    47        15        288.4     19.2      0.0              print(objectId, science_stamp.shape, template_stamp.shape)
    48        15          5.1      0.3      0.0              return -99
    49       985        571.2      0.6      0.0          science_stamp_clipped, template_stamp_clipped = (
    50       985    4539642.4   4608.8      6.8              self._run_sigma_clipping(science_stamp, template_stamp))
    51      1970    2558995.9   1299.0      3.8          is_hostless_candidate = run_hostless_detection_with_clipped_data(
    52       985        208.7      0.2      0.0              science_stamp_clipped, template_stamp_clipped,
    53       985        541.1      0.5      0.0              self.configs, self._image_shape)
    54       985        418.2      0.4      0.0          if is_hostless_candidate:
    55        30   57607331.3    2e+06     86.0              power_spectrum_results = run_powerspectrum_analysis(
    56        15          3.8      0.3      0.0                  science_stamp, template_stamp,
    57        15         80.6      5.4      0.0                  science_stamp_clipped.mask.astype(int),
    58        15         55.5      3.7      0.0                  template_stamp_clipped.mask.astype(int), self._image_shape)
    59        15          8.4      0.6      0.0              return power_spectrum_results["kstest_SCIENCE_15_statistic"]
    60       970        449.2      0.5      0.0          return -99
```

What matters first is the column `% Time` which indicates the percentage of time
spent per call. In this example above, 86% is spent in calling `run_powerspectrum_analysis`
which would be the target to optimize if we want to improve the performances.

Another important column is `Hits`, that is the number of time an instruction has been done.
In this example, we started with 1,000 alerts, and the first lines were called 1,000 times.
But then we have a branch (`if is_hostless_candidate:`), and the costly function is 
actually only called 30 times.

## Performance checks

TBD
