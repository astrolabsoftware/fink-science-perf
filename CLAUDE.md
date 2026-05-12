# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains scripts to perform profiling and performance checks of [fink-science](https://github.com/astrolabsoftware/fink-science) modules. It supports both ZTF (Zwicky Transient Facility) and LSST/Rubin Observatory data processing pipelines.

## Architecture

The codebase is organized into two main survey-specific directories:
- `ztf/` - ZTF-specific profiling and performance modules
- `rubin/` - Rubin Observatory-specific modules

### Key Components

1. **Science Module Configurations** (`ztf/science_modules.py`, `rubin/science_modules.py`)
   - Define all available science modules with their processors, required columns, types, and output column names
   - Each module has metadata: processor function, required columns, type (xmatch/ml/feature/agg), output column name

2. **Profiling Scripts** (`ztf/prof_science_module.py`, `rubin/prof_science_module.py`)
   - Individual module profiling using line_profiler
   - Load data and apply specific science modules for detailed performance analysis

3. **Performance Testing** (`ztf/perf_science_modules.py`)
   - Spark-based performance benchmarking across different CPU/memory configurations
   - Measures throughput (alerts/second/core) for all modules

4. **CO2 Emission Analysis** (`ztf/co2_science_modules.py`)
   - Uses codecarbon to measure energy consumption and CO2 equivalent emissions
   - Compares execution energy vs baseline system consumption

## Common Commands

### Profiling a Single Module
```bash
# List available modules
./profile_module.sh -survey ztf --list_modules

# Profile specific module
./profile_module.sh -survey ztf -name "Early SN Ia" -d /path/to/data

# Profile all modules
./profile_module.sh -survey ztf -d /path/to/data
```

### Performance Testing
```bash
# Run performance benchmarks (requires Spark cluster)
python ztf/perf_science_modules.py -night 20240716 -total_memory 16 -gb_per_executor 2 -core_per_executor 1 -nloops 2
```

### CO2 Emissions Analysis
```bash
# Measure CO2 emissions for modules
taskset --cpu-list 0 python ztf/co2_science_modules.py
```

### Code Quality
```bash
# Code formatting and linting
ruff check .
ruff format .
```

## Data Requirements

The scripts expect data from the Fink Data Transfer service. Data should be downloaded using:
```bash
# Install fink-client and register credentials first
pip install fink-client
fink_client_register -survey lsst

# Download data
TOPIC=ftransfer_ztf_2024-07-16_682277
mkdir -p /data/$TOPIC
fink_datatransfer -topic $TOPIC -outdir /data/$TOPIC -partitionby finkclass --verbose
```

## Docker Environment

The project is designed to work within Docker containers with fink dependencies:
```bash
# For ZTF data
docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-ztf:latest

# For LSST data
docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest
```

## Testing New fink-science Modules

When profiling new fink-science code:

1. Remove existing fink-science: `pip uninstall fink-science`
2. Clone and checkout target branch
3. Add line_profiler decorators: `@profile`
4. Install: `pip install .`
5. Update module configuration in `ztf/science_modules.py` or `rubin/science_modules.py`
6. Run profiling with `./profile_module.sh`

## Performance Analysis

- Focus on `% Time` column in line_profiler output for optimization targets
- Monitor `Hits` column to understand execution frequency
- Use Spark performance tests to measure scalability across different configurations
- SSOFT module requires special aggregated test data, not raw alerts

## Spark Configuration

Performance scripts assume VirtualData cluster configuration. Edit `ztf/utils.py` and `load_spark_session` function to modify:
- Master URI
- Data paths
- Mesos configuration