# Quickstart Guide

This guide shows how to quickly get started with fink-science profiling using Docker.

## Prerequisites

- Docker installed on your system
- Access to the fink-science-perf repository
- Python environment with pip (for data download)

## Quick Setup

### 1. Download Sample Data (Host Machine)

First, download sample data using the fink-client outside of Docker:

```bash
# Install fink-client
pip install fink-client

# Register with Fink broker (replace with your credentials)
fink_client_register \
    -survey ztf \
    -username <your_username> \
    -group_id <your_group_id> \
    -servers kafka-ztf.fink-broker.org:24499

# Download sample data
fink_datatransfer \
    -topic ftransfer_ztf_2026-04-28_434189 \
    -outdir ftransfer_ztf_2026-04-28_434189 \
    -partitionby finkclass \
    -survey ztf \
    --verbose
```

### 2. Pull the Docker Image

```bash
# 3GB compressed - ZTF dependencies
docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-ztf:latest

# For Rubin/LSST dependencies (alternative)
docker pull gitlab-registry.in2p3.fr/astrolabsoftware/fink/fink-deps-sentinel-rubin:latest
```

### 3. Launch Docker Container

From the `fink-science-perf` directory, use `run_container.sh`. It mounts the workspace,
sets `PYTHONPATH`, and pip-installs any extra packages in editable mode:

```bash
# Minimal: just fink-science-perf
./run_container.sh

# With a local fink-science checkout (overrides the pip-installed version)
./run_container.sh -s $HOME/src/github.com/astrolabsoftware/fink-science

# With additional packages (e.g. fink_sn_activelearning)
./run_container.sh \
  -s $HOME/src/github.com/astrolabsoftware/fink-science \
  -m $HOME/src/github.com/emilleishida/fink_sn_activelearning
```

Once inside the container, navigate to the workspace:

```bash
cd /workspace/fink-science-perf
```

### 4. Verify Installation

```bash
# Check available science modules
./profile_module.sh -survey ztf --list_modules
```

Expected output (with warnings that can be ignored):
```
['CDS xmatch (SIMBAD)', 'CDS xmatch (vizier)', 'Local xmatch', 'Kilonova', 'Fast transient', 'Feature extraction', 'Microlensing', 'Asteroid', 'SuperNNova', 'Early SN Ia', 'SSOFT']
```

## Basic Usage

### List All Available Modules

```bash
./profile_module.sh -survey ztf --list_modules
```

### Profile a Specific Module

```bash
# Data must be under /workspace/fink-science-perf/ (or any mounted path)
./profile_module.sh -survey ztf -name "Early SN Ia" -d /workspace/fink-science-perf/ftransfer_ztf_2026-04-28_434189/
```

Expected output ends with the throughput and a profiling summary, e.g.:
```
[profiling  INFO] Throughput: 6704.9 alert/second
Wrote profile results to profiling_Early_SN_Ia.lprof
Inspect results with:
python -m line_profiler -rmt "profiling_Early_SN_Ia.lprof"
```

### Get Help

```bash
./profile_module.sh -h
```

### Check fink-science Version

```bash
./profile_module.sh --version
```

## Next Steps

Now you can run profiling on your downloaded data! The data should be available in the container at the mounted path.

See the main [README.md](README.md) for detailed instructions on advanced profiling workflows and performance testing.

## Notes

- The Docker container comes with all required dependencies pre-installed
- Spark warnings about pandas UDF types can be safely ignored
- The container runs as root user by default
- All changes inside the container are ephemeral unless you mount volumes

## Troubleshooting

### "No module named 'ztf.science_modules'"
- Make sure you launched the container with `run_container.sh` — it sets `PYTHONPATH` automatically
- If you started the container manually, run: `export PYTHONPATH=$PYTHONPATH:/workspace/fink-science-perf`

### "AssertionError" with Spark functions
- This happens when trying to use Spark functions outside of a Spark context
- For listing modules only, this error appears but the list is still displayed

### Permission issues
- The container runs as root, which should avoid most permission issues
- Ensure the host directory is readable by Docker
