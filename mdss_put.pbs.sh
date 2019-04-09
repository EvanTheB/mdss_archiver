#!/bin/bash
#PBS -q copyq
#PBS -P wq2
#PBS -l mem=2GB
#PBS -l walltime=10:00:00
#PBS -l other=gdata3:mdss

set -euo pipefail
set -x

mdss mkdir "$(dirname "$DESTINATION")"
mdss -P wq2 put "$FILESOURCE" "$DESTINATION"
touch "$FILESOURCE".put.done
