#!/bin/bash
set -euo pipefail
shopt -s nullglob

INPUT="$1"
OUTDIR="results/phase3/hs37d5x/GATK3_fastq2gvcf-hs37d5x-1.0/bams"

COPYQS="$(qstat -u eb8858 | grep -c copyq || true)"
COUNTER=$((40 - COPYQS))


echo will submit: "$COUNTER"
while read -r bam_file; do
    if (("$COUNTER" > 0))
    then
        sampleid=$(basename "${bam_file}")
        
        queue_file="log/${sampleid}.queued"
        lock_file="log/${sampleid}.lock"
        done_file="log/${sampleid}.done"
        term_file="log/${sampleid}.term"

        if [ -e "${queue_file}" ]; then
            echo "${sampleid} already queued"
        elif [ -e "${lock_file}" ]; then
            echo "${sampleid} already running"
        elif [ -e "${done_file}" ]; then
            echo "${sampleid} already done"
        elif [ -e "${term_file}" ]; then
            echo "${sampleid} was terminated"
        else
            if ! test -e "$bam_file".md5.OK; then
                (cd "$(dirname "$bam_file")"; md5sum -c "$bam_file.md5" && touch "$bam_file.md5.OK")
            fi
            test -e "$bam_file".md5.OK
            netmv -l other=mdss -l other=gdata3 "$bam_file" "$OUTDIR"
            touch "${queue_file}"
            echo "${sampleid} queued"
            echo 'COUNTER =' ${COUNTER}
            COUNTER=$((COUNTER - 1))
        fi
    fi

done < "$INPUT"

