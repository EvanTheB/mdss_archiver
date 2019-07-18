#!/bin/bash
# mdss put a file into project:destination
# the mdss file commands are gimped so we cannot
# do this correctly.
# (eg atomically and without directory ambiguity)
# we put to a .part file and mv into position

set -euo pipefail
set -x

(set -o posix; set; ulimit -a; uname -a; lsb_release -a; hostname -A) 1>&2
me="$(readlink -f "${BASH_SOURCE[0]}")"
bme="$(basename "$me")"

function logger {
    local d
    d="$(date -Iseconds)"
    echo "$d - $bme -" "$@" 1>&2
}

function err_trap {
    local PS caller
    PS=("${PIPESTATUS[@]}")
    caller="$(caller)"
    echo BASH_ERROR: "${PS[@]}" 1>&2
    echo BASH_ERROR: "$caller" 1>&2
}
trap err_trap ERR

logger Begin

FILESOURCE="$1"
DESTINATION="$2"
PROJECT="$3"

# because we are qsubbing `exec bash` we don't get
# any /opt/bin/nfsh or .rc stuff including path to mdss
# this doesn't seem to matter to mdss otherwise.

# mdss mkdir succeeds if exists
/opt/bin/mdss -P "$PROJECT" mkdir "$(dirname "$DESTINATION")"

# there is no ln or mv -n, so we have no atomic way to do this

# if ls has a different error, we cannot detect it
if /opt/bin/mdss -P "$PROJECT" ls "$DESTINATION"; then
    logger fail: file already exists
    exit 1
fi
if /opt/bin/mdss -P "$PROJECT" ls "$DESTINATION".part; then
    logger fail: part file already exists
    exit 1
fi
/opt/bin/mdss -P "$PROJECT" put "$FILESOURCE" "$DESTINATION".part
/opt/bin/mdss -P "$PROJECT" mv "$DESTINATION".part "$DESTINATION"

touch "$bme.ok"

logger End
