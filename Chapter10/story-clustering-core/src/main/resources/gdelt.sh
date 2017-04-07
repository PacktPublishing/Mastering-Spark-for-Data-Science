#!/usr/bin/env bash

MASTER_URL="http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
DATE_PREFIX=$1

#http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
#http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt

TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
RECORDS=`lynx -width 300 -dump ${MASTER_URL} | grep ${DATE_PREFIX} | grep "gkg.csv.zip" | awk '{print $3}' | sort`
TMP_DIR=$(mktemp -d -t GZET)
DEST_DIR="/tmp/gkg"
SLEEP=900

if [[ ! -e ${TMP_DIR} ]] ; then
    mkdir ${TMP_DIR}
fi

for RECORD in ${RECORDS}
do
    ZIP_NAME=$(basename ${RECORD})
    wget ${RECORD} -O ${TMP_DIR}/${ZIP_NAME} >/dev/null 2>&1
    mv ${TMP_DIR}/${ZIP_NAME} ${DEST_DIR}
    echo "${TIMESTAMP}: ${ZIP_NAME}"
    sleep ${SLEEP}
done
