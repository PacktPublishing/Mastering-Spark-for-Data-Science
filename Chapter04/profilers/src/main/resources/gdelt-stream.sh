#!/usr/bin/env bash

MASTER_URL="http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
LAST_PROCESSED=""
SLEEP=900

TMP_DIR=$(mktemp -d -t GZET)
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
echo "${TIMESTAMP}: Creating staging directory ${TMP_DIR}"

function finish {
  TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
  echo "${TIMESTAMP}: Cleaning up staging directory ${TMP_DIR}"
  rm -rf "${TMP_DIR}"
}

while true
do

    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    LAST_URL=`lynx -dump ${MASTER_URL} -width 300 | awk '{print $3}' | grep export`
    if [[ ${LAST_URL} != ${LAST_PROCESSED} ]] ; then

      ZIP_NAME=$(basename ${LAST_URL})
      FILE_NAME=${ZIP_NAME%.*}

      echo "${TIMESTAMP}: Downloading ${LAST_URL}"
      wget ${LAST_URL} -O ${TMP_DIR}/${ZIP_NAME} >/dev/null 2>&1
      unzip ${TMP_DIR}/${ZIP_NAME} -d ${TMP_DIR} >/dev/null 2>&1

      FILE=${TMP_DIR}/${FILE_NAME}
      echo "${TIMESTAMP}: Starting new Spark job for file ${FILE_NAME}"

      spark-submit --class io.gzet.profilers.GdeltStructuralProfiler \
        --packages io.gzet:profilers:2.0 \
        /Users/antoine/Workspace/gzet/profilers/target/profilers-2.0.jar \
        file://${FILE}

      rm ${FILE}
      rm ${TMP_DIR}/${ZIP_NAME}

      LAST_PROCESSED=${LAST_URL}

    fi

    echo "${TIMESTAMP}: Sleeping ${SLEEP}s"
	sleep ${SLEEP}

done


trap finish SIGINT SIGTERM EXIT