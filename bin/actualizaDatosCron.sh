#!/bin/bash

CONFIGFILE=/etc/sysconfig/GitTimeSeries

ENVIRONMENTLIST=$1

function adiosMundoCruel {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}

[ -n "${ENVIRONMENTLIST}" ] || adiosMundoCruel "No se ha especificado el parámetro con la lista de ficheros de entorno a descargar"
[ -f "${ENVIRONMENTLIST}" ] || adiosMundoCruel "Fichero con lista de entornos '${ENVIRONMENTLIST}' no existe"


[ -f "${CONFIGFILE}" ] || adiosMundoCruel "Fichero con entorno '${CONFIGFILE}' no existe"

source ${CONFIGFILE}

[ -f "${CONFIGFILE}" ] || adiosMundoCruel "Fichero con entorno '${CONFIGFILE}' no existe"


[ -n "${GTS_REPO}" ] || adiosMundoCruel "No se ha especificado la variable GTS_REPO"
[ -n "${GTS_CODEDIR}" ] || adiosMundoCruel "No se ha especificado la variable GTS_CODEDIR"

BASEDIR=$(cd "$(dirname $(readlink -e $0))/../" && pwd )
TODAY=$(date '+%Y%m%d%H%M')

[ -d ${GTS_CODEDIR} ] && rm -rf  ${GTS_CODEDIR}
mkdir -p ${GTS_CODEDIR}
git clone -q --branch master ${GTS_REPO} ${GTS_CODEDIR}


for ENTORNO in $(grep -v '^#' ${ENVIRONMENTLIST})
do
  ${GTS_CODEDIR}/bin/descargaDatosGen.sh ${ENTORNO}
done
