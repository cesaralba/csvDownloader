#!/bin/bash

ME=$(readlink -e $0)
MYMD5=$(md5sum $ME | cut -d' ' -f1)

CONFIGFILE=/etc/sysconfig/GitTimeSeries

ENVIRONMENTLIST=$1

function adiosMundoCruel() {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}

[ -n "${ENVIRONMENTLIST}" ] || adiosMundoCruel "No se ha especificado el parámetro con la lista de ficheros de entorno a descargar"
[ -f "${ENVIRONMENTLIST}" ] || adiosMundoCruel "Fichero con lista de entornos '${ENVIRONMENTLIST}' no existe"

[ -f "${CONFIGFILE}" ] || adiosMundoCruel "Fichero con entorno '${CONFIGFILE}' no existe"

source ${CONFIGFILE}

[ -n "${GTS_REPO}" ] || adiosMundoCruel "No se ha especificado la variable GTS_REPO"
[ -n "${GTS_CODEDIR}" ] || adiosMundoCruel "No se ha especificado la variable GTS_CODEDIR"

BASEDIR=$(cd "$(dirname $(readlink -e $0))/../" && pwd)
TODAY=$(date '+%Y%m%d%H%M')

[ -d ${GTS_CODEDIR} ] && rm -rf ${GTS_CODEDIR}
mkdir -p ${GTS_CODEDIR}
git clone -q --branch master ${GTS_REPO} ${GTS_CODEDIR}  || adiosMundoCruel "Problemas descargando ${GTS_REPO}"

NEWMD5=$(md5sum ${GTS_CODEDIR}/bin/actualizaDatosCron.sh | cut -d' ' -f1)

if [ ${MYMD5} != ${NEWMD5} ]
then
  echo "AVISO: El MD5 de $ME es distinto de lo que acabo de bajar '${GTS_CODEDIR}/bin/actualizaDatosCron.sh'"
fi


for ENTORNO in $(grep -v '^#' ${ENVIRONMENTLIST}); do
  ${GTS_CODEDIR}/bin/descargaDatosGen.sh ${ENTORNO}
done
