#!/bin/bash

function adiosMundoCruel {
  MSG=${1:-No msg}
  echo ${MSG}
  exit 1
}

if [ "x$1" != "x" ]
then
    ENVFILE=$1
    [ -f "${ENVFILE}" ] || adiosMundoCruel "Fichero con entorno '${ENVFILE}' no existe"

    source ${ENVFILE}
fi

[ -n "${GTS_CODEDIR}" ] || adiosMundoCruel "No se ha especificado la variable GTS_CODEDIR"
[ -n "${GTS_VENV}" ] || adiosMundoCruel "No se ha especificado la variable GTS_VENV"


[ -n "${DATADIR}" ] || adiosMundoCruel "No se ha especificado la variable DATADIR"
[ -n "${DATAFILE}" ] || adiosMundoCruel "No se ha especificado la variable DATAFILE"
[ -n "${GTS_INFILE}" ] || adiosMundoCruel "No se ha especificado la variable GTS_INFILE"
[ -n "${GTS_OUTFILE}" ] || adiosMundoCruel "No se ha especificado la variable GTS_OUTFILE"

[ -n "${GTS_SCRIPTFILE}" ] || adiosMundoCruel "No se ha especificado la variable GTS_SCRIPTFILE"
[ -f "${GTS_SCRIPTFILE}" ] || adiosMundoCruel "No se ha encontrado el fichero '${GTS_SCRIPTFILE}'"


if [ -d ${GTS_VENV} -a -x "${GTS_VENV}/bin/python" ]
then
  :
else
  #Toca crear el virtualenv para ejecutar el python
  [ -d ${GTS_VENV} ] || rm -rf  ${GTS_VENV} || adiosMundoCruel "Problemas eliminando ${GTS_VENV}. Bye"
  python3 -mvenv --clear ${GTS_VENV}
fi

#En cualquier caso, actualizamos paquetes, si procede
source ${GTS_VENV}/bin/activate
pip install -q -U pip wheel
pip install -U -q -r ${GTS_CODEDIR}/gitTimeSeries/requirements.txt

export PYTHONPATH=${PYTHONPATH}:${GTS_CODEDIR}/gitTimeSeries


DATAFILEENREPO="${DATAFILE##${DATADIR}/}"


${GTS_VENV}/bin/python ${GTS_SCRIPTFILE} -r ${DATADIR} -f ${DATAFILEENREPO}

RES=$?
if [ $RES != 0 ]
then
  adiosMundoCruel "Fallo en la ejecución de '${GTS_SCRIPTFILE}'"
fi

if [ -f ${GTS_INFILE} ]
then
  cp ${GTS_INFILE} ${GTS_INFILE}.prev || adiosMundoCruel "Problemas copiando copia de cache ${GTS_INFILE}"
fi
[ -f ${GTS_OUTFILE} ] && cp ${GTS_OUTFILE} ${GTS_INFILE} || adiosMundoCruel "Problemas actualizando cache con ${GTS_OUTFILE}"
